"""Event producer service.

Modes
-----
catchup   Publish the deterministic timeline from go-live (or the last checkpoint) up to *now*, then exit.
live      catchup, then keep publishing in real time as the simulated clock follows the wall clock.
burst     Load-test mode: publish N events from a fresh timeline as fast as possible (no checkpoint).

Restarts are safe: the simulator is deterministic, so the producer replays
the timeline silently up to its checkpoint (``_simulation/producer_state.json``)
and resumes publishing from there. Events between the last checkpoint and a
crash may be re-published. They carry the same ``event_id`` and Silver
deduplicates them (at-least-once delivery, see ADR 0010).
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from prometheus_client import Gauge

from data_generator.cli import build_simulator, load_anchor
from data_generator.events import Event
from data_generator.sinks import Landing, upsert_customers, upsert_supplier_deliveries
from ingestion.kafka_producers.producer import EventPublisher
from pipelines.common.logs import get_logger, log_context
from pipelines.common.metrics import serve_metrics

log = get_logger("event-producer")
STATE_KEY = "_simulation/producer_state.json"
TARGET_RATE = Gauge("ecom_producer_target_events_per_second", "Configured publish rate limit (0 = unlimited)")
CATCHING_UP = Gauge("ecom_producer_catching_up", "1 while replaying history faster than real time")


class RateLimiter:
    """Token bucket; rate <= 0 disables limiting."""

    def __init__(self, rate: float, burst: float | None = None) -> None:
        self.rate = rate
        self.capacity = burst or max(rate, 1.0)
        self.tokens = self.capacity
        self.updated = time.monotonic()

    def acquire(self) -> None:
        if self.rate <= 0:
            return
        while True:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens >= 1:
                self.tokens -= 1
                return
            time.sleep((1 - self.tokens) / self.rate)


class OpsSync:
    """Writes customer registrations/changes and supplier orders back to the operational DB."""

    def __init__(self, enabled: bool, go_live: datetime) -> None:
        self.enabled = enabled
        self.go_live = go_live
        self.customers: dict[str, dict[str, Any]] = {}
        self.delivery_idx = 0
        self.last_flush = time.time()
        self.conn = None
        if enabled:
            from pipelines.common.db import connect

            self.conn = connect()

    def on_event(self, event: Event) -> None:
        if self.enabled and event.entity == "customers" and event.payload.get("customer_id"):
            self.customers[event.payload["customer_id"]] = event.payload

    def maybe_flush(self, sim: Any, clock: datetime, force: bool = False) -> None:
        if not self.enabled or (not force and time.time() - self.last_flush < 5):
            return
        upsert_customers(self.conn, list(self.customers.values()))
        self.customers.clear()
        deliveries = sim.supplier_deliveries
        batch = [d.record() for d in deliveries[self.delivery_idx :] if d.ordered_at <= clock]
        if batch:
            upsert_supplier_deliveries(self.conn, [d for d in batch if d["ordered_at"] >= self.go_live])
            self.delivery_idx += len(batch)
        self.last_flush = time.time()


def load_state(landing: Landing) -> datetime | None:
    raw = landing.get(STATE_KEY)
    return datetime.fromisoformat(json.loads(raw)["last_emit_at"]) if raw else None


def save_state(landing: Landing, emit_at: datetime, published: int) -> None:
    landing.put(
        STATE_KEY,
        json.dumps(
            {
                "last_emit_at": emit_at.isoformat(),
                "published_total": published,
                "saved_at": datetime.now(UTC).isoformat(),
            }
        ).encode(),
    )


def run_timeline(args: argparse.Namespace) -> int:
    landing = Landing()
    anchor = load_anchor(landing)
    resume_after = None if args.from_go_live else load_state(landing)
    sim = build_simulator(anchor)
    publisher = EventPublisher()
    ops = OpsSync(args.sync_ops_db, anchor.go_live)
    limiter = RateLimiter(args.events_per_second)
    TARGET_RATE.set(args.events_per_second)
    stop = {"flag": False}

    def handle_signal(*_: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # Fast-forward silently through history (pre go-live) and the already-published range.
    skip_until = max(anchor.go_live, resume_after) if resume_after else anchor.go_live
    log.info(
        "fast-forwarding simulation",
        extra={"until": skip_until.isoformat(), "resume_after": str(resume_after)},
    )
    skipped = sum(1 for _ in sim.advance(skip_until))
    log.info("fast-forward complete", extra={"skipped_events": skipped})

    published = 0
    last_checkpoint = time.time()
    last_emit: datetime = skip_until
    CATCHING_UP.set(1)
    while not stop["flag"]:
        now = datetime.now(UTC)
        target = now if args.mode == "live" else min(now, args.until or now)
        caught_up = target - last_emit < timedelta(minutes=2)
        CATCHING_UP.set(0 if caught_up else 1)
        chunk_end = min(target, last_emit + timedelta(hours=1))
        for event in sim.advance(chunk_end):
            if stop["flag"]:
                break
            limiter.acquire()
            publisher.publish(event)
            ops.on_event(event)
            published += 1
            last_emit = event.emit_at or event.event_timestamp
            if args.max_events and published >= args.max_events:
                stop["flag"] = True
                break
        last_emit = max(last_emit, chunk_end) if not stop["flag"] else last_emit
        ops.maybe_flush(sim, last_emit)
        if time.time() - last_checkpoint > args.checkpoint_seconds:
            publisher.flush()
            save_state(landing, last_emit, published)
            last_checkpoint = time.time()
            log.info(
                "checkpoint",
                extra={
                    "published": published,
                    "simulated_clock": last_emit.isoformat(),
                    "dead_lettered": publisher.dead_lettered,
                },
            )
        if args.mode != "live" and last_emit >= target:
            break
        if caught_up and args.mode == "live":
            time.sleep(1.0)
    publisher.flush()
    ops.maybe_flush(sim, last_emit, force=True)
    save_state(landing, last_emit, published)
    print(
        json.dumps(
            {
                "published": published,
                "dead_lettered": publisher.dead_lettered,
                "simulated_clock": last_emit.isoformat(),
            }
        )
    )
    return 0


def run_burst(args: argparse.Namespace) -> int:
    """Publish N events as fast as possible (benchmarks). Uses a separate timeline, no checkpoint."""
    from data_generator.settings import get_settings
    from data_generator.simulator import JourneySimulator
    from data_generator.world import build_world

    settings = get_settings(args.scale)
    start = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(days=args.days_back)
    sim = JourneySimulator(build_world(settings, start), start)
    publisher = EventPublisher()
    limiter = RateLimiter(args.events_per_second)
    t0 = time.perf_counter()
    n = 0
    for event in sim.advance(datetime.now(UTC)):
        limiter.acquire()
        publisher.publish(event)
        n += 1
        if n >= args.max_events:
            break
    publish_s = time.perf_counter() - t0
    publisher.flush(300)
    total_s = time.perf_counter() - t0
    result = {
        "events": n,
        "acked": publisher.acked,
        "dead_lettered": publisher.dead_lettered,
        "publish_seconds": round(publish_s, 2),
        "total_seconds_incl_flush": round(total_s, 2),
        "events_per_second": round(n / total_s, 1),
    }
    print(json.dumps(result))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("mode", choices=["catchup", "live", "burst"])
    parser.add_argument("--events-per-second", type=float, default=0, help="0 = unlimited")
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument(
        "--from-go-live", action="store_true", help="ignore checkpoint and republish everything"
    )
    parser.add_argument(
        "--sync-ops-db", action="store_true", help="write customer/supplier changes to PostgreSQL"
    )
    parser.add_argument("--checkpoint-seconds", type=int, default=15)
    parser.add_argument("--metrics-port", type=int, default=8000)
    parser.add_argument("--scale", default=None, help="burst mode: generator scale")
    parser.add_argument("--days-back", type=int, default=3, help="burst mode: timeline start")
    args = parser.parse_args(argv)
    args.until = None
    if args.metrics_port:
        serve_metrics(args.metrics_port)
    with log_context("event-producer"):
        if args.mode == "burst":
            if not args.max_events:
                parser.error("burst mode requires --max-events")
            return run_burst(args)
        return run_timeline(args)


if __name__ == "__main__":
    sys.exit(main())
