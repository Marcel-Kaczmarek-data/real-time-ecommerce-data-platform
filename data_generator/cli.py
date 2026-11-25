"""Command line interface for the synthetic data generator.

    python -m data_generator.cli seed   --history-days 120 --stream-days 7
    python -m data_generator.cli export --days 2 --local-dir sample_data/generated
    python -m data_generator.cli sample             # refresh committed samples in sample_data/

``seed`` creates the operational state at go-live: master data and opening
inventory in PostgreSQL, legacy order history exports and partner files in the
landing bucket, plus the simulation anchor that the live producer uses to
continue the same deterministic timeline.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from data_generator.reference import promotions_between
from data_generator.settings import get_settings
from data_generator.simulator import JourneySimulator
from data_generator.sinks import (
    HistoryCollector,
    Landing,
    SimulationAnchor,
    load_operational_db,
    to_json,
    write_partner_files,
)
from data_generator.world import build_world
from pipelines.common.logs import get_logger, log_context

log = get_logger("data_generator")
ANCHOR_KEY = "_simulation/anchor.json"
REPO_ROOT = Path(__file__).resolve().parents[1]


def default_anchor(seed: int, scale: str, history_days: int, stream_days: int) -> SimulationAnchor:
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    go_live = today - timedelta(days=stream_days)
    return SimulationAnchor(
        seed=seed, scale=scale, history_start=go_live - timedelta(days=history_days), go_live=go_live
    )


def load_anchor(landing: Landing) -> SimulationAnchor:
    raw = landing.get(ANCHOR_KEY)
    if raw is None:
        raise SystemExit("No simulation anchor found - run `make seed` (data_generator.cli seed) first.")
    return SimulationAnchor.from_dict(json.loads(raw))


def build_simulator(anchor: SimulationAnchor) -> JourneySimulator:
    settings = get_settings(anchor.scale, anchor.seed)
    world = build_world(settings, anchor.history_start)
    # Behavioural tracking (clickstream) only exists since the platform go-live.
    return JourneySimulator(world, anchor.history_start, clickstream_from=anchor.go_live)


def cmd_seed(args: argparse.Namespace) -> int:
    settings = get_settings(args.scale, args.seed)
    anchor = default_anchor(settings.seed, settings.scale, args.history_days, args.stream_days)
    landing = Landing(Path(args.local_dir)) if args.local_dir else Landing()
    started = time.time()
    with log_context("seed"):
        log.info("building world", extra={"scale": settings.scale, "seed": settings.seed, **anchor.to_dict()})
        sim = build_simulator(anchor)
        history = HistoryCollector()
        n = 0
        for event in sim.advance(anchor.go_live):
            history.add(event)
            n += 1
        log.info("history simulated", extra={"events": n, "orders": len(history.orders)})
        files = history.write(landing, anchor.go_live.date())
        files += write_partner_files(sim.world, landing, anchor.go_live.date())
        if not args.skip_db:
            from pipelines.common.db import connect

            promos = [
                {**p.__dict__, "category_id": None}
                for p in promotions_between(
                    anchor.history_start.date(), date(anchor.go_live.year + 1, 12, 31)
                )
            ]
            deliveries = [d.record() for d in sim.supplier_deliveries]
            conn = connect()
            try:
                counts = load_operational_db(conn, sim.world, deliveries, promos, anchor.go_live)
            finally:
                conn.close()
            log.info("operational database loaded", extra={"rows": counts})
        landing.put(ANCHOR_KEY, json.dumps(anchor.to_dict(), indent=2).encode())
        log.info("seed complete", extra={"files": len(files), "seconds": round(time.time() - started, 1)})
    print(
        json.dumps(
            {"anchor": anchor.to_dict(), "history_orders": len(history.orders), "files": len(files)}, indent=2
        )
    )
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Write N days of events as JSON lines (one file per topic) - for tests and offline replay."""
    settings = get_settings(args.scale, args.seed)
    start = (
        datetime.fromisoformat(args.start).replace(tzinfo=UTC)
        if args.start
        else (
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=args.days)
        )
    )
    world = build_world(settings, start)
    sim = JourneySimulator(world, start, inject_errors=not args.clean)
    out = Path(args.local_dir)
    out.mkdir(parents=True, exist_ok=True)
    handles: dict[str, Any] = {}
    n = 0
    try:
        for event in sim.advance(start + timedelta(days=args.days)):
            fh = handles.get(event.entity) or (out / f"{event.entity}.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            )
            handles[event.entity] = fh
            fh.write(
                to_json({**event.envelope(), "_emit_at": event.emit_at, "_defect": event.injected_defect})
                + "\n"
            )
            n += 1
    finally:
        for fh in handles.values():
            fh.close()
    print(json.dumps({"events": n, "directory": str(out), "stats": dict(sim.stats)}, indent=2, default=str))
    return 0


def cmd_sample(args: argparse.Namespace) -> int:
    """Small, committed, human-readable samples of every dataset (sample_data/)."""
    settings = get_settings("test", 7)
    start = datetime(2026, 11, 23, tzinfo=UTC)
    world = build_world(settings, start)
    sim = JourneySimulator(world, start, inject_errors=False)
    out = REPO_ROOT / "sample_data"
    events: dict[str, list[str]] = {}
    for event in sim.advance(start + timedelta(days=2)):
        bucket = events.setdefault(event.entity, [])
        if len(bucket) < args.rows:
            bucket.append(to_json(event.envelope()))
    (out / "events").mkdir(parents=True, exist_ok=True)
    for entity, lines in events.items():
        (out / "events" / f"{entity}.jsonl").write_text(
            "\n".join(lines) + "\n", encoding="utf-8", newline="\n"
        )
    from data_generator.sinks import csv_bytes

    (out / "reference").mkdir(parents=True, exist_ok=True)
    (out / "reference" / "customers.csv").write_bytes(
        csv_bytes([c.record() for c in world.customers[: args.rows]])
    )
    (out / "reference" / "products.csv").write_bytes(
        csv_bytes([p.record() for p in world.products[: args.rows]])
    )
    (out / "reference" / "categories.csv").write_bytes(csv_bytes([c.record() for c in world.categories]))
    (out / "reference" / "warehouses.csv").write_bytes(csv_bytes([w.record() for w in world.warehouses]))
    (out / "reference" / "countries.csv").write_bytes(csv_bytes(world.country_records()))
    print(f"samples written to {out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ecom-generate", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--scale", choices=["test", "lite", "full"], default=None)
    parser.add_argument("--seed", type=int, default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    p_seed = sub.add_parser("seed", help="seed PostgreSQL + landing zone and fix the simulation anchor")
    p_seed.add_argument("--history-days", type=int, default=120)
    p_seed.add_argument("--stream-days", type=int, default=7)
    p_seed.add_argument(
        "--local-dir", default=None, help="write landing files to a local directory instead of S3"
    )
    p_seed.add_argument("--skip-db", action="store_true")
    p_seed.set_defaults(func=cmd_seed)

    p_export = sub.add_parser("export", help="write events to JSON lines files")
    p_export.add_argument("--days", type=int, default=1)
    p_export.add_argument("--start", default=None, help="ISO date (UTC); default: N days ago")
    p_export.add_argument("--local-dir", default="sample_data/generated")
    p_export.add_argument("--clean", action="store_true", help="disable defect injection")
    p_export.set_defaults(func=cmd_export)

    p_sample = sub.add_parser("sample", help="refresh committed sample datasets")
    p_sample.add_argument("--rows", type=int, default=25)
    p_sample.set_defaults(func=cmd_sample)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
