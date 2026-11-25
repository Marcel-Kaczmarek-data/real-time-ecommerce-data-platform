"""Reproducible platform benchmarks (measured, never estimated).

    python -m benchmarks.run --events 100000 --partitions 3 12 --cores 2 4
    python -m benchmarks.run --events 1000000 --partitions 12 --cores 4 --live-seconds 120

Every configuration runs in isolation: its own Kafka topics (prefix
``bench<run>p<P>.``), its own lakehouse root (``s3a://lakehouse/_bench/...``)
and its own checkpoints, so benchmarks never touch the demo data.

Stages
------
1. generator   simulate N events (pure Python) -> events/s
2. produce     Avro + Schema Registry + idempotent acks=all publish -> events/s, ack latency p50/p95/p99
3. bronze      Kafka -> Bronze Delta drain (availableNow) per Spark core count -> rows/s, CPU, peak RSS
4. live        steady-rate production while the Bronze stream runs continuously ->
               end-to-end latency (Kafka append -> Bronze commit) p50/p95/p99 and consumer lag
5. storage     Bronze bytes on object storage vs Kafka payload bytes
6. optimize    file count + query time before/after OPTIMIZE on the Bronze clickstream table
7. gold        DuckDB serving-layer query times on the main Gold layer

Results: benchmarks/results/<timestamp>.json and benchmarks/results/<timestamp>.md
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psutil

RESULTS_DIR = Path(__file__).resolve().parent / "results"


def pct(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
    return ordered[idx]


@dataclass
class ProcessSampler:
    """Samples CPU% and RSS of a process tree while a stage runs."""

    pid: int
    interval: float = 0.5
    cpu: list[float] = field(default_factory=list)
    rss: list[int] = field(default_factory=list)
    _stop: threading.Event = field(default_factory=threading.Event)

    def _tree(self) -> list[psutil.Process]:
        try:
            parent = psutil.Process(self.pid)
            return [parent, *parent.children(recursive=True)]
        except psutil.NoSuchProcess:
            return []

    def run(self) -> None:
        procs: dict[int, psutil.Process] = {}
        while not self._stop.is_set():
            total_cpu, total_rss = 0.0, 0
            for p in self._tree():
                proc = procs.setdefault(p.pid, p)
                try:
                    total_cpu += proc.cpu_percent(None)
                    total_rss += proc.memory_info().rss
                except psutil.Error:
                    continue
            self.cpu.append(total_cpu)
            self.rss.append(total_rss)
            time.sleep(self.interval)

    def __enter__(self) -> ProcessSampler:
        self._thread = threading.Thread(target=self.run, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        self._thread.join(timeout=2)

    def summary(self) -> dict[str, float | None]:
        busy = list(self.cpu[2:]) or self.cpu
        return {
            "cpu_avg_percent": round(statistics.mean(busy), 1) if busy else None,
            "cpu_peak_percent": round(max(busy), 1) if busy else None,
            "rss_peak_mb": round(max(self.rss) / 2**20, 1) if self.rss else None,
        }


def bench_env(prefix: str, root: str, ckpt: str, cores: int) -> dict[str, str]:
    return {
        **os.environ,
        "KAFKA_TOPIC_PREFIX": prefix,
        "LAKEHOUSE_ROOT": root,
        "CHECKPOINT_ROOT": ckpt,
        "SPARK_MASTER": f"local[{cores}]",
        "SPARK_DRIVER_MEMORY": os.environ.get("BENCH_DRIVER_MEMORY", "3g"),
        "PYTHONPATH": os.environ.get("PYTHONPATH", "/opt/platform"),
    }


def simulator(events_per_day: int, days_back: int = 30) -> Any:
    from data_generator.settings import get_settings
    from data_generator.simulator import JourneySimulator
    from data_generator.world import build_world

    settings = get_settings("lite", 4242, sessions_per_day=max(1000, events_per_day // 6))
    start = (datetime.now(UTC) - timedelta(days=days_back)).replace(minute=0, second=0, microsecond=0)
    return JourneySimulator(build_world(settings, start), start), start


# ----------------------------------------------------------------- stages --
def stage_generator(n: int) -> dict[str, Any]:
    sim, _ = simulator(events_per_day=n)
    t0 = time.perf_counter()
    count = 0
    for _ in sim.advance(datetime.now(UTC)):
        count += 1
        if count >= n:
            break
    secs = time.perf_counter() - t0
    return {"events": count, "seconds": round(secs, 2), "events_per_second": round(count / secs, 1)}


def stage_produce(n: int, prefix: str, partitions: int, rate: float = 0) -> dict[str, Any]:
    os.environ["KAFKA_TOPIC_PREFIX"] = prefix
    subprocess.run(
        [
            sys.executable,
            "-m",
            "ingestion.kafka_producers.admin",
            "bootstrap",
            "--partitions",
            str(partitions),
        ],
        check=True,
        env={**os.environ, "KAFKA_TOPIC_PREFIX": prefix},
        stdout=subprocess.DEVNULL,
    )
    # Point the shared topic mapping at the isolated benchmark topics (no module reloads:
    # Prometheus collectors must stay registered exactly once).
    import data_generator.events as ev
    import ingestion.kafka_producers.producer as prod

    ev.TOPICS.update({entity: f"{prefix}{name}" for entity, name in ev._BASE_TOPICS.items()})
    prod.DEAD_LETTER_TOPIC = f"{prefix}ecommerce.dead-letter"
    from ingestion.kafka_producers.run import RateLimiter

    sim, _ = simulator(events_per_day=n)
    publisher = prod.EventPublisher(record_latencies=True)
    limiter = RateLimiter(rate)
    t0 = time.perf_counter()
    count = 0
    for event in sim.advance(datetime.now(UTC)):
        limiter.acquire()
        publisher.publish(event)
        count += 1
        if count >= n:
            break
    publish_s = time.perf_counter() - t0
    publisher.flush(600)
    total_s = time.perf_counter() - t0
    lat = publisher.latencies or []
    return {
        "events": count,
        "acked": publisher.acked,
        "dead_lettered": publisher.dead_lettered,
        "seconds": round(total_s, 2),
        "publish_seconds": round(publish_s, 2),
        "events_per_second": round(count / total_s, 1),
        "payload_mb": round(publisher.bytes_sent / 2**20, 1),
        "ack_latency_ms": {"p50": _ms(pct(lat, 0.5)), "p95": _ms(pct(lat, 0.95)), "p99": _ms(pct(lat, 0.99))},
    }


def _ms(v: float | None) -> float | None:
    return None if v is None else round(v * 1000, 2)


def bronze_rows(root: str) -> tuple[int, int]:
    """(rows, bytes) across the Bronze event tables of a lakehouse root."""
    from deltalake import DeltaTable

    from pipelines.common.storage import delta_rs_storage_options

    rows = size = 0
    for entity in (
        "customers",
        "orders",
        "order_items",
        "payments",
        "inventory",
        "shipments",
        "clickstream",
        "refunds",
        "returns",
    ):
        uri = root.replace("s3a://", "s3://") + f"/bronze/{entity}_events"
        try:
            actions = DeltaTable(uri, storage_options=delta_rs_storage_options()).get_add_actions(
                flatten=True
            )
        except Exception:  # noqa: S112 - table absent for this configuration
            continue
        if "num_records" not in actions.schema.names:  # empty table (no data files yet)
            continue
        rows += sum(int(x) for x in actions.column("num_records").to_pylist())
        size += sum(int(x) for x in actions.column("size_bytes").to_pylist())
    return rows, size


def stage_bronze(prefix: str, root: str, ckpt: str, cores: int) -> dict[str, Any]:
    env = bench_env(prefix, root, ckpt, cores)
    t0 = time.perf_counter()
    proc = subprocess.Popen(
        [sys.executable, "-m", "pipelines.bronze.stream_kafka_to_bronze", "--once", "--no-realtime"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    with ProcessSampler(proc.pid) as sampler:
        rc = proc.wait()
    secs = time.perf_counter() - t0
    if rc != 0:
        raise RuntimeError(f"bronze drain failed with exit code {rc}")
    rows, size = bronze_rows(root)
    return {
        "cores": cores,
        "rows": rows,
        "seconds": round(secs, 2),
        "rows_per_second": round(rows / secs, 1),
        "bronze_mb": round(size / 2**20, 1),
        **sampler.summary(),
        "note": "includes ~10-15 s JVM/Spark start-up",
    }


def stage_live(prefix: str, root: str, ckpt: str, cores: int, rate: float, seconds: int) -> dict[str, Any]:
    """Steady-rate production while the stream runs continuously -> true end-to-end latency."""
    env = bench_env(prefix, root, ckpt, cores) | {"STREAM_METRICS_PORT": "0"}
    # Topics must exist before the stream subscribes to them.
    subprocess.run(
        [sys.executable, "-m", "ingestion.kafka_producers.admin", "bootstrap", "--partitions", "6"],
        check=True,
        env=env,
        stdout=subprocess.DEVNULL,
    )
    stream = subprocess.Popen(
        [sys.executable, "-m", "pipelines.bronze.stream_kafka_to_bronze", "--no-realtime"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(45)  # JVM start + first (empty) micro-batch
        produced = stage_produce(int(rate * seconds), prefix, partitions=6, rate=rate)
        time.sleep(30)  # let the last micro-batches commit
    finally:
        stream.terminate()
        stream.wait(timeout=60)
    import duckdb
    from deltalake import DeltaTable

    from pipelines.common.storage import delta_rs_storage_options

    con = duckdb.connect()
    frames = []
    for entity in ("orders", "clickstream", "payments", "inventory", "order_items", "shipments"):
        try:
            dt = DeltaTable(
                root.replace("s3a://", "s3://") + f"/bronze/{entity}_events",
                storage_options=delta_rs_storage_options(),
            )
            frames.append(dt.to_pyarrow_table(columns=["kafka_timestamp", "ingestion_timestamp"]))
        except Exception:  # noqa: S112
            continue
    import pyarrow as pa

    con.register("t", pa.concat_tables(frames))
    row = con.sql("""SELECT count(*),
                            quantile_cont(epoch(ingestion_timestamp) - epoch(kafka_timestamp), 0.5),
                            quantile_cont(epoch(ingestion_timestamp) - epoch(kafka_timestamp), 0.95),
                            quantile_cont(epoch(ingestion_timestamp) - epoch(kafka_timestamp), 0.99),
                            max(epoch(ingestion_timestamp) - epoch(kafka_timestamp))
                     FROM t""").fetchone()
    return {
        "rate_events_per_second": rate,
        "duration_seconds": seconds,
        "produced": produced["events"],
        "rows_in_bronze": row[0],
        "trigger_seconds": 10,
        "end_to_end_latency_s": {"p50": _r(row[1]), "p95": _r(row[2]), "p99": _r(row[3]), "max": _r(row[4])},
        "producer_ack_latency_ms": produced["ack_latency_ms"],
        "definition": "Kafka append time -> Bronze Delta commit (includes the 10 s micro-batch trigger)",
    }


def _r(v: float | None) -> float | None:
    return None if v is None else round(float(v), 2)


def stage_optimize(prefix: str, root: str, ckpt: str) -> dict[str, Any]:
    import duckdb
    from deltalake import DeltaTable

    from pipelines.common.storage import delta_rs_storage_options

    uri = root.replace("s3a://", "s3://") + "/bronze/clickstream_events"
    query = (
        "SELECT event_type, count(*) FROM t WHERE kafka_partition = 1 AND event_type = 'add_to_cart' "
        "GROUP BY 1"
    )

    def measure() -> dict[str, Any]:
        dt = DeltaTable(uri, storage_options=delta_rs_storage_options())
        files = len(dt.files())
        con = duckdb.connect()
        con.register("t", dt.to_pyarrow_dataset())
        times = []
        for _ in range(5):
            t0 = time.perf_counter()
            con.sql(query).fetchall()
            times.append(time.perf_counter() - t0)
        return {"files": files, "query_ms_median": round(statistics.median(times) * 1000, 1)}

    before = measure()
    env = bench_env(prefix, root, ckpt, 4)
    t0 = time.perf_counter()
    subprocess.run(
        [sys.executable, "-m", "pipelines.maintenance", "--optimize", "--table", "bronze.clickstream_events"],
        env=env,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    optimize_s = time.perf_counter() - t0
    after = measure()
    return {
        "table": "bronze.clickstream_events",
        "before": before,
        "after": after,
        "optimize_seconds": round(optimize_s, 1),
        "query": query,
    }


GOLD_QUERIES = {
    "executive_kpis_30d": "SELECT sum(net_revenue_usd), sum(paid_orders) FROM mart_daily_sales "
    "WHERE date >= current_date - 30",
    "revenue_by_country": "SELECT country_code, sum(net_revenue_usd) FROM mart_daily_sales GROUP BY 1",
    "top_products_from_fact": "SELECT product_id, sum(line_net_amount_usd) r FROM fact_order_items WHERE is_paid "
    "GROUP BY 1 ORDER BY r DESC LIMIT 10",
    "customer_360_lookup": "SELECT * FROM mart_customer_360 WHERE customer_id = 'CUST-0000042'",
    "orders_fact_full_scan": "SELECT sales_channel, count(*), sum(gross_revenue_usd) FROM fact_orders GROUP BY 1",
}


def stage_gold() -> dict[str, Any]:
    os.environ.pop("LAKEHOUSE_ROOT", None)
    from serving.warehouse import GoldWarehouse

    wh = GoldWarehouse()
    out = {}
    for name, sql in GOLD_QUERIES.items():
        wh.query(sql)  # warm-up: table registration
        times = []
        for _ in range(5):
            t0 = time.perf_counter()
            wh.query(sql)
            times.append(time.perf_counter() - t0)
        out[name] = {
            "median_ms": round(statistics.median(times) * 1000, 1),
            "p95_ms": round((pct(times, 0.95) or 0) * 1000, 1),
        }
    return out


def environment() -> dict[str, Any]:
    import pyspark

    cpu_model = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    return {
        "timestamp_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "host_cpu": cpu_model,
        "logical_cpus_visible": psutil.cpu_count(),
        "memory_visible_gb": round(psutil.virtual_memory().total / 2**30, 1),
        "os": platform.platform(),
        "python": platform.python_version(),
        "pyspark": pyspark.__version__,
        "kafka": "Confluent Platform 7.8.0 (single broker, KRaft)",
        "object_storage": "SeaweedFS 3.85 (single node)",
        "note": "Docker Desktop VM on a Windows 11 host; all services on one machine",
    }


def write_report(result: dict[str, Any]) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    base = RESULTS_DIR / f"{stamp}_{result['config']['events']}"
    base.with_suffix(".json").write_text(json.dumps(result, indent=2), encoding="utf-8", newline="\n")
    env, cfg = result["environment"], result["config"]
    lines = [
        f"# Benchmark {stamp} - {cfg['events']:,} events",
        "",
        f"Host: {env['host_cpu']} · {env['logical_cpus_visible']} vCPU / {env['memory_visible_gb']} GB visible "
        f"to Docker · PySpark {env['pyspark']} · {env['kafka']} · {env['object_storage']}",
        "",
    ]
    if "generator" in result:
        g = result["generator"]
        lines += [
            f"**Generator:** {g['events']:,} events in {g['seconds']} s = **{g['events_per_second']:,.0f} events/s**",
            "",
        ]
    lines += [
        "| Partitions | Producer events/s | Ack p50 / p95 / p99 (ms) | Spark cores | Bronze rows/s | "
        "Drain time (s) | CPU avg / peak % | Peak RSS (MB) | Bronze size (MB) |",
        "|---:|---:|---|---:|---:|---:|---|---:|---:|",
    ]
    for run in result["runs"]:
        p = run["produce"]
        for b in run["bronze"]:
            lat = p["ack_latency_ms"]
            lines.append(
                f"| {run['partitions']} | {p['events_per_second']:,.0f} | {lat['p50']} / {lat['p95']} / "
                f"{lat['p99']} | {b['cores']} | {b['rows_per_second']:,.0f} | {b['seconds']} | "
                f"{b['cpu_avg_percent']} / {b['cpu_peak_percent']} | {b['rss_peak_mb']} | {b['bronze_mb']} |"
            )
    if "live" in result:
        lv = result["live"]
        e = lv["end_to_end_latency_s"]
        lines += [
            "",
            f"**Live end-to-end latency** at {lv['rate_events_per_second']} events/s for "
            f"{lv['duration_seconds']} s ({lv['rows_in_bronze']:,} rows): p50 **{e['p50']} s**, "
            f"p95 **{e['p95']} s**, p99 **{e['p99']} s**, max {e['max']} s ({lv['definition']}).",
        ]
    if "optimize" in result:
        o = result["optimize"]
        lines += [
            "",
            f"**OPTIMIZE** `{o['table']}`: files {o['before']['files']} → {o['after']['files']}, "
            f"query median {o['before']['query_ms_median']} ms → {o['after']['query_ms_median']} ms "
            f"(optimize took {o['optimize_seconds']} s).",
        ]
    if "gold" in result:
        lines += ["", "| Gold serving query (DuckDB over Delta) | median ms | p95 ms |", "|---|---:|---:|"]
        lines += [f"| {k} | {v['median_ms']} | {v['p95_ms']} |" for k, v in result["gold"].items()]
    base.with_suffix(".md").write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    return base.with_suffix(".md")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--partitions", type=int, nargs="+", default=[6])
    parser.add_argument("--cores", type=int, nargs="+", default=[4])
    parser.add_argument("--live-seconds", type=int, default=0, help="0 = skip the live latency stage")
    parser.add_argument("--live-rate", type=float, default=200.0)
    parser.add_argument("--skip-gold", action="store_true")
    parser.add_argument("--skip-optimize", action="store_true")
    args = parser.parse_args(argv)
    run_id = datetime.now(UTC).strftime("%m%d%H%M")
    result: dict[str, Any] = {"config": vars(args), "environment": environment(), "runs": []}
    print(f"[bench] generator {args.events:,}", flush=True)
    result["generator"] = stage_generator(args.events)
    last = None
    for partitions in args.partitions:
        prefix = f"bench{run_id}p{partitions}."
        print(f"[bench] produce {args.events:,} -> {prefix}* ({partitions} partitions)", flush=True)
        run: dict[str, Any] = {
            "partitions": partitions,
            "produce": stage_produce(args.events, prefix, partitions),
            "bronze": [],
        }
        for cores in args.cores:
            root = f"s3a://lakehouse/_bench/{run_id}/p{partitions}c{cores}"
            ckpt = f"file:///tmp/bench-ckpt/{run_id}/p{partitions}c{cores}"
            print(f"[bench] bronze drain p={partitions} cores={cores}", flush=True)
            run["bronze"].append(stage_bronze(prefix, root, ckpt, cores))
            last = (prefix, root, ckpt)
        result["runs"].append(run)
        print(json.dumps(run, indent=1), flush=True)
    if last and not args.skip_optimize:
        print("[bench] optimize before/after", flush=True)
        result["optimize"] = stage_optimize(*last)
    if args.live_seconds:
        prefix = f"bench{run_id}live."
        print(f"[bench] live latency {args.live_rate} ev/s for {args.live_seconds}s", flush=True)
        result["live"] = stage_live(
            prefix,
            f"s3a://lakehouse/_bench/{run_id}/live",
            f"file:///tmp/bench-ckpt/{run_id}/live",
            max(args.cores),
            args.live_rate,
            args.live_seconds,
        )
    if not args.skip_gold:
        print("[bench] gold serving queries", flush=True)
        result["gold"] = stage_gold()
    report = write_report(result)
    print(report.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
