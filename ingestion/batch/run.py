"""Batch ingestion entrypoint.

    python -m ingestion.batch.run --group reference
    python -m ingestion.batch.run --source ops_customers --source fx_rates
    python -m ingestion.batch.run --all
    python -m ingestion.batch.run --source fx_rates --start-date 2026-07-01 --end-date 2026-07-31   # backfill

Per source: extract (PostgreSQL / REST -> landing) -> load (landing -> Bronze)
-> commit watermark. File sources skip the extract step (files are dropped by
partners). Each source is isolated: a failure is recorded and the remaining
sources still run; the process exits non-zero if any source failed.
"""

from __future__ import annotations

import argparse
import sys
import uuid
from datetime import UTC, date, datetime
from typing import Any

from data_generator.sinks import Landing
from ingestion.batch.extract import extract_postgres, extract_rest
from ingestion.batch.loader import load_source
from ingestion.batch.registry import Registry
from pipelines.common.config import CONFIG_DIR, _resolve, load_config, load_yaml
from pipelines.common.db import connect
from pipelines.common.logs import get_logger, log_context
from pipelines.common.metrics import track_job
from pipelines.common.spark import get_spark
from pipelines.common.storage import s3_client

log = get_logger("batch-ingestion")


def load_catalogue() -> dict[str, Any]:
    return _resolve(load_yaml(CONFIG_DIR / "batch_sources.yaml"))


def select_sources(
    catalogue: dict[str, Any], names: list[str], groups: list[str], all_: bool
) -> dict[str, Any]:
    sources = catalogue["sources"]
    if all_:
        return dict(sources)
    chosen = {n: sources[n] for n in names}
    for g in groups:
        chosen.update({n: s for n, s in sources.items() if s["group"] == g})
    unknown = set(names) - set(sources)
    if unknown:
        raise SystemExit(f"unknown sources: {sorted(unknown)}")
    if not chosen:
        raise SystemExit("no sources selected (use --source, --group or --all)")
    return chosen


def run(
    names: list[str],
    groups: list[str],
    all_: bool,
    start: date | None,
    end: date | None,
    skip_extract: bool = False,
) -> int:
    cfg = load_config()
    catalogue = load_catalogue()
    sources = select_sources(catalogue, names, groups, all_)
    bucket = str(cfg.get_path("storage.landing_bucket"))
    landing = Landing()
    s3 = s3_client(cfg)
    conn = connect(cfg)
    registry = Registry(conn)
    run_id = f"batch-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    registry.start_run(run_id, "batch_ingestion")
    spark = None
    failures: dict[str, str] = {}
    summary: dict[str, Any] = {}
    run_ts = datetime.now(UTC)
    with track_job("batch_ingestion") as metrics:
        for name, source in sources.items():
            with log_context(f"ingest:{name}"):
                try:
                    if not skip_extract:
                        if source["type"] == "postgres":
                            extract_postgres(conn, registry, landing, name, source, run_ts)
                        elif source["type"] == "rest":
                            extract_rest(
                                registry,
                                landing,
                                name,
                                source,
                                catalogue["rest"],
                                start,
                                end or run_ts.date(),
                            )
                    spark = spark or get_spark("batch-ingestion")
                    result = load_source(spark, registry, s3, bucket, name, source)
                    if result.files_failed:
                        failures[name] = "; ".join(result.errors)
                    elif source.get("mode") == "incremental" or source["type"] == "rest":
                        registry.commit_watermark(name, result.rows_loaded)
                    summary[name] = result.__dict__
                    metrics.set_rows(source["target"], "loaded", result.rows_loaded)
                    metrics.set_rows(source["target"], "quarantined", result.rows_quarantined)
                    metrics.gauge(
                        "ecom_ingestion_files",
                        "Files processed by status",
                        {"source": name, "status": "loaded"},
                        result.files_loaded,
                    )
                    metrics.gauge(
                        "ecom_ingestion_files",
                        "Files processed by status",
                        {"source": name, "status": "failed"},
                        result.files_failed,
                    )
                    metrics.gauge(
                        "ecom_ingestion_files",
                        "Files processed by status",
                        {"source": name, "status": "duplicate"},
                        result.files_duplicate,
                    )
                except Exception as exc:  # isolate sources; report at the end
                    log.exception("source failed", extra={"source": name})
                    failures[name] = f"{type(exc).__name__}: {exc}"
        rows = sum(int(v.get("rows_loaded", 0)) for v in summary.values())
        rejected = sum(int(v.get("rows_quarantined", 0)) for v in summary.values())
        registry.finish_run(
            run_id,
            "FAILED" if failures else "SUCCESS",
            rows,
            rows,
            rejected,
            {"sources": summary, "failures": failures},
            "; ".join(failures.values()) or None,
        )
        if spark is not None:
            spark.stop()
        conn.close()
        for name, info in summary.items():
            print(
                f"{name:28s} loaded={info['rows_loaded']:>8} files={info['files_loaded']} "
                f"dup={info['files_duplicate']} failed={info['files_failed']} quarantined={info['rows_quarantined']}"
            )
        if failures:
            raise RuntimeError(f"batch ingestion failed for: {sorted(failures)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source", action="append", default=[])
    parser.add_argument("--group", action="append", default=[])
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--start-date", type=date.fromisoformat, default=None, help="REST backfill start")
    parser.add_argument("--end-date", type=date.fromisoformat, default=None, help="REST backfill end")
    parser.add_argument("--skip-extract", action="store_true", help="only load files already in landing")
    args = parser.parse_args(argv)
    try:
        return run(args.source, args.group, args.all, args.start_date, args.end_date, args.skip_extract)
    except RuntimeError as exc:
        log.error(str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
