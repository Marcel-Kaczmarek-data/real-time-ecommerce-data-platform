"""Rebuild Silver tables by replaying Bronze (historical data replay).

Bronze is immutable and complete, so any Silver table can be recomputed after
a transformation bug fix. The script drops the selected Silver tables and
their streaming checkpoints, then reruns the Silver jobs. Gold must be
refreshed afterwards (Airflow ``historical_backfill`` or ``make transform``).

    python scripts/replay_bronze.py --job orders --job payments --yes
    python scripts/replay_bronze.py --all-streams --yes
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

from pipelines.common.config import load_config
from pipelines.common.logs import get_logger
from pipelines.common.storage import s3_client
from pipelines.common.tables import checkpoint, table
from pipelines.silver.jobs import STREAM_JOBS

log = get_logger("replay")

# Silver tables written by each stream job (a table shared by several jobs is
# rebuilt only when all of its jobs are replayed together).
JOB_TABLES = {
    "orders": ["orders", "order_status_history"],
    "orders_history": ["orders"],
    "order_items": ["order_items"],
    "order_items_history": ["order_items"],
    "payments": ["payments"],
    "payments_history": ["payments"],
    "shipments": ["shipments"],
    "inventory_movements": ["inventory_movements"],
    "refunds": ["refunds"],
    "returns": ["returns"],
    "clickstream": ["clickstream", "customer_sessions"],
}


def expand(jobs: list[str]) -> list[str]:
    """Replaying a shared table requires all jobs that feed it."""
    selected = set(jobs)
    changed = True
    while changed:
        changed = False
        tables = {t for j in selected for t in JOB_TABLES[j]}
        for job, job_tables in JOB_TABLES.items():
            if job not in selected and set(job_tables) & tables:
                selected.add(job)
                changed = True
    return [j for j in STREAM_JOBS if j in selected]


def drop_prefix(bucket: str, prefix: str) -> int:
    s3 = s3_client()
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
            n += 1
    return n


def drop_checkpoint(path: str) -> None:
    if path.startswith("file://"):
        local = Path(path[len("file://") :])
        if local.exists():
            shutil.rmtree(local)
    elif path.startswith("s3"):
        bucket, _, key = path.split("://", 1)[1].partition("/")
        drop_prefix(bucket, key.rstrip("/") + "/")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--job", action="append", default=[], choices=list(STREAM_JOBS))
    parser.add_argument("--all-streams", action="store_true")
    parser.add_argument("--yes", action="store_true", help="confirm dropping derived Silver data")
    parser.add_argument("--no-run", action="store_true", help="only reset, do not reprocess")
    args = parser.parse_args(argv)
    jobs = expand(list(STREAM_JOBS) if args.all_streams else args.job)
    if not jobs:
        parser.error("select --job or --all-streams")
    tables = sorted({t for j in jobs for t in JOB_TABLES[j]})
    print(f"jobs to replay: {jobs}\nsilver tables to rebuild: {tables}")
    if not args.yes:
        print("dry run - pass --yes to drop and rebuild")
        return 0
    cfg = load_config()
    bucket = str(cfg.get_path("storage.lakehouse_bucket"))
    for t in tables:
        ref = table(f"silver.{t}")
        n = drop_prefix(bucket, f"silver/{t}/")
        log.info("silver table dropped", extra={"table": ref.fqn, "objects": n})
    for j in jobs:
        drop_checkpoint(checkpoint(f"silver/{j}"))
    if args.no_run:
        return 0
    from pipelines.silver.run import main as silver_main

    run_args: list[str] = []
    for j in jobs:
        run_args += ["--job", j]
    return silver_main(run_args)


if __name__ == "__main__":
    sys.exit(main())
