"""File arrival detection for landing prefixes.

    python -m ingestion.batch.file_arrival historical_orders/ [--min-age-seconds 60]

Lists objects under the prefix that are not yet LOADED / DUPLICATE in the
file registry. Files younger than ``--min-age-seconds`` are ignored, so a
partner upload still in progress is not picked up half-written.

Exit codes: 0 = new files found, 99 = nothing to do. Airflow's BashOperator
treats 99 as "skipped", so downstream tasks are skipped instead of failing.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from ingestion.batch.registry import Registry
from pipelines.common.config import load_config
from pipelines.common.db import connect
from pipelines.common.logs import get_logger
from pipelines.common.storage import s3_client

log = get_logger("file-arrival")
NOTHING_TO_DO = 99


def new_files(prefix: str, min_age_seconds: int = 60) -> list[str]:
    cfg = load_config()
    bucket = str(cfg.get_path("storage.landing_bucket"))
    s3 = s3_client(cfg)
    conn = connect(cfg)
    try:
        registry = Registry(conn)
        sources = {
            "historical_orders/": ["historical_orders", "historical_order_items", "historical_payments"]
        }
        known: set[str] = set()
        for source in sources.get(prefix, []):
            known |= registry.known_keys(source)
    finally:
        conn.close()
    now = datetime.now(UTC)
    found = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            age = (now - obj["LastModified"]).total_seconds()
            if obj["Key"] not in known and age >= min_age_seconds and not obj["Key"].endswith("/"):
                found.append(obj["Key"])
    return found


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("prefix")
    parser.add_argument("--min-age-seconds", type=int, default=60)
    args = parser.parse_args(argv)
    files = new_files(args.prefix, args.min_age_seconds)
    log.info("file arrival check", extra={"prefix": args.prefix, "new_files": len(files)})
    for key in files[:50]:
        print(key)
    return 0 if files else NOTHING_TO_DO


if __name__ == "__main__":
    sys.exit(main())
