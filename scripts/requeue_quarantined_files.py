"""Move quarantined landing files back to their original keys for reprocessing.

Use after fixing the root cause (corrected file, loader fix). The loader
retries files whose registry status is FAILED when they reappear.

    python scripts/requeue_quarantined_files.py --source-prefix historical_orders/ [--dry-run]
"""

from __future__ import annotations

import argparse
import sys

from pipelines.common.config import load_config
from pipelines.common.storage import s3_client


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--source-prefix", required=True, help="landing prefix, e.g. historical_orders/")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    cfg = load_config()
    bucket = str(cfg.get_path("storage.landing_bucket"))
    s3 = s3_client(cfg)
    moved = 0
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"quarantine/{args.source_prefix}"
    ):
        for obj in page.get("Contents", []):
            original = obj["Key"][len("quarantine/") :]
            print(("would move " if args.dry_run else "moving ") + f"{obj['Key']} -> {original}")
            if not args.dry_run:
                s3.copy_object(Bucket=bucket, Key=original, CopySource={"Bucket": bucket, "Key": obj["Key"]})
                s3.delete_object(Bucket=bucket, Key=obj["Key"])
            moved += 1
    print(f"{moved} file(s) {'to requeue' if args.dry_run else 'requeued'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
