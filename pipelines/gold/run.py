"""Gold processing entrypoint.

python -m pipelines.gold.run                     # dimensions -> facts -> marts
python -m pipelines.gold.run --stage facts --fact fact_orders
python -m pipelines.gold.run --stage marts --mart mart_customer_360
"""

from __future__ import annotations

import argparse
import sys

from pipelines.common.logs import get_logger, log_context
from pipelines.common.metrics import track_job
from pipelines.common.spark import get_spark
from pipelines.gold.dimensions import build_all_dimensions
from pipelines.gold.facts import FACT_LOADERS, load_all_facts
from pipelines.gold.marts import MART_ORDER, build_marts

log = get_logger("gold-run")
STAGES = ["dimensions", "facts", "marts"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--stage", action="append", choices=STAGES)
    parser.add_argument("--fact", action="append", choices=list(FACT_LOADERS))
    parser.add_argument("--mart", action="append", choices=MART_ORDER)
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="rebuild the selected facts from Silver (drops tables + watermarks)",
    )
    parser.add_argument(
        "--skip-quality-gate",
        action="store_true",
        help="DANGEROUS: promote even if critical Silver checks fail (incident use only)",
    )
    args = parser.parse_args(argv)
    stages = args.stage or STAGES
    spark = get_spark("gold")
    with log_context("gold"), track_job("gold_processing") as metrics:
        if not args.skip_quality_gate:
            from data_quality.gate import assert_promotable

            assert_promotable(spark, stage="gold")
        if "dimensions" in stages:
            for name, n in build_all_dimensions(spark).items():
                print(f"gold.{name:28s} {n}")
        if "facts" in stages:
            for name, n in load_all_facts(spark, args.fact, args.full_refresh).items():
                metrics.set_rows(f"gold.{name}", "merged", n)
                print(f"gold.{name:28s} merged={n:,}")
        if "marts" in stages:
            for name, n in build_marts(spark, args.mart).items():
                metrics.set_rows(f"gold.{name}", "total", n)
                print(f"gold.{name:28s} rows={n:,}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
