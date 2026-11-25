"""Silver processing entrypoint.

python -m pipelines.silver.run                       # all jobs, incremental (availableNow)
python -m pipelines.silver.run --job orders --job payments
python -m pipelines.silver.run --job clickstream --continuous   # long-running service mode
"""

from __future__ import annotations

import argparse
import sys
import time

from pipelines.common.logs import get_logger, log_context
from pipelines.common.metrics import track_job
from pipelines.common.spark import get_spark
from pipelines.common.tables import read_table, table, table_exists
from pipelines.silver.jobs import ALL_JOBS, STREAM_JOBS, run_job

log = get_logger("silver-run")

SILVER_TABLES = [
    "customers",
    "customers_history",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory",
    "inventory_movements",
    "shipments",
    "clickstream",
    "refunds",
    "returns",
    "customer_sessions",
    "order_status_history",
    "exchange_rates",
    "marketing_spend",
    "supplier_deliveries",
    "countries",
    "currencies",
    "categories",
    "warehouses",
    "promotions",
    "suppliers",
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--job", action="append", choices=ALL_JOBS, help="default: all jobs in dependency order"
    )
    parser.add_argument(
        "--continuous", action="store_true", help="run a stream job continuously (service mode)"
    )
    args = parser.parse_args(argv)
    jobs = args.job or ALL_JOBS
    spark = get_spark("silver")
    with log_context("silver"), track_job("silver_processing") as metrics:
        if args.continuous:
            for name in jobs:
                if name not in STREAM_JOBS:
                    parser.error(f"--continuous only applies to stream jobs, not {name}")
                run_job(spark, name, once=False)
            spark.streams.awaitAnyTermination()
            return 0
        for name in jobs:
            t0 = time.time()
            result = run_job(spark, name)
            log.info(
                "silver job finished",
                extra={"job": name, "result": result, "seconds": round(time.time() - t0, 1)},
            )
        for name in SILVER_TABLES:
            ref = table(f"silver.{name}")
            if table_exists(spark, ref):
                n = read_table(spark, ref).count()
                metrics.set_rows(ref.fqn, "total", n)
                print(f"silver.{name:24s} {n:>10,}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
