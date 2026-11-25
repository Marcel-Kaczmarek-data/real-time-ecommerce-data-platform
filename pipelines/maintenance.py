"""Delta table maintenance: compaction, Z-ordering, VACUUM and retention.

    python -m pipelines.maintenance --optimize --vacuum --retention
    python -m pipelines.maintenance --optimize --table silver.orders

* OPTIMIZE compacts the small files produced by streaming micro-batches.
  ZORDER BY co-locates rows by the most selective filter / MERGE key.
* VACUUM removes files no longer referenced, older than the retention window
  (default 168 h). Time travel works only within that window.
* Retention deletes Bronze partitions older than the layer's retention period.
  Bronze is partitioned by processing_date, so these are cheap
  metadata-plus-file deletes.

OSS Delta vs Databricks: on Databricks, ``delta.autoOptimize.*`` / predictive
optimisation make most of this automatic. The same commands still run there.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from typing import Any

from pyspark.sql import SparkSession

from pipelines.common.logs import get_logger, log_context
from pipelines.common.metrics import track_job
from pipelines.common.spark import get_spark
from pipelines.common.tables import table, table_exists

log = get_logger("maintenance")

# table -> Z-order columns (empty = compaction only)
OPTIMIZE_PLAN: dict[str, list[str]] = {
    "bronze.clickstream_events": [],
    "bronze.orders_events": [],
    "bronze.payments_events": [],
    "bronze.inventory_events": [],
    "silver.orders": ["order_id"],
    "silver.order_items": ["order_id"],
    "silver.payments": ["order_id"],
    "silver.clickstream": ["session_id"],
    "silver.customer_sessions": ["customer_id"],
    "gold.fact_orders": ["order_date", "customer_id"],
    "gold.fact_order_items": ["order_date", "product_id"],
    "gold.fact_clickstream": ["session_id"],
}
BRONZE_RETENTION_DAYS = 90


def optimize(spark: SparkSession, fqn: str, zorder: list[str]) -> dict[str, Any]:
    ref = table(fqn)
    if not table_exists(spark, ref):
        return {"table": fqn, "skipped": "missing"}
    t0 = time.time()
    sql = f"OPTIMIZE {ref.qualified_name}" + (f" ZORDER BY ({', '.join(zorder)})" if zorder else "")
    metrics = spark.sql(sql).select("metrics.*").first()
    out = {
        "table": fqn,
        "files_removed": metrics["numFilesRemoved"],
        "files_added": metrics["numFilesAdded"],
        "seconds": round(time.time() - t0, 1),
    }
    log.info("optimize", extra=out)
    return out


def vacuum(spark: SparkSession, fqn: str, retain_hours: int) -> None:
    ref = table(fqn)
    if table_exists(spark, ref):
        spark.sql(f"VACUUM {ref.qualified_name} RETAIN {retain_hours} HOURS")
        log.info("vacuum", extra={"table": fqn, "retain_hours": retain_hours})


def apply_bronze_retention(spark: SparkSession, days: int) -> None:
    cutoff = date.today() - timedelta(days=days)
    for entity in (
        "orders",
        "order_items",
        "payments",
        "inventory",
        "shipments",
        "clickstream",
        "refunds",
        "returns",
        "customers",
    ):
        ref = table(f"bronze.{entity}_events")
        if table_exists(spark, ref):
            spark.sql(f"DELETE FROM {ref.qualified_name} WHERE processing_date < DATE'{cutoff}'")
    log.info("bronze retention applied", extra={"cutoff": str(cutoff)})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--optimize", action="store_true")
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--retention", action="store_true")
    parser.add_argument("--table", action="append", help="limit to these tables")
    parser.add_argument("--retain-hours", type=int, default=168)
    args = parser.parse_args(argv)
    spark = get_spark("maintenance")
    plan = {t: z for t, z in OPTIMIZE_PLAN.items() if not args.table or t in args.table}
    with log_context("maintenance"), track_job("delta_table_maintenance") as metrics:
        if args.retention:
            apply_bronze_retention(spark, BRONZE_RETENTION_DAYS)
        if args.optimize:
            for fqn, zorder in plan.items():
                result = optimize(spark, fqn, zorder)
                if "files_removed" in result:
                    metrics.set_rows(fqn, "files_compacted", int(result["files_removed"] or 0))
        if args.vacuum:
            for fqn in plan:
                vacuum(spark, fqn, args.retain_hours)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
