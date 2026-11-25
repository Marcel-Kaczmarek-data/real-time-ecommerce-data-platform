"""Daily financial reconciliation (orders vs payments vs refunds).

For each order date it compares:
  * paid order totals (fact_orders)    vs captured payment amounts (fact_payments)
  * refunds recorded per order         vs the order totals they refer to
  * order counts with exactly one capture

The output is ``gold.financial_reconciliation`` (one row per day). The job
exits non-zero when the absolute daily difference exceeds the tolerance, so
the Airflow task fails and alerts. Finance sign-off depends on this table.

    python -m pipelines.gold.reconciliation --tolerance-usd 1.0
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import functions as F

from pipelines.common.logs import get_logger, log_context
from pipelines.common.metrics import track_job
from pipelines.common.spark import get_spark
from pipelines.common.tables import read_table
from pipelines.gold.dimensions import overwrite

log = get_logger("reconciliation")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--tolerance-usd", type=float, default=1.0)
    args = parser.parse_args(argv)
    spark = get_spark("financial-reconciliation")
    with log_context("financial-reconciliation"), track_job("financial_reconciliation") as metrics:
        orders = read_table(spark, "gold.fact_orders").filter("is_paid")
        captured = (
            read_table(spark, "gold.fact_payments")
            .filter("is_success")
            .groupBy("order_id")
            .agg(F.sum("payment_amount_usd").alias("captured_usd"), F.count("*").alias("captures"))
        )
        refunds = (
            read_table(spark, "gold.fact_refunds")
            .groupBy("order_id")
            .agg(F.sum("refund_amount_usd").alias("refunded_usd"))
        )
        joined = (
            orders.select("order_id", "order_date", "total_amount_usd")
            .join(captured, "order_id", "left")
            .join(refunds, "order_id", "left")
            .fillna({"captured_usd": 0.0, "captures": 0, "refunded_usd": 0.0})
        )
        daily = (
            joined.groupBy("order_date")
            .agg(
                F.count("*").alias("paid_orders"),
                F.round(F.sum("total_amount_usd"), 2).alias("order_total_usd"),
                F.round(F.sum("captured_usd"), 2).alias("captured_usd"),
                F.round(F.sum("refunded_usd"), 2).alias("refunded_usd"),
                F.sum(F.when(F.col("captures") != 1, 1).otherwise(0)).alias("orders_without_single_capture"),
                F.sum(F.when(F.col("refunded_usd") > F.col("total_amount_usd") + 0.05, 1).otherwise(0)).alias(
                    "over_refunded_orders"
                ),
            )
            .withColumn("difference_usd", F.round(F.col("captured_usd") - F.col("order_total_usd"), 2))
            .withColumn(
                "is_reconciled",
                (F.abs("difference_usd") <= args.tolerance_usd)
                & (F.col("over_refunded_orders") == 0)
                & (F.col("orders_without_single_capture") == 0),
            )
            .withColumn("reconciled_at", F.current_timestamp())
        )
        n = overwrite(daily, "gold.financial_reconciliation")
        breaches = daily.filter(~F.col("is_reconciled")).collect()
        totals = daily.agg(F.sum("difference_usd"), F.sum("order_total_usd")).first()
        metrics.gauge(
            "ecom_reconciliation_difference_usd",
            "Captured minus paid order totals",
            {"scope": "all"},
            float(totals[0] or 0),
        )
        metrics.gauge(
            "ecom_reconciliation_breach_days", "Days outside tolerance", {"scope": "all"}, len(breaches)
        )
        print(
            f"reconciled {n} days; total paid orders {totals[1]:,.2f} USD; difference {totals[0]:,.2f} USD; "
            f"breach days: {len(breaches)}"
        )
        for b in breaches[:10]:
            print(
                f"  BREACH {b.order_date}: diff={b.difference_usd} no_single_capture={b.orders_without_single_capture} "
                f"over_refunded={b.over_refunded_orders}"
            )
    spark.stop()
    return 1 if breaches else 0


if __name__ == "__main__":
    sys.exit(main())
