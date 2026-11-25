"""Publish Gold marts and dimensions to the PostgreSQL ``analytics`` schema (Power BI source).

Power BI Desktop connects to PostgreSQL natively. Each table is written to a
staging table and swapped in one transaction (DROP + RENAME), so reports never
read a half-written table.

    python -m pipelines.gold.publish_serving
"""

from __future__ import annotations

import argparse
import sys

from pipelines.common.config import load_config
from pipelines.common.db import connect, jdbc_url, pg_params
from pipelines.common.logs import get_logger, log_context
from pipelines.common.metrics import track_job
from pipelines.common.spark import get_spark
from pipelines.common.tables import read_table, table, table_exists

log = get_logger("publish-serving")

PUBLISHED = [
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_category",
    "dim_country",
    "dim_sales_channel",
    "dim_warehouse",
    "dim_promotion",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_shipments",
    "fact_refunds",
    "fact_returns",
    "mart_daily_sales",
    "mart_daily_category_sales",
    "mart_product_performance",
    "mart_customer_360",
    "mart_customer_retention",
    "mart_customer_lifetime_value",
    "mart_conversion_funnel",
    "mart_inventory_health",
    "mart_order_fulfillment",
    "mart_payment_performance",
    "mart_returns_analysis",
    "mart_marketing_performance",
    "mart_executive_summary",
    "financial_reconciliation",
]
DROP_COLUMNS = {
    "_hash",
    "_gold_updated_at",
    "email_hash",
}  # internal / sensitive columns never leave the lakehouse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", action="append", choices=PUBLISHED)
    args = parser.parse_args(argv)
    cfg = load_config()
    params = pg_params(cfg)
    spark = get_spark("publish-serving")
    with log_context("publish-serving"), track_job("publish_serving") as metrics:
        conn = connect(cfg)
        for name in args.table or PUBLISHED:
            ref = table(f"gold.{name}")
            if not table_exists(spark, ref):
                continue
            df = read_table(spark, ref)
            df = df.drop(*[c for c in df.columns if c in DROP_COLUMNS])
            staging = f"analytics.{name}__staging"
            (
                df.write.format("jdbc")
                .mode("overwrite")
                .option("url", jdbc_url(cfg))
                .option("dbtable", staging)
                .option("user", params["user"])
                .option("password", params["password"])
                .option("driver", "org.postgresql.Driver")
                .option("batchsize", 10000)
                .option("truncate", "false")
                .save()
            )
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS analytics.{name}")
                cur.execute(f"ALTER TABLE {staging} RENAME TO {name}")
            conn.commit()
            n = df.count()
            metrics.set_rows(f"analytics.{name}", "published", n)
            print(f"analytics.{name:32s} {n:>10,}")
        conn.close()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
