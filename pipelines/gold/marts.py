"""Build Gold marts from the SQL models in ``data_models/marts/*.sql``.

Each SQL file is one mart. The files query temporary views named after Gold
tables (``fact_orders``, ``dim_customer``...) plus a few Silver helper views
(``silver_sessions``, ``silver_orders``, ``silver_marketing_spend``,
``silver_inventory``). Marts are small, fully recomputed aggregates, so they
are always consistent with the facts (overwrite, atomic per table).
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from pipelines.common.logs import get_logger
from pipelines.common.tables import read_table, table, table_exists
from pipelines.gold.dimensions import overwrite

log = get_logger("gold-marts")
MART_DIR = Path(__file__).resolve().parents[2] / "data_models" / "marts"

GOLD_VIEWS = [
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_category",
    "dim_supplier",
    "dim_warehouse",
    "dim_country",
    "dim_currency",
    "dim_sales_channel",
    "dim_promotion",
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_shipments",
    "fact_refunds",
    "fact_returns",
    "fact_clickstream",
    "fact_inventory_snapshots",
]
SILVER_VIEWS = {
    "silver_sessions": "silver.customer_sessions",
    "silver_orders": "silver.orders",
    "silver_marketing_spend": "silver.marketing_spend",
    "silver_inventory": "silver.inventory",
}

# Dependency order: customer_360 feeds the CLV and executive marts.
MART_ORDER = [
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
]


def register_views(spark: SparkSession) -> None:
    for name in GOLD_VIEWS:
        if table_exists(spark, table(f"gold.{name}")):
            read_table(spark, f"gold.{name}").createOrReplaceTempView(name)
    for view, fqn in SILVER_VIEWS.items():
        if table_exists(spark, table(fqn)):
            read_table(spark, fqn).createOrReplaceTempView(view)


def build_marts(spark: SparkSession, names: list[str] | None = None) -> dict[str, int]:
    register_views(spark)
    out: dict[str, int] = {}
    for name in names or MART_ORDER:
        sql = (MART_DIR / f"{name}.sql").read_text(encoding="utf-8")
        df = spark.sql(sql)
        out[name] = overwrite(df, f"gold.{name}")
        read_table(spark, f"gold.{name}").createOrReplaceTempView(name)  # later marts may build on it
        log.info("mart built", extra={"mart": name, "rows": out[name]})
    return out
