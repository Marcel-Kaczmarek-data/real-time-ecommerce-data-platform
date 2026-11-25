"""Sales, product, funnel, fulfilment, payment, returns, marketing and executive marts, then publish
Gold to the PostgreSQL analytics schema for Power BI."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, GOLD_FACTS, GOLD_MARTS, default_args, platform_task

MARTS = [
    "mart_daily_sales",
    "mart_daily_category_sales",
    "mart_product_performance",
    "mart_conversion_funnel",
    "mart_inventory_health",
    "mart_order_fulfillment",
    "mart_payment_performance",
    "mart_returns_analysis",
    "mart_marketing_performance",
    "mart_executive_summary",
]

with DAG(
    dag_id="daily_sales_aggregation",
    schedule=[GOLD_FACTS],
    default_args=default_args(timeout_minutes=45, sla_minutes=60),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    marts = platform_task(
        "build_sales_marts",
        "python -m pipelines.gold.run --skip-quality-gate --stage marts "
        + " ".join(f"--mart {m}" for m in MARTS),
        outlets=[GOLD_MARTS],
    )
    publish = platform_task("publish_to_powerbi_serving", "python -m pipelines.gold.publish_serving")
    marts >> publish
