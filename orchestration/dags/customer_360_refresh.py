"""Customer 360, CLV and retention marts (after every fact load)."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, GOLD_FACTS, GOLD_MARTS, default_args, platform_task

with DAG(
    dag_id="customer_360_refresh",
    schedule=[GOLD_FACTS],
    default_args=default_args(timeout_minutes=30),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    platform_task(
        "build_customer_marts",
        "python -m pipelines.gold.run --skip-quality-gate --stage marts --mart mart_customer_360 "
        "--mart mart_customer_lifetime_value --mart mart_customer_retention",
        outlets=[GOLD_MARTS],
    )
