"""FX rates and marketing spend from the reference REST API (retries with exponential back-off)."""

from __future__ import annotations

from airflow import DAG

from common.platform import BRONZE_BATCH, DAG_DEFAULTS, SILVER, default_args, platform_task

with DAG(
    dag_id="currency_rate_ingestion",
    schedule="5 0,6,12,18 * * *",
    default_args=default_args(retries=5, timeout_minutes=20, sla_minutes=30),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    fetch = platform_task(
        "fetch_fx_and_campaigns",
        "python -m ingestion.batch.run --group fx --group marketing",
        outlets=[BRONZE_BATCH],
    )
    silver = platform_task(
        "refresh_silver_fx",
        "python -m pipelines.silver.run --job exchange_rates --job marketing",
        outlets=[SILVER],
    )
    fetch >> silver
