"""Product catalogue ingestion: operational DB products + categories and the PIM CSV feed."""

from __future__ import annotations

from airflow import DAG

from common.platform import BRONZE_BATCH, DAG_DEFAULTS, SILVER, default_args, platform_task

with DAG(
    dag_id="product_catalog_ingestion",
    schedule="30 */2 * * *",
    default_args=default_args(timeout_minutes=45),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    ingest = platform_task(
        "ingest_catalog_sources", "python -m ingestion.batch.run --group catalog", outlets=[BRONZE_BATCH]
    )
    silver = platform_task(
        "refresh_silver_products",
        "python -m pipelines.silver.run --job reference --job products",
        outlets=[SILVER],
    )
    ingest >> silver
