"""Reference & master data ingestion (PostgreSQL ops schema + supplier file feed -> Bronze -> Silver).

Incremental (watermark on updated_at) for customers / supplier deliveries, full snapshots for small
reference tables. Feeds the SCD2 customer history."""

from __future__ import annotations

from airflow import DAG

from common.platform import BRONZE_BATCH, DAG_DEFAULTS, SILVER, default_args, platform_task

with DAG(
    dag_id="reference_data_ingestion",
    description="Customers, countries, currencies, warehouses, promotions, inventory snapshot, suppliers",
    schedule="15 * * * *",
    default_args=default_args(timeout_minutes=45, sla_minutes=40),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    ingest = platform_task(
        "ingest_reference_sources", "python -m ingestion.batch.run --group reference", outlets=[BRONZE_BATCH]
    )
    silver = platform_task(
        "refresh_silver_reference",
        "python -m pipelines.silver.run --job reference --job supplier_deliveries --job customers",
        outlets=[SILVER],
    )
    ingest >> silver
