"""Legacy order-system exports (Parquet / CSV) dropped in the landing zone.

File arrival detection runs first; duplicate files are skipped by checksum in the registry and
corrupted files are quarantined by the loader."""

from __future__ import annotations

from airflow import DAG

from common.platform import BRONZE_BATCH, DAG_DEFAULTS, SILVER, default_args, platform_task

with DAG(
    dag_id="historical_order_ingestion",
    schedule="0 */6 * * *",
    default_args=default_args(timeout_minutes=60),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    detect = platform_task(
        "detect_new_files", "python -m ingestion.batch.file_arrival historical_orders/", spark=False
    )
    load = platform_task(
        "load_history_files",
        "python -m ingestion.batch.run --group history --skip-extract",
        outlets=[BRONZE_BATCH],
    )
    silver = platform_task(
        "merge_history_into_silver",
        "python -m pipelines.silver.run --job orders_history --job order_items_history --job payments_history",
        outlets=[SILVER],
    )
    detect >> load >> silver
