"""Daily inventory position snapshot (replaceWhere on the snapshot date: idempotent re-runs)."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, default_args, platform_task

with DAG(
    dag_id="inventory_snapshot",
    schedule="55 23 * * *",
    default_args=default_args(timeout_minutes=30, sla_minutes=30),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    position = platform_task(
        "compute_inventory_position",
        "python -m pipelines.silver.run --job inventory_movements --job inventory",
    )
    snapshot = platform_task(
        "write_daily_snapshot", "python -m pipelines.gold.run --stage facts --fact fact_inventory_snapshots"
    )
    health = platform_task(
        "inventory_health_mart",
        "python -m pipelines.gold.run --skip-quality-gate --stage marts --mart mart_inventory_health",
    )
    position >> snapshot >> health
