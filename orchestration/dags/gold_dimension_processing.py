"""Gold dimensions (SCD1 + SCD2 customer / product, unknown and inferred members).

Triggered by Silver updates (data-aware scheduling) and guarded by the critical data-quality gate."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, GOLD_DIMENSIONS, SILVER, default_args, platform_task

with DAG(
    dag_id="gold_dimension_processing",
    schedule=[SILVER],
    default_args=default_args(timeout_minutes=45, sla_minutes=45),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    platform_task(
        "build_dimensions", "python -m pipelines.gold.run --stage dimensions", outlets=[GOLD_DIMENSIONS]
    )
