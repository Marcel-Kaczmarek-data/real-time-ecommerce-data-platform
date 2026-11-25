"""Nightly Delta maintenance: Bronze retention, OPTIMIZE / ZORDER compaction and VACUUM."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, default_args, platform_task

with DAG(
    dag_id="delta_table_maintenance",
    schedule="30 2 * * *",
    default_args=default_args(retries=1, timeout_minutes=120),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    retention = platform_task("apply_retention", "python -m pipelines.maintenance --retention")
    optimize = platform_task("optimize_and_zorder", "python -m pipelines.maintenance --optimize")
    vacuum = platform_task("vacuum", "python -m pipelines.maintenance --vacuum --retain-hours 168")
    retention >> optimize >> vacuum
