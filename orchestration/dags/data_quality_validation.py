"""Full data-quality run (Bronze, Silver, Gold) -> audit.dq_results, Prometheus metrics and a markdown
report. Fails on critical findings (alerting); warnings are reported only."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, GOLD_MARTS, default_args, platform_task

with DAG(
    dag_id="data_quality_validation",
    schedule=[GOLD_MARTS],
    default_args=default_args(retries=1, timeout_minutes=30),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    platform_task("run_quality_suites", "python -m data_quality.run --fail-on critical")
