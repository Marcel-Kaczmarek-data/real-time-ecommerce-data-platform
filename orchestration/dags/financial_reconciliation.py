"""Daily orders vs payments vs refunds reconciliation (fails outside tolerance)."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, default_args, platform_task

with DAG(
    dag_id="financial_reconciliation",
    schedule="0 3 * * *",
    default_args=default_args(retries=1, timeout_minutes=30, sla_minutes=60),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    facts = platform_task(
        "refresh_payment_facts",
        "python -m pipelines.gold.run --stage facts --fact fact_orders --fact fact_payments --fact fact_refunds",
    )
    recon = platform_task("reconcile", "python -m pipelines.gold.reconciliation --tolerance-usd 1.0")
    facts >> recon
