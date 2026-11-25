"""Incremental Gold fact loads (watermark on Silver changes, MERGE on natural keys)."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, GOLD_DIMENSIONS, GOLD_FACTS, default_args, platform_task

FACTS = [
    "fact_orders",
    "fact_order_items",
    "fact_payments",
    "fact_shipments",
    "fact_refunds",
    "fact_returns",
    "fact_clickstream",
]

with DAG(
    dag_id="gold_fact_processing",
    schedule=[GOLD_DIMENSIONS],
    default_args=default_args(timeout_minutes=60, sla_minutes=60),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    previous = None
    for fact in FACTS:
        task = platform_task(
            f"load_{fact}",
            f"python -m pipelines.gold.run --skip-quality-gate --stage facts --fact {fact}"
            if previous is not None
            else f"python -m pipelines.gold.run --stage facts --fact {fact}",
            outlets=[GOLD_FACTS] if fact == FACTS[-1] else None,
        )
        if previous is not None:
            previous >> task
        previous = task
