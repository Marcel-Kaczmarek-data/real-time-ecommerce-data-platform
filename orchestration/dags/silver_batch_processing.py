"""Incremental Bronze -> Silver processing of the event streams.

Each job is a Delta stream run with availableNow: it drains what arrived since its checkpoint and
stops (a bounded task). The same jobs can run continuously as a service."""

from __future__ import annotations

from airflow import DAG

from common.platform import DAG_DEFAULTS, SILVER, default_args, platform_task

with DAG(
    dag_id="silver_batch_processing",
    schedule="*/15 * * * *",
    default_args=default_args(timeout_minutes=40, sla_minutes=20),
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    orders = platform_task(
        "orders_and_items", "python -m pipelines.silver.run --job orders --job order_items"
    )
    payments = platform_task(
        "payments_refunds_returns",
        "python -m pipelines.silver.run --job payments --job refunds --job returns",
    )
    fulfilment = platform_task(
        "shipments_and_inventory",
        "python -m pipelines.silver.run --job shipments --job inventory_movements --job inventory",
    )
    behaviour = platform_task(
        "clickstream_and_sessions", "python -m pipelines.silver.run --job clickstream", outlets=[SILVER]
    )
    orders >> payments >> fulfilment >> behaviour
