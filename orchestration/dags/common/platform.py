"""Shared building blocks for the platform DAGs (Airflow 2.10).

* Tasks run the same CLI entrypoints used locally and on Databricks
  (``python -m pipelines.gold.run ...``). DAG files hold no business logic.
* Every task is idempotent: watermarks, Delta MERGE / txn ids and partition
  overwrites make retries and re-runs safe.
* Spark tasks share the ``spark_jobs`` pool (2 slots). Delta on S3 with the OSS
  log store allows one writer per table at a time (ADR 0010).
* Retries use exponential back-off. Failures and SLA misses are logged as
  structured JSON and pushed to Prometheus (Pushgateway) for alerting.
* Continuous streaming jobs never run here. They are long-running services
  (docker compose profile ``processing``) or Databricks continuous jobs.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta
from typing import Any

from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

log = logging.getLogger("ecom.airflow")

PLATFORM_DIR = os.environ.get("PLATFORM_DIR", "/opt/platform")
PLATFORM_ENV = os.environ.get("PLATFORM_ENV", "local")
SPARK_POOL = "spark_jobs"
START_DATE = datetime(2026, 1, 1)

# Data-aware scheduling: producers declare outlets, consumers schedule on them.
BRONZE_BATCH = Dataset("delta://lakehouse/bronze/batch")
SILVER = Dataset("delta://lakehouse/silver")
GOLD_DIMENSIONS = Dataset("delta://lakehouse/gold/dimensions")
GOLD_FACTS = Dataset("delta://lakehouse/gold/facts")
GOLD_MARTS = Dataset("delta://lakehouse/gold/marts")


def _push_metric(name: str, labels: dict[str, str], value: float = 1.0) -> None:
    """Best-effort Pushgateway update (monitoring must never fail a task)."""
    url = os.environ.get("PUSHGATEWAY_URL")
    if not url:
        return
    label_path = "/".join(f"{k}/{v}" for k, v in labels.items())
    body = f"# TYPE {name} gauge\n{name} {value}\n".encode()
    req = urllib.request.Request(f"{url}/metrics/job/airflow_callbacks/{label_path}", data=body, method="PUT")
    try:
        urllib.request.urlopen(req, timeout=3)
    except Exception as exc:  # pragma: no cover
        log.warning("pushgateway unavailable: %s", exc)


def on_failure(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    payload = {
        "event": "task_failed",
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "try_number": ti.try_number,
        "exception": str(context.get("exception"))[:1000],
        "log_url": ti.log_url,
    }
    log.error(json.dumps(payload))
    _push_metric("ecom_airflow_task_failed", {"dag": ti.dag_id, "task": ti.task_id})


def on_success(context: dict[str, Any]) -> None:
    ti = context["task_instance"]
    _push_metric("ecom_airflow_task_failed", {"dag": ti.dag_id, "task": ti.task_id}, 0.0)


def on_sla_miss(
    dag: Any, task_list: str, blocking_task_list: str, slas: list[Any], blocking_tis: list[Any]
) -> None:
    log.error(
        json.dumps(
            {"event": "sla_missed", "dag_id": dag.dag_id, "tasks": task_list, "blocking": blocking_task_list}
        )
    )
    _push_metric("ecom_airflow_sla_missed", {"dag": dag.dag_id})


def default_args(
    retries: int = 3, timeout_minutes: int = 60, sla_minutes: int | None = None
) -> dict[str, Any]:
    args: dict[str, Any] = {
        "owner": "data-platform",
        "depends_on_past": False,
        "retries": retries,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=20),
        "execution_timeout": timedelta(minutes=timeout_minutes),
        "on_failure_callback": on_failure,
        "on_success_callback": on_success,
    }
    if sla_minutes:
        args["sla"] = timedelta(minutes=sla_minutes)
    return args


def platform_task(
    task_id: str, command: str, spark: bool = True, outlets: list[Dataset] | None = None, **kwargs: Any
) -> BashOperator:
    """Run a platform CLI command with the run's correlation id."""
    env = {
        **os.environ,
        "PYTHONPATH": PLATFORM_DIR,
        "PLATFORM_ENV": PLATFORM_ENV,
        "AIRFLOW_RUN_ID": "{{ run_id }}",
        "AIRFLOW_LOGICAL_DATE": "{{ ds }}",
    }
    extra: dict[str, Any] = {"pool": SPARK_POOL} if spark else {}
    extra.update(kwargs)
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PLATFORM_DIR} && {command}",
        env=env,
        append_env=False,
        outlets=outlets or [],
        **extra,
    )


DAG_DEFAULTS: dict[str, Any] = {
    "start_date": START_DATE,
    "catchup": False,
    "max_active_runs": 1,
    "sla_miss_callback": on_sla_miss,
    "tags": ["ecommerce-platform"],
}
