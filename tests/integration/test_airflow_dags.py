"""Airflow DAG validation (runs where Airflow is installed: the airflow image / CI)."""

from __future__ import annotations

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAG_DIR = Path(__file__).resolve().parents[2] / "orchestration" / "dags"
EXPECTED = {
    "reference_data_ingestion",
    "product_catalog_ingestion",
    "historical_order_ingestion",
    "currency_rate_ingestion",
    "silver_batch_processing",
    "gold_dimension_processing",
    "gold_fact_processing",
    "customer_360_refresh",
    "daily_sales_aggregation",
    "inventory_snapshot",
    "data_quality_validation",
    "financial_reconciliation",
    "historical_backfill",
    "delta_table_maintenance",
}


@pytest.fixture(scope="module")
def dagbag():  # type: ignore[no-untyped-def]
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAG_DIR), include_examples=False)


def test_no_import_errors(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert dagbag.import_errors == {}


def test_all_expected_dags_present(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert set(dagbag.dag_ids) == EXPECTED


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_dag_production_settings(dagbag, dag_id: str) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.get_dag(dag_id)
    assert dag.max_active_runs == 1, "concurrent runs would race on Delta tables"
    assert not dag.catchup
    assert "ecommerce-platform" in dag.tags
    assert dag.doc_md, "every DAG documents its purpose"
    for task in dag.tasks:
        assert task.retries >= 1, f"{dag_id}.{task.task_id} must retry"
        assert task.retry_exponential_backoff
        assert task.execution_timeout is not None
        assert task.on_failure_callback is not None
        if "pipelines." in task.bash_command or "data_quality" in task.bash_command:
            assert task.pool == "spark_jobs", f"{task.task_id} runs Spark and must use the spark_jobs pool"


def test_streaming_jobs_never_run_in_airflow(dagbag) -> None:  # type: ignore[no-untyped-def]
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            cmd = getattr(task, "bash_command", "")
            assert "stream_kafka_to_bronze" not in cmd or "--once" in cmd
            assert "--continuous" not in cmd


def test_backfill_is_parameterised(dagbag) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.get_dag("historical_backfill")
    assert {"start_date", "end_date", "replay_silver"} <= set(dag.params.keys())
    assert dag.schedule_interval is None
