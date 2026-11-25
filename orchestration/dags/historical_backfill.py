"""Parameterised historical backfill / replay.

Trigger with config, for example {"start_date": "2026-08-01", "end_date": "2026-08-31", "replay_silver": true}:
1. re-fetch REST sources (FX, campaigns) for the date range,
2. load landing files that are not loaded yet,
3. optionally rebuild Silver from Bronze (replay) and fully refresh the Gold facts,
4. rebuild the marts and validate quality."""

from __future__ import annotations

from airflow import DAG
from airflow.models.param import Param

from common.platform import DAG_DEFAULTS, default_args, platform_task

with DAG(
    dag_id="historical_backfill",
    schedule=None,
    default_args=default_args(retries=1, timeout_minutes=180),
    params={
        "start_date": Param("2026-08-01", type="string", format="date"),
        "end_date": Param("2026-08-31", type="string", format="date"),
        "replay_silver": Param(False, type="boolean"),
    },
    doc_md=__doc__,
    **DAG_DEFAULTS,
) as dag:
    rest = platform_task(
        "backfill_rest_sources",
        "python -m ingestion.batch.run --group fx --group marketing "
        "--start-date {{ params.start_date }} --end-date {{ params.end_date }}",
    )
    files = platform_task(
        "reload_pending_files", "python -m ingestion.batch.run --group history --group catalog --skip-extract"
    )
    silver = platform_task(
        "rebuild_silver",
        "{% if params.replay_silver %}python scripts/replay_bronze.py --all-streams --yes"
        "{% else %}python -m pipelines.silver.run{% endif %}",
    )
    gold = platform_task(
        "rebuild_gold", "python -m pipelines.gold.run {% if params.replay_silver %}--full-refresh{% endif %}"
    )
    dq = platform_task("validate", "python -m data_quality.run --fail-on critical")
    rest >> files >> silver >> gold >> dq
