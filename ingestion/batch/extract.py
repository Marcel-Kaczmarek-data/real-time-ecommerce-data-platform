"""Extract raw data from PostgreSQL and REST APIs into the landing bucket."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from data_generator.sinks import Landing, parquet_bytes
from ingestion.batch.registry import Registry
from pipelines.common.logs import get_logger

log = get_logger(__name__)

# Rows committed with an updated_at inside this window may still be in flight
# (long transactions); they are picked up by the next run instead of being lost.
WATERMARK_SAFETY_LAG = timedelta(seconds=30)


def _normalise(value: Any) -> Any:
    return float(value) if isinstance(value, Decimal) else value


def extract_postgres(
    conn: Any, registry: Registry, landing: Landing, name: str, source: dict[str, Any], run_ts: datetime
) -> dict[str, Any]:
    """Full or watermark-incremental extract of an operational table to Parquet in the landing zone."""
    table = source["table"]
    params: list[Any] = []
    sql = f"SELECT * FROM {table}"  # table names come from the trusted source catalogue
    upper = None
    if source["mode"] == "incremental":
        column = source["watermark_column"]
        lower = registry.get_watermark(name)
        with conn.cursor() as cur:
            cur.execute("SELECT now()")
            upper = cur.fetchone()[0] - WATERMARK_SAFETY_LAG
        sql += f" WHERE {column} > %s AND {column} <= %s ORDER BY {column}"
        params = [lower, upper]
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [d.name for d in cur.description]
        rows = [{c: _normalise(v) for c, v in zip(columns, r, strict=True)} for r in cur.fetchall()]
    conn.commit()
    if not rows:
        log.info("no new rows", extra={"source": name})
        if upper is not None:
            registry.stage_watermark(name, source["watermark_column"], upper)
        return {"source": name, "rows": 0, "key": None}
    key = (
        f"{source['landing']}/extract_date={run_ts:%Y-%m-%d}/"
        f"{table.split('.')[-1]}_{run_ts:%Y%m%dT%H%M%S}_{source['mode']}.parquet"
    )
    landing.put(key, parquet_bytes(rows))
    if source["mode"] == "incremental":
        new_wm = max(r[source["watermark_column"]] for r in rows)
        registry.stage_watermark(name, source["watermark_column"], new_wm)
    log.info("postgres extract written", extra={"source": name, "rows": len(rows), "key": key})
    return {"source": name, "rows": len(rows), "key": key}


class TransientAPIError(RuntimeError):
    pass


def _get_json(base_url: str, endpoint: str, api_key: str, day: date, timeout: int, retries: int) -> Any:
    @retry(
        retry=retry_if_exception_type((TransientAPIError, requests.ConnectionError, requests.Timeout)),
        stop=stop_after_attempt(retries),
        wait=wait_exponential(multiplier=0.5, max=15),
        reraise=True,
    )
    def call() -> Any:
        resp = requests.get(
            f"{base_url}{endpoint}",
            params={"date": day.isoformat()},
            headers={"X-API-Key": api_key},
            timeout=timeout,
        )
        if resp.status_code >= 500 or resp.status_code == 429:
            raise TransientAPIError(f"{resp.status_code} from {endpoint}")
        resp.raise_for_status()
        return resp.json()

    return call()


def extract_rest(
    registry: Registry,
    landing: Landing,
    name: str,
    source: dict[str, Any],
    rest_cfg: dict[str, Any],
    start: date | None,
    end: date,
) -> dict[str, Any]:
    """Fetch one JSON document per day since the watermark (or from ``start`` for backfills)."""
    wm = registry.get_watermark(name)
    first = start or (wm.date() + timedelta(days=1) if wm.year > 1970 else end - timedelta(days=30))
    keys = []
    day = first
    while day <= end:
        doc = _get_json(
            rest_cfg["base_url"],
            source["endpoint"],
            rest_cfg["api_key"],
            day,
            int(rest_cfg.get("timeout_seconds", 15)),
            int(rest_cfg.get("max_retries", 5)),
        )
        key = f"{source['landing']}/date={day.isoformat()}/{name}_{day:%Y%m%d}.json"
        landing.put(key, json.dumps(doc).encode())
        keys.append(key)
        day += timedelta(days=1)
    if keys:
        registry.stage_watermark(
            name, "response_date", datetime.combine(end, datetime.min.time(), tzinfo=UTC)
        )
    log.info(
        "rest extract written",
        extra={"source": name, "documents": len(keys), "from": str(first), "to": str(end)},
    )
    return {"source": name, "documents": len(keys)}
