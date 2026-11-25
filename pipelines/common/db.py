"""PostgreSQL connectivity for the operational and control-plane schemas."""

from __future__ import annotations

from typing import Any

from tenacity import retry, stop_after_attempt, wait_exponential

from pipelines.common.config import Config, load_config


def pg_params(cfg: Config | None = None) -> dict[str, Any]:
    cfg = cfg or load_config()
    pg = cfg["postgres"]
    return {
        "host": pg["host"],
        "port": int(pg["port"]),
        "dbname": pg["database"],
        "user": pg["user"],
        "password": pg["password"],
    }


def jdbc_url(cfg: Config | None = None) -> str:
    p = pg_params(cfg)
    return f"jdbc:postgresql://{p['host']}:{p['port']}/{p['dbname']}"


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=20), reraise=True)
def connect(cfg: Config | None = None) -> Any:
    import psycopg2

    return psycopg2.connect(**pg_params(cfg), connect_timeout=10, application_name="ecom-platform")
