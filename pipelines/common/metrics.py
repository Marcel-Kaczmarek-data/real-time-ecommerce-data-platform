"""Prometheus instrumentation helpers.

* Long-running services (producer, Bronze stream) expose ``/metrics`` over HTTP.
* Batch jobs (Silver, Gold, DQ, ingestion) push a final set of metrics to the
  Pushgateway, grouped by job name, so Grafana sees duration, row counts and
  success/failure for every run.
"""

from __future__ import annotations

import contextlib
import os
import time
from collections.abc import Iterator
from typing import Any

from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway, start_http_server

from pipelines.common.config import load_config
from pipelines.common.logs import get_logger

log = get_logger(__name__)


def serve_metrics(port: int) -> None:
    start_http_server(port)
    log.info("metrics endpoint started", extra={"port": port})


class JobMetrics:
    """Collects per-run metrics for a batch job and pushes them at the end."""

    def __init__(self, job: str) -> None:
        self.job = job
        self.registry = CollectorRegistry()
        self.duration = Gauge(
            "ecom_job_duration_seconds",
            "Wall-clock duration of the last run",
            ["job"],
            registry=self.registry,
        )
        self.success = Gauge(
            "ecom_job_last_success", "1 if the last run succeeded, else 0", ["job"], registry=self.registry
        )
        self.last_run = Gauge(
            "ecom_job_last_run_timestamp_seconds",
            "Unix time of the last run",
            ["job"],
            registry=self.registry,
        )
        self.last_success_ts = Gauge(
            "ecom_job_last_success_timestamp_seconds",
            "Unix time of the last success",
            ["job"],
            registry=self.registry,
        )
        self.rows = Gauge(
            "ecom_job_rows",
            "Rows processed in the last run",
            ["job", "table", "kind"],
            registry=self.registry,
        )
        self.runs = Counter(
            "ecom_job_runs", "Runs by outcome (per push)", ["job", "outcome"], registry=self.registry
        )
        self.freshness = Gauge(
            "ecom_table_freshness_seconds",
            "Age of the newest record in a table",
            ["table"],
            registry=self.registry,
        )
        self.gauges: dict[str, Gauge] = {}

    def set_rows(self, table: str, kind: str, value: int | float) -> None:
        self.rows.labels(self.job, table, kind).set(value)

    def gauge(self, name: str, doc: str, labels: dict[str, str], value: float) -> None:
        if name not in self.gauges:
            self.gauges[name] = Gauge(name, doc, list(labels), registry=self.registry)
        self.gauges[name].labels(**labels).set(value)

    def push(self) -> None:
        url = os.environ.get("PUSHGATEWAY_URL") or load_config().get_path("monitoring.pushgateway_url")
        if not url or not load_config().get_path("monitoring.enabled", True):
            return
        try:
            push_to_gateway(str(url).replace("http://", ""), job=self.job, registry=self.registry, timeout=5)
        except Exception as exc:  # monitoring must never fail a pipeline
            log.warning("pushgateway unavailable", extra={"error": str(exc)})


@contextlib.contextmanager
def track_job(job: str) -> Iterator[JobMetrics]:
    metrics = JobMetrics(job)
    start = time.time()
    metrics.last_run.labels(job).set(start)
    try:
        yield metrics
        metrics.success.labels(job).set(1)
        metrics.last_success_ts.labels(job).set(time.time())
        metrics.runs.labels(job, "success").inc()
    except BaseException:
        metrics.success.labels(job).set(0)
        metrics.runs.labels(job, "failure").inc()
        raise
    finally:
        metrics.duration.labels(job).set(time.time() - start)
        metrics.push()


def safe_labels(**labels: Any) -> dict[str, str]:
    return {k: str(v) for k, v in labels.items()}
