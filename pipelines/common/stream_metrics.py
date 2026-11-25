"""Prometheus metrics for Spark Structured Streaming queries (driver side).

A ``StreamingQueryListener`` turns every progress report into gauges:
throughput, batch duration, offsets behind latest (consumer lag; Spark keeps
offsets in its checkpoint, not in a Kafka consumer group, so kafka-exporter
cannot see it), watermark drops and the time of the last completed batch.
"""

from __future__ import annotations

import math
from typing import Any

from prometheus_client import Counter, Gauge
from pyspark.sql.streaming import StreamingQueryListener

from pipelines.common.logs import get_logger

log = get_logger(__name__)

INPUT_RATE = Gauge("ecom_stream_input_rows_per_second", "Input rows/s of the last micro-batch", ["query"])
PROCESS_RATE = Gauge(
    "ecom_stream_processed_rows_per_second", "Processed rows/s of the last micro-batch", ["query"]
)
BATCH_ROWS = Gauge("ecom_stream_batch_input_rows", "Rows in the last micro-batch", ["query"])
BATCH_DURATION = Gauge(
    "ecom_stream_batch_duration_seconds", "Trigger execution time of the last micro-batch", ["query"]
)
LAST_PROGRESS = Gauge(
    "ecom_stream_last_progress_timestamp_seconds", "Unix time of the last progress event", ["query"]
)
OFFSETS_BEHIND = Gauge(
    "ecom_stream_offsets_behind_latest", "Kafka offsets behind latest (consumer lag)", ["query", "stat"]
)
WATERMARK_DROPS = Counter(
    "ecom_stream_rows_dropped_by_watermark_total", "Late rows dropped by watermark", ["query"]
)
STATE_ROWS = Gauge("ecom_stream_state_rows", "Rows held in streaming state", ["query"])
ROWS_TOTAL = Counter("ecom_stream_rows_processed_total", "Rows processed", ["query"])
QUERY_ACTIVE = Gauge("ecom_stream_query_active", "1 while the query is running", ["query"])

# Written from foreachBatch handlers (per topic / quality / latency).
TOPIC_ROWS = Counter("ecom_bronze_rows_written_total", "Rows appended to Bronze", ["topic"])
CORRUPT_ROWS = Counter("ecom_bronze_corrupt_rows_total", "Undecodable records quarantined", ["topic"])
PIPELINE_LATENCY = Gauge(
    "ecom_bronze_ingest_latency_seconds",
    "Kafka append time -> Bronze commit latency of the last batch",
    ["quantile"],
)
EVENT_LATENCY = Gauge(
    "ecom_bronze_event_latency_seconds", "Event time -> Bronze commit latency of the last batch", ["quantile"]
)
LATEST_EVENT_TS = Gauge("ecom_bronze_latest_event_timestamp_seconds", "Newest event time written", ["topic"])


class PrometheusStreamingListener(StreamingQueryListener):
    def onQueryStarted(self, event: Any) -> None:
        QUERY_ACTIVE.labels(event.name or str(event.id)).set(1)
        log.info("streaming query started", extra={"query": event.name, "id": str(event.id)})

    def onQueryProgress(self, event: Any) -> None:
        p = event.progress
        name = p.name or str(p.id)
        INPUT_RATE.labels(name).set(_num(p.inputRowsPerSecond))
        PROCESS_RATE.labels(name).set(_num(p.processedRowsPerSecond))
        BATCH_ROWS.labels(name).set(_num(p.numInputRows))
        ROWS_TOTAL.labels(name).inc(_num(p.numInputRows))
        BATCH_DURATION.labels(name).set(_num((p.durationMs or {}).get("triggerExecution", 0)) / 1000.0)
        import time

        LAST_PROGRESS.labels(name).set(time.time())
        for source in p.sources or []:
            metrics = source.metrics or {}
            for stat in ("avgOffsetsBehindLatest", "maxOffsetsBehindLatest", "minOffsetsBehindLatest"):
                if stat in metrics:
                    OFFSETS_BEHIND.labels(name, stat).set(_num(metrics[stat]))
        for op in p.stateOperators or []:
            WATERMARK_DROPS.labels(name).inc(_num(getattr(op, "numRowsDroppedByWatermark", 0)))
            STATE_ROWS.labels(name).set(_num(op.numRowsTotal))

    def onQueryIdle(self, event: Any) -> None:
        return None

    def onQueryTerminated(self, event: Any) -> None:
        QUERY_ACTIVE.labels(str(event.id)).set(0)
        if event.exception:
            log.error(
                "streaming query terminated with error", extra={"id": str(event.id), "error": event.exception}
            )


def _num(value: Any) -> float:
    try:
        number = float(value) if value is not None else 0.0
        return 0.0 if math.isnan(number) else number
    except (TypeError, ValueError):
        return 0.0
