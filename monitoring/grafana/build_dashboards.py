"""Generate the Grafana dashboards (dashboards-as-code).

    python monitoring/grafana/build_dashboards.py

Writes one JSON file per dashboard into monitoring/grafana/dashboards/. The
JSON is committed; this script is the single place to change panels.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parent / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}


class Board:
    def __init__(self, uid: str, title: str, description: str) -> None:
        self.uid, self.title, self.description = uid, title, description
        self.panels: list[dict[str, Any]] = []
        self.x = 0
        self.y = 0
        self.row_h = 0
        self.next_id = 1

    def _place(self, w: int, h: int) -> dict[str, int]:
        if self.x + w > 24:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        return pos

    def row(self, title: str) -> None:
        if self.x:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        self.panels.append(
            {
                "type": "row",
                "title": title,
                "id": self.next_id,
                "collapsed": False,
                "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1},
                "panels": [],
            }
        )
        self.next_id += 1
        self.y += 1

    def _panel(
        self,
        kind: str,
        title: str,
        targets: list[tuple[str, str]],
        w: int,
        h: int,
        unit: str,
        extra: dict[str, Any] | None = None,
        description: str = "",
    ) -> None:
        panel = {
            "id": self.next_id,
            "type": kind,
            "title": title,
            "description": description,
            "datasource": DS,
            "gridPos": self._place(w, h),
            "targets": [
                {"refId": chr(65 + i), "expr": expr, "legendFormat": legend, "datasource": DS}
                for i, (expr, legend) in enumerate(targets)
            ],
            "fieldConfig": {
                "defaults": {"unit": unit, "color": {"mode": "palette-classic"}},
                "overrides": [],
            },
            "options": {
                "legend": {"displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "multi"},
            },
        }
        if extra:
            for key, value in extra.items():
                if key == "defaults":
                    panel["fieldConfig"]["defaults"].update(value)
                else:
                    panel[key] = value
        self.panels.append(panel)
        self.next_id += 1

    def ts(
        self,
        title: str,
        targets: list[tuple[str, str]],
        w: int = 12,
        h: int = 8,
        unit: str = "short",
        description: str = "",
    ) -> None:
        self._panel(
            "timeseries",
            title,
            targets,
            w,
            h,
            unit,
            {"defaults": {"custom": {"lineWidth": 2, "fillOpacity": 8, "showPoints": "never"}}},
            description,
        )

    def stat(
        self,
        title: str,
        expr: str,
        w: int = 4,
        h: int = 4,
        unit: str = "short",
        thresholds: list[tuple[float | None, str]] | None = None,
        description: str = "",
    ) -> None:
        steps = [{"value": v, "color": c} for v, c in (thresholds or [(None, "blue")])]
        self._panel(
            "stat",
            title,
            [(expr, title)],
            w,
            h,
            unit,
            {
                "defaults": {
                    "thresholds": {"mode": "absolute", "steps": steps},
                    "color": {"mode": "thresholds"},
                },
                "options": {
                    "reduceOptions": {"calcs": ["lastNotNull"]},
                    "colorMode": "value",
                    "graphMode": "area",
                    "textMode": "value",
                },
            },
            description,
        )

    def table(self, title: str, expr: str, w: int = 24, h: int = 8) -> None:
        self._panel(
            "table",
            title,
            [(expr, "")],
            w,
            h,
            "short",
            {"targets": [{"refId": "A", "expr": expr, "format": "table", "instant": True, "datasource": DS}]},
        )

    def json(self) -> dict[str, Any]:
        return {
            "uid": self.uid,
            "title": self.title,
            "description": self.description,
            "tags": ["ecommerce-platform"],
            "timezone": "utc",
            "schemaVersion": 39,
            "version": 1,
            "refresh": "30s",
            "time": {"from": "now-6h", "to": "now"},
            "editable": False,
            "panels": self.panels,
            "templating": {"list": []},
            "annotations": {"list": []},
        }


GOOD_BAD = [(None, "green"), (1, "red")]


def kafka() -> Board:
    b = Board("kafka-health", "Kafka health", "Broker, topic throughput, partitions and consumer lag.")
    b.stat("Brokers online", "kafka_brokers", thresholds=[(None, "red"), (1, "green")])
    b.stat("Topics", 'count(count by (topic) (kafka_topic_partitions{topic=~"ecommerce.*"}))')
    b.stat("Partitions", 'sum(kafka_topic_partitions{topic=~"ecommerce.*"})')
    b.stat(
        "Messages / s (in)",
        'sum(rate(kafka_topic_partition_current_offset{topic=~"ecommerce.*"}[1m]))',
        unit="ops",
    )
    b.stat(
        "Dead-letter / 15 min",
        'sum(increase(kafka_topic_partition_current_offset{topic="ecommerce.dead-letter"}[15m]))',
        thresholds=[(None, "green"), (50, "orange")],
    )
    b.stat(
        "Spark stream lag (max)",
        'max(ecom_stream_offsets_behind_latest{stat="maxOffsetsBehindLatest"})',
        thresholds=[(None, "green"), (50000, "orange")],
    )
    b.row("Throughput")
    b.ts(
        "Messages per second by topic",
        [
            (
                'sum by (topic) (rate(kafka_topic_partition_current_offset{topic=~"ecommerce.*"}[1m]))',
                "{{topic}}",
            )
        ],
        w=24,
        unit="ops",
    )
    b.row("Producer")
    b.ts(
        "Producer acknowledged events / s",
        [("sum by (topic) (rate(ecom_producer_events_total[1m]))", "{{topic}}")],
        unit="ops",
    )
    b.ts(
        "Producer ack latency (p50 / p95 / p99)",
        [
            (
                "histogram_quantile(0.5, sum by (le) (rate(ecom_producer_ack_latency_seconds_bucket[5m])))",
                "p50",
            ),
            (
                "histogram_quantile(0.95, sum by (le) (rate(ecom_producer_ack_latency_seconds_bucket[5m])))",
                "p95",
            ),
            (
                "histogram_quantile(0.99, sum by (le) (rate(ecom_producer_ack_latency_seconds_bucket[5m])))",
                "p99",
            ),
        ],
        unit="s",
    )
    b.ts(
        "Dead-letter events by stage",
        [("sum by (stage) (increase(ecom_dead_letter_events_total[5m]))", "{{stage}}")],
    )
    b.ts(
        "Consumer-group lag (kafka-exporter)",
        [("sum by (consumergroup, topic) (kafka_consumergroup_lag)", "{{consumergroup}} {{topic}}")],
    )
    return b


def spark_streaming() -> Board:
    b = Board(
        "spark-streaming",
        "Spark streaming health",
        "Structured Streaming queries: throughput, batch duration, lag, latency and watermark drops.",
    )
    b.stat("Active queries", "sum(ecom_stream_query_active)", thresholds=[(None, "red"), (1, "green")])
    b.stat(
        "Last progress (s ago)",
        "time() - max(ecom_stream_last_progress_timestamp_seconds)",
        unit="s",
        thresholds=[(None, "green"), (120, "orange"), (300, "red")],
    )
    b.stat(
        "Bronze ingest latency p95",
        'ecom_bronze_ingest_latency_seconds{quantile="p95"}',
        unit="s",
        thresholds=[(None, "green"), (60, "orange"), (120, "red")],
    )
    b.stat("Offsets behind (max)", 'max(ecom_stream_offsets_behind_latest{stat="maxOffsetsBehindLatest"})')
    b.stat(
        "Corrupt records (1h)",
        "sum(increase(ecom_bronze_corrupt_rows_total[1h]))",
        thresholds=[(None, "green"), (1, "orange")],
    )
    b.stat("Late rows dropped (1h)", "sum(increase(ecom_stream_rows_dropped_by_watermark_total[1h]))")
    b.row("Throughput and duration")
    b.ts(
        "Input vs processed rows / s",
        [
            ("ecom_stream_input_rows_per_second", "input {{query}}"),
            ("ecom_stream_processed_rows_per_second", "processed {{query}}"),
        ],
        unit="ops",
    )
    b.ts("Micro-batch duration", [("ecom_stream_batch_duration_seconds", "{{query}}")], unit="s")
    b.ts(
        "Bronze rows written / s by topic",
        [("sum by (topic) (rate(ecom_bronze_rows_written_total[1m]))", "{{topic}}")],
        unit="ops",
    )
    b.ts(
        "End-to-end latency",
        [
            ('ecom_bronze_ingest_latency_seconds{quantile="p50"}', "Kafka→Bronze p50"),
            ('ecom_bronze_ingest_latency_seconds{quantile="p95"}', "Kafka→Bronze p95"),
            ('ecom_bronze_ingest_latency_seconds{quantile="p99"}', "Kafka→Bronze p99"),
        ],
        unit="s",
        description="Kafka append time to Bronze commit.",
    )
    b.ts("Offsets behind latest (lag)", [("ecom_stream_offsets_behind_latest", "{{query}} {{stat}}")])
    b.ts("State rows (real-time aggregates)", [("ecom_stream_state_rows", "{{query}}")])
    return b


def airflow() -> Board:
    b = Board("airflow-health", "Airflow workflow health", "DAG and task outcomes, durations and SLA misses.")
    b.stat("Task failures (24h)", "sum(increase(airflow_task_failures_total[24h]))", thresholds=GOOD_BAD)
    b.stat("Task successes (24h)", "sum(increase(airflow_task_successes_total[24h]))")
    b.stat("SLA misses", "sum(ecom_airflow_sla_missed)", thresholds=GOOD_BAD)
    b.stat("Failed tasks now", "sum(ecom_airflow_task_failed)", thresholds=GOOD_BAD)
    b.stat("Running tasks", "airflow_executor_running_tasks")
    b.stat("Open Spark pool slots", 'airflow_pool_open_slots{pool="spark_jobs"}')
    b.row("Runs")
    b.ts("DAG run duration", [("airflow_dagrun_duration_seconds", "{{dag_id}} {{state}}")], unit="s")
    b.ts(
        "Task finishes by state", [("sum by (state) (increase(airflow_task_finish_total[15m]))", "{{state}}")]
    )
    b.ts("Scheduling delay", [("airflow_dagrun_schedule_delay_seconds", "{{dag_id}}")], unit="s")
    b.ts("Pipeline success (last run)", [("ecom_job_last_success", "{{job}}")])
    return b


def lakehouse() -> Board:
    b = Board(
        "lakehouse-ingestion", "Lakehouse ingestion", "Bronze/Silver/Gold job runs, rows and freshness."
    )
    b.stat("Pipelines succeeding", "sum(ecom_job_last_success)", thresholds=[(None, "orange")])
    b.stat("Pipelines failing", "count(ecom_job_last_success == 0) or vector(0)", thresholds=GOOD_BAD)
    b.stat(
        "Oldest data (h)", "max(ecom_dq_data_age_hours)", unit="h", thresholds=[(None, "green"), (6, "red")]
    )
    b.stat("Reconciliation diff (USD)", 'ecom_reconciliation_difference_usd{scope="all"}', unit="currencyUSD")
    b.stat("Files failed", 'sum(ecom_ingestion_files{status="failed"})', thresholds=GOOD_BAD)
    b.stat("Files duplicate", 'sum(ecom_ingestion_files{status="duplicate"})')
    b.row("Jobs")
    b.ts("Job duration (last run)", [("ecom_job_duration_seconds", "{{job}}")], unit="s")
    b.ts(
        "Time since last success", [("time() - ecom_job_last_success_timestamp_seconds", "{{job}}")], unit="s"
    )
    b.table("Rows per table (last run)", "ecom_job_rows")
    return b


def data_quality() -> Board:
    b = Board("data-quality", "Data quality", "Rule-suite results, failed checks by severity, freshness.")
    b.stat(
        "Quality score (silver)",
        'ecom_dq_quality_score{suite="silver"}',
        unit="percent",
        thresholds=[(None, "red"), (95, "orange"), (99, "green")],
    )
    b.stat(
        "Quality score (gold)",
        'ecom_dq_quality_score{suite="gold"}',
        unit="percent",
        thresholds=[(None, "red"), (95, "orange"), (99, "green")],
    )
    b.stat("Critical failures", 'sum(ecom_dq_failed_checks{severity="critical"})', thresholds=GOOD_BAD)
    b.stat(
        "Warning failures",
        'sum(ecom_dq_failed_checks{severity="warning"})',
        thresholds=[(None, "green"), (1, "orange")],
    )
    b.stat("Bronze quarantined (1h)", "sum(increase(ecom_bronze_corrupt_rows_total[1h]))")
    b.stat("Rejected rows (rejects)", 'sum(ecom_dq_check_failed_records{check="silver_rejects_volume"})')
    b.row("Trends")
    b.ts("Quality score by suite", [("ecom_dq_quality_score", "{{suite}}")], unit="percent")
    b.ts("Data age by table", [("ecom_dq_data_age_hours", "{{table}}")], unit="h")
    b.table("Failing records per check", "ecom_dq_check_failed_records > 0")
    return b


def infrastructure() -> Board:
    b = Board(
        "infrastructure", "Infrastructure resources", "Container CPU, memory, network and storage (cAdvisor)."
    )
    b.stat("Containers", 'count(container_last_seen{name=~"ecom-.*"})')
    b.stat("Object storage used", 'sum(container_fs_usage_bytes{name="ecom-object-storage"})', unit="bytes")
    b.stat("Total memory (ecom-*)", 'sum(container_memory_working_set_bytes{name=~"ecom-.*"})', unit="bytes")
    b.stat("Total CPU (cores)", 'sum(rate(container_cpu_usage_seconds_total{name=~"ecom-.*"}[2m]))')
    b.row("Per container")
    b.ts(
        "CPU (cores)",
        [('sum by (name) (rate(container_cpu_usage_seconds_total{name=~"ecom-.*"}[2m]))', "{{name}}")],
    )
    b.ts(
        "Memory working set",
        [('sum by (name) (container_memory_working_set_bytes{name=~"ecom-.*"})', "{{name}}")],
        unit="bytes",
    )
    b.ts(
        "Network receive",
        [('sum by (name) (rate(container_network_receive_bytes_total{name=~"ecom-.*"}[2m]))', "{{name}}")],
        unit="Bps",
    )
    b.ts(
        "Filesystem usage",
        [('sum by (name) (container_fs_usage_bytes{name=~"ecom-.*"})', "{{name}}")],
        unit="bytes",
    )
    return b


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for board, name in (
        (kafka(), "kafka-health"),
        (spark_streaming(), "spark-streaming-health"),
        (airflow(), "airflow-health"),
        (lakehouse(), "lakehouse-ingestion"),
        (data_quality(), "data-quality"),
        (infrastructure(), "infrastructure"),
    ):
        (OUT / f"{name}.json").write_text(
            json.dumps(board.json(), indent=2) + "\n", encoding="utf-8", newline="\n"
        )
        print(f"wrote {name}.json ({len(board.panels)} panels)")


if __name__ == "__main__":
    main()
