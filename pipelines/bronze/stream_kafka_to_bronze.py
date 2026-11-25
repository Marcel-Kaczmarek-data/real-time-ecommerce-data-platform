"""Kafka -> Bronze streaming ingestion (+ real-time operational aggregates).

    python -m pipelines.bronze.stream_kafka_to_bronze            # continuous (service)
    python -m pipelines.bronze.stream_kafka_to_bronze --once     # drain available data, then stop

Bronze query
    One query subscribes to every event topic. Each micro-batch is decoded
    once (Confluent Avro -> envelope + JSON payload). Rows are then appended
    to one Delta table per topic using ``txnAppId=bronze-<entity>`` and
    ``txnVersion=batchId``. After a crash, Spark replays the uncommitted batch
    from the checkpointed offsets, and Delta skips the per-table writes that
    had already committed. The result is effectively-once ingestion per offset.

Real-time queries
    Read the Bronze order and payment tables as Delta streams. They aggregate
    per minute with a 10-minute event-time watermark and MERGE the updated
    windows into ``gold.rt_sales_minute`` and ``gold.rt_payments_minute``.
    Events later than the watermark are dropped from these operational views
    only; Silver/Gold still include them (see architecture/data-flow.md).
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from data_generator.events import DEAD_LETTER_TOPIC
from pipelines.bronze.avro_decoder import TOPIC_ENTITY
from pipelines.bronze.schemas import BRONZE_EVENT_SCHEMA, CORRUPT_EVENT_SCHEMA, bronze_event_table
from pipelines.common import stream_metrics as sm
from pipelines.common.config import load_config
from pipelines.common.delta_utils import append_idempotent, ensure_table, merge_upsert
from pipelines.common.logs import get_logger, log_context
from pipelines.common.spark import get_spark
from pipelines.common.tables import checkpoint, read_stream_table, read_table, table
from pipelines.gold.realtime import RT_PAYMENTS_SCHEMA, RT_SALES_SCHEMA, payments_minute_agg, sales_minute_agg

log = get_logger("bronze-stream")
MAP_SCHEMA = (
    "key BINARY, value BINARY, topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP, "
    "event_id STRING, event_type STRING, event_version INT, event_timestamp_ms BIGINT, source_system STRING, "
    "correlation_id STRING, raw_payload STRING, schema_id INT, decode_error STRING"
)


def ensure_bronze_tables(spark: SparkSession) -> None:
    for entity in TOPIC_ENTITY.values():
        ensure_table(
            spark,
            table(bronze_event_table(entity)),
            BRONZE_EVENT_SCHEMA,
            partition_by=["processing_date"],
            comment=f"Raw {entity} events from Kafka (append-only, duplicates preserved)",
        )
    ensure_table(
        spark,
        table("quarantine.bronze_corrupt_events"),
        CORRUPT_EVENT_SCHEMA,
        partition_by=["processing_date"],
        comment="Kafka records that could not be decoded",
    )
    ensure_table(spark, table("gold.rt_sales_minute"), RT_SALES_SCHEMA)
    ensure_table(spark, table("gold.rt_payments_minute"), RT_PAYMENTS_SCHEMA)


def decode(batch_df: DataFrame, batch_id: int) -> DataFrame:
    from pipelines.bronze.avro_decoder import decode_batches

    decoded = batch_df.select("key", "value", "topic", "partition", "offset", "timestamp").mapInPandas(
        decode_batches, schema=MAP_SCHEMA
    )
    return (
        decoded.withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("processing_date", F.to_date("ingestion_timestamp"))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
    )


def write_batch(batch_df: DataFrame, batch_id: int, cfg: Any) -> None:
    decoded = decode(batch_df, batch_id).persist()
    try:
        good = decoded.filter(F.col("decode_error").isNull()).select(
            "event_id",
            "event_type",
            "event_version",
            F.timestamp_millis("event_timestamp_ms").alias("event_timestamp"),
            "source_system",
            "correlation_id",
            "raw_payload",
            F.col("value").alias("raw_value"),
            "schema_id",
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.lit(None).cast("string").alias("source_file"),
            "ingestion_timestamp",
            "processing_date",
            "batch_id",
        )
        counts = {
            r["kafka_topic"]: r["n"]
            for r in good.groupBy("kafka_topic").agg(F.count("*").alias("n")).collect()
        }
        for topic, n in counts.items():
            entity = TOPIC_ENTITY[topic]
            append_idempotent(
                good.filter(F.col("kafka_topic") == topic),
                table(bronze_event_table(entity)),
                app_id=f"bronze-stream-{entity}",
                version=batch_id,
            )
            sm.TOPIC_ROWS.labels(topic).inc(n)

        corrupt = (
            decoded.filter(F.col("decode_error").isNotNull())
            .select(
                F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_timestamp"),
                F.col("key").cast("string").alias("kafka_key"),
                F.col("value").alias("raw_value"),
                "decode_error",
                "ingestion_timestamp",
                "processing_date",
                "batch_id",
            )
            .persist()
        )
        n_corrupt = corrupt.count()
        if n_corrupt:
            append_idempotent(
                corrupt, table("quarantine.bronze_corrupt_events"), "bronze-stream-corrupt", batch_id
            )
            dlq = corrupt.select(
                F.col("kafka_key").cast("binary").alias("key"),
                F.to_json(
                    F.struct(
                        F.current_timestamp().cast("string").alias("failed_at"),
                        F.lit("bronze_decode").alias("stage"),
                        F.col("decode_error").alias("error"),
                        F.col("kafka_topic").alias("original_topic"),
                        F.col("kafka_partition"),
                        F.col("kafka_offset"),
                        F.base64("raw_value").alias("raw_value_base64"),
                    )
                ).alias("value"),
            )
            (
                dlq.write.format("kafka")
                .option("kafka.bootstrap.servers", cfg.get_path("kafka.bootstrap_servers"))
                .option("topic", DEAD_LETTER_TOPIC)
                .save()
            )
            for r in corrupt.groupBy("kafka_topic").count().collect():
                sm.CORRUPT_ROWS.labels(r["kafka_topic"]).inc(r["count"])
        corrupt.unpersist()

        if counts:
            lat = (
                good.select(
                    (
                        F.col("ingestion_timestamp").cast("double") - F.col("kafka_timestamp").cast("double")
                    ).alias("ingest"),
                    (
                        F.col("ingestion_timestamp").cast("double") - F.col("event_timestamp").cast("double")
                    ).alias("event"),
                )
                .agg(
                    F.percentile_approx("ingest", [0.5, 0.95, 0.99]).alias("ingest"),
                    F.percentile_approx("event", [0.5, 0.95, 0.99]).alias("event"),
                )
                .first()
            )
            if lat is not None and lat["ingest"]:
                for q, v_ing, v_evt in zip(("p50", "p95", "p99"), lat["ingest"], lat["event"], strict=True):
                    sm.PIPELINE_LATENCY.labels(q).set(max(0.0, v_ing))
                    sm.EVENT_LATENCY.labels(q).set(max(0.0, v_evt))
            for r in good.groupBy("kafka_topic").agg(F.max("event_timestamp").alias("mx")).collect():
                if r["mx"] is not None:
                    sm.LATEST_EVENT_TS.labels(r["kafka_topic"]).set(r["mx"].timestamp())
        log.info("bronze batch committed", extra={"batch_id": batch_id, "rows": counts, "corrupt": n_corrupt})
    finally:
        decoded.unpersist()


def start_bronze_query(spark: SparkSession, cfg: Any, once: bool, starting_offsets: str) -> Any:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.get_path("kafka.bootstrap_servers"))
        .option("subscribe", ",".join(TOPIC_ENTITY))
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", int(cfg.get_path("kafka.consumer.max_offsets_per_trigger", 200000)))
        .option("failOnDataLoss", str(cfg.get_path("kafka.consumer.fail_on_data_loss", False)).lower())
    )
    if cfg.get_path("kafka.security_protocol", "PLAINTEXT") != "PLAINTEXT":
        # Cloud (Confluent Cloud / MSK): credentials are injected as environment variables from a
        # Databricks secret scope ({{secrets/ecom-platform/kafka-api-key}}), never stored in config.
        user, secret = os.environ["KAFKA_API_KEY"], os.environ["KAFKA_API_SECRET"]
        jaas = (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{user}" password="{secret}";'
        )
        reader = (
            reader.option("kafka.security.protocol", cfg.get_path("kafka.security_protocol"))
            .option("kafka.sasl.mechanism", cfg.get_path("kafka.sasl_mechanism", "PLAIN"))
            .option("kafka.sasl.jaas.config", jaas)
        )
    writer = (
        reader.load()
        .writeStream.queryName("bronze_kafka_ingest")
        .foreachBatch(lambda df, bid: write_batch(df, bid, cfg))
        .option("checkpointLocation", checkpoint("bronze_kafka_ingest"))
    )
    if once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(
            processingTime=f"{int(cfg.get_path('streaming.bronze_trigger_seconds', 10))} seconds"
        )
    return writer.start()


def _rt_merge(target: str, keys: list[str]) -> Any:
    def handler(df: DataFrame, _batch_id: int) -> None:
        if df.isEmpty():
            return
        merge_upsert(df.sparkSession, df, table(target), keys)

    return handler


def start_realtime_queries(spark: SparkSession, cfg: Any, once: bool) -> list[Any]:
    wm = f"{int(cfg.get_path('streaming.realtime_watermark_minutes', 10))} minutes"
    trigger: dict[str, Any] = {"availableNow": True} if once else {"processingTime": "30 seconds"}
    queries = []
    orders = read_stream_table(spark, bronze_event_table("orders"))
    payments = read_stream_table(spark, bronze_event_table("payments"))
    for name, agg, keys in (
        (
            "rt_sales_minute",
            sales_minute_agg(orders, wm),
            ["window_start", "currency", "sales_channel", "country"],
        ),
        (
            "rt_payments_minute",
            payments_minute_agg(payments, wm),
            ["window_start", "currency", "payment_method"],
        ),
    ):
        q = (
            agg.writeStream.queryName(name)
            .outputMode("update")
            .foreachBatch(_rt_merge(f"gold.{name}", keys))
            .option("checkpointLocation", checkpoint(name))
            .trigger(**trigger)
            .start()
        )
        queries.append(q)
    return queries


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--once", action="store_true", help="process available data and stop (availableNow)")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--no-realtime", action="store_true", help="skip the real-time aggregate queries")
    args = parser.parse_args(argv)
    cfg = load_config()
    port = int(os.environ.get("STREAM_METRICS_PORT", "0"))
    if port:
        from pipelines.common.metrics import serve_metrics

        serve_metrics(port)
    with log_context("bronze-stream"):
        spark = get_spark("bronze-stream", {"spark.sql.streaming.forceDeleteTempCheckpointLocation": "true"})
        spark.streams.addListener(sm.PrometheusStreamingListener())
        ensure_bronze_tables(spark)
        bronze = start_bronze_query(spark, cfg, args.once, args.starting_offsets)
        if args.once:
            bronze.awaitTermination()
            if not args.no_realtime:
                for q in start_realtime_queries(spark, cfg, once=True):
                    q.awaitTermination()
            counts = {e: read_table(spark, bronze_event_table(e)).count() for e in TOPIC_ENTITY.values()}
            log.info("bronze drain complete", extra={"table_rows": counts})
            print(counts)
            spark.stop()
            return 0
        if not args.no_realtime:
            start_realtime_queries(spark, cfg, once=False)
        spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
