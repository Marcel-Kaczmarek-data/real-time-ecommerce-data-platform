"""Real-time (operational) per-minute aggregates over Bronze event streams.

These are approximate *operational* views: an event-time watermark bounds the
state, so events arriving later than the watermark are dropped from them
(counted in ``ecom_stream_rows_dropped_by_watermark_total``). The reconciled
numbers come from the Silver/Gold batch path.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

from schemas.contracts import spark_ddl

RT_SALES_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("currency", StringType(), True),
        StructField("sales_channel", StringType(), True),
        StructField("country", StringType(), True),
        StructField("orders_created", LongType(), True),
        StructField("orders_cancelled", LongType(), True),
        StructField("gross_amount_local", DoubleType(), True),
        StructField("items_ordered", LongType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

RT_PAYMENTS_SCHEMA = StructType(
    [
        StructField("window_start", TimestampType(), False),
        StructField("window_end", TimestampType(), False),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payments_captured", LongType(), True),
        StructField("payments_failed", LongType(), True),
        StructField("captured_amount_local", DoubleType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


def _parsed(events: DataFrame, entity: str) -> DataFrame:
    return events.select(
        "event_id", "event_type", "event_timestamp", F.from_json("raw_payload", spark_ddl(entity)).alias("p")
    )


def sales_minute_agg(order_events: DataFrame, watermark: str) -> DataFrame:
    o = _parsed(order_events, "orders").filter(F.col("event_type").isin("order_created", "order_cancelled"))
    return (
        o.withWatermark("event_timestamp", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(
            F.window("event_timestamp", "1 minute").alias("w"),
            F.col("p.currency").alias("currency"),
            F.col("p.sales_channel").alias("sales_channel"),
            F.col("p.country").alias("country"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "order_created", 1).otherwise(0))
            .cast("long")
            .alias("orders_created"),
            F.sum(F.when(F.col("event_type") == "order_cancelled", 1).otherwise(0))
            .cast("long")
            .alias("orders_cancelled"),
            F.sum(
                F.when(F.col("event_type") == "order_created", F.col("p.total_amount")).otherwise(0.0)
            ).alias("gross_amount_local"),
            F.sum(F.when(F.col("event_type") == "order_created", F.col("p.item_count")).otherwise(0))
            .cast("long")
            .alias("items_ordered"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "currency",
            "sales_channel",
            "country",
            "orders_created",
            "orders_cancelled",
            "gross_amount_local",
            "items_ordered",
            F.current_timestamp().alias("updated_at"),
        )
    )


def payments_minute_agg(payment_events: DataFrame, watermark: str) -> DataFrame:
    p = _parsed(payment_events, "payments").filter(
        F.col("event_type").isin("payment_captured", "payment_failed")
    )
    return (
        p.withWatermark("event_timestamp", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(
            F.window("event_timestamp", "1 minute").alias("w"),
            F.col("p.currency").alias("currency"),
            F.col("p.payment_method").alias("payment_method"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "payment_captured", 1).otherwise(0))
            .cast("long")
            .alias("payments_captured"),
            F.sum(F.when(F.col("event_type") == "payment_failed", 1).otherwise(0))
            .cast("long")
            .alias("payments_failed"),
            F.sum(
                F.when(F.col("event_type") == "payment_captured", F.col("p.payment_amount")).otherwise(0.0)
            ).alias("captured_amount_local"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "currency",
            "payment_method",
            "payments_captured",
            "payments_failed",
            "captured_amount_local",
            F.current_timestamp().alias("updated_at"),
        )
    )
