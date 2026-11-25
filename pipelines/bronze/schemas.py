"""Bronze table schemas."""

from __future__ import annotations

from pyspark.sql.types import (
    BinaryType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Uniform schema for every Kafka-fed Bronze table: envelope + Kafka coordinates
# + ingestion metadata + the untouched payload (JSON) and original bytes.
BRONZE_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_version", IntegerType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("source_system", StringType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("raw_value", BinaryType(), True),
        StructField("schema_id", IntegerType(), True),
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("kafka_key", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("processing_date", DateType(), False),
        StructField("batch_id", LongType(), False),
    ]
)

CORRUPT_EVENT_SCHEMA = StructType(
    [
        StructField("kafka_topic", StringType(), False),
        StructField("kafka_partition", IntegerType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("kafka_key", StringType(), True),
        StructField("raw_value", BinaryType(), True),
        StructField("decode_error", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("processing_date", DateType(), False),
        StructField("batch_id", LongType(), False),
    ]
)


def bronze_event_table(entity: str) -> str:
    return f"bronze.{entity}_events"
