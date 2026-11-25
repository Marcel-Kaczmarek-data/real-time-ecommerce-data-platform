"""Landing zone -> Bronze file loader (Spark).

For every new object under a source's landing prefix:
  1. compute SHA-256 and register it (same content already loaded -> DUPLICATE, skipped)
  2. parse (CSV / JSON / Parquet) in PERMISSIVE mode, capturing malformed rows
  3. malformed rows -> ``quarantine.batch_corrupt_records``; unreadable files ->
     moved under ``quarantine/`` in the landing bucket and marked FAILED
  4. valid rows + ingestion metadata -> Bronze (append, ``mergeSchema`` for
     additive schema evolution, ``txnAppId=batch-<source>`` / ``txnVersion=file_id``
     so a retried load of the same file is a no-op)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType, TimestampType

from ingestion.batch.registry import Registry
from pipelines.common.delta_utils import append_idempotent, ensure_table
from pipelines.common.logs import get_logger
from pipelines.common.storage import sha256_bytes
from pipelines.common.tables import read_table, table, table_exists

log = get_logger(__name__)
CORRUPT_COL = "_corrupt_record"
META_COLUMNS = [
    "ingestion_timestamp",
    "source_system",
    "source_file",
    "file_checksum",
    "batch_id",
    "processing_date",
    "raw_payload",
]

QUARANTINE_SCHEMA = StructType(
    [
        StructField("source_name", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("raw_record", StringType(), True),
        StructField("reason", StringType(), False),
        StructField("batch_id", StringType(), False),
        StructField("ingestion_timestamp", TimestampType(), False),
        StructField("processing_date", DateType(), False),
        StructField("file_id", LongType(), False),
    ]
)


@dataclass
class LoadResult:
    source: str
    files_loaded: int = 0
    files_duplicate: int = 0
    files_failed: int = 0
    rows_loaded: int = 0
    rows_quarantined: int = 0
    errors: list[str] = field(default_factory=list)


def _read(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    if fmt == "parquet":
        return spark.read.parquet(path)
    if fmt == "csv":
        header = spark.read.option("header", "true").csv(path).columns
        schema = StructType(
            [StructField(c, StringType(), True) for c in header]
            + [StructField(CORRUPT_COL, StringType(), True)]
        )
        return (
            spark.read.option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .option("enforceSchema", "false")
            .schema(schema)
            .csv(path)
        )
    if fmt == "json":
        return (
            spark.read.option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .json(path)
        )
    if fmt == "json_document":
        return spark.read.option("multiLine", "true").option("mode", "FAILFAST").json(path)
    raise ValueError(f"unsupported format {fmt}")


def align_null_columns(spark: SparkSession, df: DataFrame, target: Any) -> DataFrame:
    """Give columns that are null in every row of the file a well-defined type.

    A file where a column is entirely null (e.g. no promotions that month)
    carries no real type information: Parquet writers emit a NULL logical type
    that Spark reads as INT, CSV/JSON readers infer NullType. Such columns take
    the existing Bronze type, or STRING for a new table, so they never break
    the schema merge with typed files.
    """
    cols = [c for c in df.columns if c != CORRUPT_COL]
    if not cols:
        return df
    non_null = df.agg(*[F.count(F.col(f"`{c}`")).alias(c) for c in cols]).first()
    all_null = [c for c in cols if non_null is not None and non_null[c] == 0]
    if not all_null:
        return df
    existing = (
        {f.name: f.dataType for f in read_table(spark, target).schema.fields}
        if table_exists(spark, target)
        else {}
    )
    for name in all_null:
        df = df.withColumn(name, F.col(f"`{name}`").cast(existing.get(name, StringType())))
    log.info("all-null columns re-typed", extra={"columns": all_null, "table": target.fqn})
    return df


def load_source(
    spark: SparkSession, registry: Registry, s3: Any, bucket: str, name: str, source: dict[str, Any]
) -> LoadResult:
    fmt = source.get("format") or ("parquet" if source["type"] == "postgres" else "json_document")
    prefix = source["landing"].rstrip("/") + "/"
    result = LoadResult(name)
    known = registry.known_keys(name)
    keys: list[tuple[str, int]] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"] not in known and not obj["Key"].endswith("/"):
                keys.append((obj["Key"], int(obj["Size"])))
    target = table(source["target"])
    for key, size in sorted(keys):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        record = registry.register_file(name, key, sha256_bytes(body), size, fmt)
        if record.status == "DUPLICATE":
            result.files_duplicate += 1
            registry.set_file_status(
                record.file_id, "DUPLICATE", 0, 0, "content already loaded under another key"
            )
            log.warning("duplicate file skipped", extra={"source": name, "key": key})
            continue
        batch_id = str(uuid.uuid4())
        path = f"s3a://{bucket}/{key}"
        try:
            if size == 0:
                raise ValueError("empty file")
            df = _read(spark, path, fmt)
            df = df.cache()
            if CORRUPT_COL in df.columns:
                bad = df.filter(F.col(CORRUPT_COL).isNotNull())
                good = df.filter(F.col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
            else:
                bad, good = None, df
            n_bad = bad.count() if bad is not None else 0
            good = align_null_columns(spark, good, target)
            base_cols = [c for c in good.columns if c not in META_COLUMNS]
            enriched = (
                good.withColumn("raw_payload", F.to_json(F.struct(*[F.col(f"`{c}`") for c in base_cols])))
                .withColumn("ingestion_timestamp", F.current_timestamp())
                .withColumn("source_system", F.lit(source["type"] + ":" + name))
                .withColumn("source_file", F.lit(path))
                .withColumn("file_checksum", F.lit(record.checksum))
                .withColumn("batch_id", F.lit(batch_id))
                .withColumn("processing_date", F.current_date())
            )
            n_good = enriched.count()
            if n_good:
                append_idempotent(
                    enriched, target, app_id=f"batch-{name}", version=record.file_id, merge_schema=True
                )
            if n_bad and bad is not None:
                ensure_table(
                    spark,
                    table("quarantine.batch_corrupt_records"),
                    QUARANTINE_SCHEMA,
                    partition_by=["processing_date"],
                )
                q = bad.select(
                    F.lit(name).alias("source_name"),
                    F.lit(path).alias("source_file"),
                    F.col(CORRUPT_COL).alias("raw_record"),
                    F.lit("malformed record").alias("reason"),
                    F.lit(batch_id).alias("batch_id"),
                    F.current_timestamp().alias("ingestion_timestamp"),
                    F.current_date().alias("processing_date"),
                    F.lit(record.file_id).cast("long").alias("file_id"),
                )
                append_idempotent(
                    q, table("quarantine.batch_corrupt_records"), f"quarantine-{name}", record.file_id
                )
            df.unpersist()
            registry.set_file_status(record.file_id, "LOADED", n_good, n_bad, batch_id=batch_id)
            result.files_loaded += 1
            result.rows_loaded += n_good
            result.rows_quarantined += n_bad
            log.info("file loaded", extra={"source": name, "key": key, "rows": n_good, "quarantined": n_bad})
        except Exception as exc:
            err = f"{type(exc).__name__}: {str(exc)[:1500]}"
            quarantine_key = f"quarantine/{key}"
            s3.copy_object(Bucket=bucket, Key=quarantine_key, CopySource={"Bucket": bucket, "Key": key})
            s3.delete_object(Bucket=bucket, Key=key)
            registry.set_file_status(record.file_id, "FAILED", 0, 0, err, batch_id)
            result.files_failed += 1
            result.errors.append(f"{key}: {err}")
            log.error("file failed and quarantined", extra={"source": name, "key": key, "error": err})
    return result
