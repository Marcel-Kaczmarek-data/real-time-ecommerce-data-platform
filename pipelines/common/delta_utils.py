"""Delta Lake helpers: table bootstrap, idempotent appends and MERGE upserts."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from pipelines.common.logs import get_logger
from pipelines.common.tables import TableRef, delta_table, table_exists

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

log = get_logger(__name__)

DEFAULT_PROPERTIES = {
    "delta.logRetentionDuration": "interval 30 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
    # Keep tables readable by delta-rs (serving layer): no deletion vectors / column mapping.
    "delta.enableDeletionVectors": "false",
}


def ensure_table(
    spark: SparkSession,
    ref: TableRef,
    schema: StructType,
    partition_by: Sequence[str] = (),
    properties: dict[str, str] | None = None,
    change_data_feed: bool = False,
    comment: str | None = None,
) -> None:
    """Create the Delta table if it does not exist (schema enforcement from day one)."""
    if table_exists(spark, ref):
        return
    from delta.tables import DeltaTable

    props = {**DEFAULT_PROPERTIES, **(properties or {})}
    if change_data_feed:
        props["delta.enableChangeDataFeed"] = "true"
    builder = DeltaTable.createIfNotExists(spark)
    builder = builder.tableName(ref.qualified_name) if ref.uses_catalog else builder.location(ref.path)
    builder = builder.addColumns(schema)
    if partition_by:
        builder = builder.partitionedBy(*partition_by)
    for key, value in props.items():
        builder = builder.property(key, value)
    if comment:
        builder = builder.comment(comment)
    builder.execute()
    log.info("delta table created", extra={"table": ref.fqn, "partitions": list(partition_by)})


def append_idempotent(
    df: DataFrame, ref: TableRef, app_id: str, version: int, merge_schema: bool = False
) -> None:
    """Append guarded by Delta transaction identifiers.

    Delta records (txnAppId, txnVersion) in the commit. A re-executed write with
    a version <= the last committed one for that app id is skipped, which makes
    ``foreachBatch`` sinks exactly-once per micro-batch.
    """
    writer = df.write.format("delta").mode("append").option("txnAppId", app_id).option("txnVersion", version)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if ref.uses_catalog:
        writer.saveAsTable(ref.qualified_name)
    else:
        writer.save(ref.path)


def merge_upsert(
    spark: SparkSession,
    source: DataFrame,
    ref: TableRef,
    keys: Iterable[str],
    update_condition: str | None = None,
    insert_only: bool = False,
    update_columns: Sequence[str] | None = None,
) -> None:
    """MERGE ``source`` into ``ref`` on ``keys``.

    * ``insert_only``: append rows whose key is absent (event dedup).
    * ``update_condition``: e.g. ``"s.updated_at > t.updated_at"`` so late or
      replayed records never overwrite newer state (out-of-order safety).
    """
    keys = list(keys)
    cond = " AND ".join(f"t.`{k}` <=> s.`{k}`" for k in keys)
    target = delta_table(spark, ref)
    merge = target.alias("t").merge(source.alias("s"), cond)
    if not insert_only:
        cols = update_columns or source.columns
        set_map = {f"`{c}`": f"s.`{c}`" for c in cols}
        merge = (
            merge.whenMatchedUpdate(condition=update_condition, set=set_map)
            if update_condition
            else (merge.whenMatchedUpdate(set=set_map))
        )
    merge.whenNotMatchedInsertAll().execute()


def last_commit_metrics(spark: SparkSession, ref: TableRef) -> dict[str, str]:
    """operationMetrics of the latest commit (rows inserted / updated, files...)."""
    history = delta_table(spark, ref).history(1).collect()
    return dict(history[0]["operationMetrics"] or {}) if history else {}


def merge_with_set(
    spark: SparkSession,
    source: DataFrame,
    ref: TableRef,
    keys: Iterable[str],
    set_map: dict[str, str] | None,
    partition_by: Sequence[str] = (),
    properties: dict[str, str] | None = None,
) -> None:
    """MERGE with explicit SQL update expressions; creates the table from ``source`` on first use.

    ``set_map`` = None means insert-only (``WHEN NOT MATCHED INSERT *``).
    """
    if not table_exists(spark, ref):
        props = {**DEFAULT_PROPERTIES, **(properties or {})}
        writer = source.limit(0).write.format("delta").mode("ignore")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if ref.uses_catalog:
            writer.saveAsTable(ref.qualified_name)
        else:
            writer.save(ref.path)
        set_props = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        spark.sql(f"ALTER TABLE {ref.qualified_name} SET TBLPROPERTIES ({set_props})")
    keys = list(keys)
    cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys)
    merge = delta_table(spark, ref).alias("t").merge(source.alias("s"), cond)
    if set_map:
        merge = merge.whenMatchedUpdate(set=set_map)
    merge.whenNotMatchedInsertAll().execute()
