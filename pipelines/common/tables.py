"""Addressing of lakehouse tables independent of the runtime.

* local / CI: path-based Delta tables under ``lakehouse.root``
  (``s3a://lakehouse/silver/orders`` or ``file:///tmp/lh/silver/orders`` in tests)
* Databricks: Unity Catalog names (``<catalog>.silver.orders``)

All pipeline code reads/writes through :func:`read_table` / :class:`TableRef`
so the same module runs unchanged in both places.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pipelines.common.config import Config, load_config

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

LAYERS = ("bronze", "silver", "gold", "audit", "quarantine")


@dataclass(frozen=True)
class TableRef:
    layer: str
    name: str
    root: str
    catalog: str | None = None

    @property
    def path(self) -> str:
        return f"{self.root.rstrip('/')}/{self.layer}/{self.name}"

    @property
    def qualified_name(self) -> str:
        if self.catalog:
            return f"{self.catalog}.{self.layer}.{self.name}"
        return f"delta.`{self.path}`"

    @property
    def fqn(self) -> str:
        """Human-readable name used in logs, DQ results and lineage."""
        return f"{self.layer}.{self.name}"

    @property
    def uses_catalog(self) -> bool:
        return self.catalog is not None


def lakehouse_root(cfg: Config | None = None) -> str:
    cfg = cfg or load_config()
    return os.environ.get("LAKEHOUSE_ROOT") or str(cfg.get_path("lakehouse.root"))


def checkpoint_root(cfg: Config | None = None) -> str:
    """Explicit CHECKPOINT_ROOT wins, then <LAKEHOUSE_ROOT>/_checkpoints, then the config value."""
    cfg = cfg or load_config()
    if os.environ.get("CHECKPOINT_ROOT"):
        return os.environ["CHECKPOINT_ROOT"]
    override = os.environ.get("LAKEHOUSE_ROOT")
    if override:
        return f"{override.rstrip('/')}/_checkpoints"
    return str(cfg.get_path("lakehouse.checkpoint_root"))


def table(fqn: str, cfg: Config | None = None) -> TableRef:
    """Resolve ``"silver.orders"`` to a :class:`TableRef`."""
    cfg = cfg or load_config()
    layer, _, name = fqn.partition(".")
    if layer not in LAYERS or not name:
        raise ValueError(f"Invalid table name '{fqn}', expected '<layer>.<table>' with layer in {LAYERS}")
    catalog = None
    if cfg.get_path("lakehouse.table_format") == "catalog" and not os.environ.get("LAKEHOUSE_ROOT"):
        catalog = str(cfg.get_path("lakehouse.catalog"))
    return TableRef(layer=layer, name=name, root=lakehouse_root(cfg), catalog=catalog)


def checkpoint(name: str, cfg: Config | None = None) -> str:
    return f"{checkpoint_root(cfg).rstrip('/')}/{name}"


def table_exists(spark: SparkSession, ref: TableRef) -> bool:
    from delta.tables import DeltaTable

    if ref.uses_catalog:
        return bool(spark.catalog.tableExists(ref.qualified_name))
    try:
        return bool(DeltaTable.isDeltaTable(spark, ref.path))
    except Exception:  # path does not exist / not accessible yet
        return False


def read_table(spark: SparkSession, fqn: str | TableRef) -> DataFrame:
    ref = table(fqn) if isinstance(fqn, str) else fqn
    if ref.uses_catalog:
        return spark.read.table(ref.qualified_name)
    return spark.read.format("delta").load(ref.path)


def delta_table(spark: SparkSession, ref: TableRef) -> Any:
    from delta.tables import DeltaTable

    if ref.uses_catalog:
        return DeltaTable.forName(spark, ref.qualified_name)
    return DeltaTable.forPath(spark, ref.path)


def writer_target(writer: Any, ref: TableRef) -> Any:
    """Finish a DataFrameWriter against a path or a catalog table."""
    if ref.uses_catalog:
        return writer.saveAsTable(ref.qualified_name)
    return writer.save(ref.path)


def read_stream_table(spark: SparkSession, fqn: str | TableRef, **options: str) -> DataFrame:
    """Incremental (streaming) read of a Delta table."""
    ref = table(fqn) if isinstance(fqn, str) else fqn
    reader = spark.readStream.format("delta")
    for key, value in options.items():
        reader = reader.option(key, value)
    if ref.uses_catalog:
        return reader.table(ref.qualified_name)
    return reader.load(ref.path)


def drop_table(spark: SparkSession, ref: TableRef) -> None:
    """Drop a table (catalog) or delete its files (path-based) - used for full refreshes of derived tables."""
    if ref.uses_catalog:
        spark.sql(f"DROP TABLE IF EXISTS {ref.qualified_name}")
        return
    if ref.path.startswith("file://"):
        import shutil

        shutil.rmtree(ref.path[len("file://") :], ignore_errors=True)
        return
    from pipelines.common.storage import s3_client

    bucket, _, prefix = ref.path.split("://", 1)[1].partition("/")
    s3 = s3_client()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
