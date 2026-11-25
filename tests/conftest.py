"""Shared pytest fixtures.

Spark tests (marker ``spark``) need Java + PySpark + the Delta jars: they run
in the platform container (``make test``) and in CI. They are skipped
automatically elsewhere (e.g. on the Windows host).
"""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest


def _spark_available() -> bool:
    if not shutil.which("java") and not os.environ.get("JAVA_HOME"):
        return False
    try:
        import delta  # noqa: F401
        import pyspark  # noqa: F401
    except ImportError:
        return False
    return os.name != "nt"


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _spark_available():
        return
    skip = pytest.mark.skip(reason="Spark runtime not available (run inside the platform container)")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def lakehouse_root() -> Iterator[Path]:
    root = Path(tempfile.mkdtemp(prefix="lakehouse-test-"))
    os.environ["LAKEHOUSE_ROOT"] = f"file://{root}"
    os.environ["CHECKPOINT_ROOT"] = f"file://{root}/_checkpoints"
    yield root
    shutil.rmtree(root, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(lakehouse_root: Path):  # type: ignore[no-untyped-def]
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("ecom-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
