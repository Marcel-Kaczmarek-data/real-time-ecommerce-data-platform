"""SparkSession factory for local (Docker / CI) and Databricks runtimes.

Locally the session is configured for Delta Lake and MinIO (S3A). On
Databricks the runtime already provides a configured session, so we only
attach application-level settings.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from pipelines.common.config import Config, load_config

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _s3a_conf(cfg: Config) -> dict[str, str]:
    storage = cfg["storage"]
    endpoint = storage.get("endpoint_url") or ""
    conf = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.path.style.access": str(storage.get("path_style_access", False)).lower(),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(storage.get("ssl_enabled", True)).lower(),
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.connection.maximum": "100",
        "spark.hadoop.fs.s3a.attempts.maximum": "10",
        "spark.hadoop.fs.s3a.retry.limit": "10",
        "spark.hadoop.fs.s3a.connection.timeout": "60000",
    }
    if endpoint:
        conf["spark.hadoop.fs.s3a.endpoint"] = endpoint
    if storage.get("access_key"):
        conf["spark.hadoop.fs.s3a.aws.credentials.provider"] = (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        conf["spark.hadoop.fs.s3a.access.key"] = storage["access_key"]
        conf["spark.hadoop.fs.s3a.secret.key"] = storage["secret_key"]
    return conf


def build_spark_conf(cfg: Config, app_name: str, extra: dict[str, Any] | None = None) -> dict[str, str]:
    spark_cfg = cfg.get("spark", {})
    conf: dict[str, str] = {
        "spark.app.name": f"{spark_cfg.get('app_name_prefix', 'ecom')}-{app_name}",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.ui.showConsoleProgress": "false",
        "spark.sql.streaming.metricsEnabled": "true",
        # Keep Delta tables readable by delta-rs in the serving layer.
        "spark.databricks.delta.properties.defaults.enableDeletionVectors": "false",
    }
    if spark_cfg.get("driver_memory"):
        conf["spark.driver.memory"] = str(spark_cfg["driver_memory"])
    conf.update({k: str(v) for k, v in (spark_cfg.get("conf") or {}).items()})
    if _lakehouse_root(cfg).startswith("s3"):
        conf.update(_s3a_conf(cfg))
    conf.update({k: str(v) for k, v in (extra or {}).items()})
    return conf


def _lakehouse_root(cfg: Config) -> str:
    return os.environ.get("LAKEHOUSE_ROOT") or str(cfg.get_path("lakehouse.root", ""))


def get_spark(
    app_name: str, extra_conf: dict[str, Any] | None = None, cfg: Config | None = None
) -> SparkSession:
    """Return a SparkSession configured for the current environment."""
    from pyspark.sql import SparkSession

    cfg = cfg or load_config()
    if is_databricks():
        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        for key, value in (extra_conf or {}).items():
            spark.conf.set(key, str(value))
        return spark

    builder = SparkSession.builder.master(str(cfg.get_path("spark.master") or "local[*]"))
    for key, value in build_spark_conf(cfg, app_name, extra_conf).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
