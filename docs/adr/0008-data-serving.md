# ADR 0008 — Data serving architecture

**Status:** Accepted

## Context
The Streamlit dashboard must query Gold interactively (filters respond in under
a second) without hard-coded numbers. Power BI needs a supported connector.

## Decision
* **Local / Streamlit:** `serving/` exposes Gold Delta tables as **DuckDB
  views over delta-rs Arrow datasets**. There is no JVM, it is a single
  process, filters are pushed down to Parquet, and queries return in well under
  a second on local data volumes.
* **Power BI (local):** Gold marts and dimensions are exported to the
  PostgreSQL `analytics` schema (`pipelines/gold/publish_serving.py`).
  Power BI reads them with its native PostgreSQL connector (Import mode).
* **Power BI (cloud):** a Databricks SQL Warehouse over Unity Catalog Gold tables.

## Alternatives considered
* Spark Thrift Server: a JVM SQL endpoint needing ~2 GB RAM, and slow for interactive use.
* Trino: an excellent engine, but one more stateful service plus a metastore to run locally.
* DuckDB `delta` extension: it is downloaded at runtime. delta-rs is a pinned
  pip dependency and reproducible offline.
