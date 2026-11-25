# ADR 0002 — Apache Spark (PySpark) for processing

**Status:** Accepted

## Context
The platform needs streaming ingestion, large incremental batch transforms
(MERGE, window functions, joins over millions of rows) and portability to Databricks.

## Decision
Use **Apache Spark 3.5 (PySpark)** for both Structured Streaming (Bronze and
the real-time aggregate) and batch (Silver, Gold, DQ). Locally, jobs run in
local mode inside containers. In the cloud they run on Databricks job clusters.

## Consequences
* One engine and one API for stream and batch, with first-class Delta Lake support.
* The same code runs on Databricks (`pipelines/common/spark.py` detects the runtime).
* JVM start-up (~10 s per job) is noticeable on tiny local datasets. This is
  acceptable because jobs are coarse-grained.
* The serving layer deliberately does **not** use Spark (see ADR 0008).

## Alternatives considered
* **Apache Flink**: better for low-latency stateful streaming. It is weaker for
  batch MERGE and lakehouse maintenance, and is not available on Databricks.
* **dbt on a warehouse**: great for SQL modelling, but it has no streaming
  ingestion, and the cloud path would need a paid warehouse.
* **Polars / DuckDB only**: fast on one node, but neither distributed nor streaming.
