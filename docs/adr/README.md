# Architecture Decision Records

| # | Decision | Status |
|---|---|---|
| [0001](0001-kafka-for-event-streaming.md) | Apache Kafka (+ Schema Registry) for event ingestion | Accepted |
| [0002](0002-spark-for-processing.md) | Apache Spark / PySpark for batch and stream processing | Accepted |
| [0003](0003-delta-lake-table-format.md) | Delta Lake as the table format | Accepted |
| [0004](0004-medallion-architecture.md) | Medallion (Bronze/Silver/Gold) layering | Accepted |
| [0005](0005-batch-vs-streaming.md) | Hybrid: streaming to Bronze, incremental micro-batch to Silver/Gold | Accepted |
| [0006](0006-partitioning-strategy.md) | Kafka keys/partitions and Delta partitioning | Accepted |
| [0007](0007-data-modeling.md) | Kimball star schema with SCD1/SCD2 and hashed surrogate keys | Accepted |
| [0008](0008-data-serving.md) | DuckDB over delta-rs for local serving; Postgres export for Power BI | Accepted |
| [0009](0009-local-vs-cloud.md) | Local-first (Docker/MinIO), optional AWS + Databricks | Accepted |
| [0010](0010-reliability-tradeoffs.md) | At-least-once + idempotent processing instead of claimed exactly-once | Accepted |
| [0011](0011-object-storage-seaweedfs.md) | SeaweedFS as local S3-compatible storage (MinIO OSS archived) | Accepted |

Each ADR follows: context → decision → consequences → alternatives considered.
