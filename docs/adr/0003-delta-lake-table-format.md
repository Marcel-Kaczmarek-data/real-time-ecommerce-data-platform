# ADR 0003 — Delta Lake as the table format

**Status:** Accepted

## Decision
Store every layer as **Delta Lake** tables: OSS `delta-spark` 3.3 locally and
Databricks Runtime Delta in the cloud.

## Rationale
* ACID transactions on object storage, so readers never see partial writes.
* `MERGE` for idempotent upserts and SCD2.
* Idempotent streaming writes via `txnAppId` / `txnVersion` in `foreachBatch`.
* Time travel and `RESTORE` for recovery and audit.
* Change Data Feed for incremental change propagation.
* Readable without a JVM through **delta-rs**, which the serving layer uses.

## OSS vs Databricks differences that affect this project
| Capability | OSS Delta 3.3 (local) | Databricks |
|---|---|---|
| `OPTIMIZE` / `ZORDER BY` | Yes | Yes (+ Liquid Clustering) |
| Optimized writes / auto-compaction | Scheduled `OPTIMIZE` (Airflow `delta_table_maintenance`) | Table properties |
| `VACUUM` | Yes (retention check enforced) | Yes |
| Change Data Feed | Yes | Yes |
| Deletion vectors | Supported but **disabled** here, to keep tables readable by delta-rs 0.24 | Enabled by default |
| Predictive optimisation | No | Yes (UC managed tables) |
| Concurrent writers on S3 | `S3SingleDriverLogStore`: one writer per table at a time | Managed commit service |

## Alternatives considered
* **Apache Iceberg**: strong, with an engine-neutral catalog. Delta was
  chosen because the cloud target is Databricks.
* **Plain Parquet**: no transactions, MERGE or time travel.
