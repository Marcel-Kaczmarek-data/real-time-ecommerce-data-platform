# Databricks deployment (optional)

The jobs run the **same Python modules** as the local stack. `infrastructure/databricks/jobs/*.py`
are thin entry points that put the synced repository on `sys.path`, set `PLATFORM_ENV=cloud` and call
each module's `main()`. On Databricks:
* `pipelines/common/spark.py` reuses the runtime's SparkSession (no local Delta/S3A setup);
* `config/cloud.yaml` switches tables to Unity Catalog names (`<catalog>.<layer>.<table>`) and checkpoints to S3;
* Kafka credentials come from the secret scope as environment variables (SASL_SSL).

## Jobs (`resources/jobs.yml`)
| Job | Schedule | Tasks |
|---|---|---|
| `bronze_streaming` | continuous (deployed **paused**) | Kafka → Bronze (+ real-time aggregates) |
| `medallion_batch` | every 30 min (paused) | Silver → critical DQ gate → Gold → Gold DQ + reconciliation |
| `maintenance` | nightly (paused) | retention, OPTIMIZE, VACUUM |

Streaming runs only in the continuous job, never inside a scheduled batch job (same rule as Airflow locally).

## Deploy
```bash
# 1. Infrastructure (bucket, IAM role, Unity Catalog catalog + schemas) - see ../terraform
# 2. Secrets
databricks secrets create-scope ecom-platform
databricks secrets put-secret ecom-platform kafka-bootstrap-servers
databricks secrets put-secret ecom-platform schema-registry-url
databricks secrets put-secret ecom-platform kafka-api-key
databricks secrets put-secret ecom-platform kafka-api-secret
databricks secrets put-secret ecom-platform pii-hash-salt
# 3. Bundle
cd infrastructure/databricks
databricks bundle validate -t dev --var="cluster_policy_id=<from terraform output>"
databricks bundle deploy   -t dev --var="cluster_policy_id=<...>"
databricks bundle run medallion_batch -t dev       # first run on demand
```
Jobs are deployed paused, so nothing consumes DBUs until you start them. Remove everything with
`databricks bundle destroy -t dev`.

## OSS Delta vs Databricks differences used here
| Feature | Local (OSS Delta 3.3) | Databricks |
|---|---|---|
| Table addressing | path-based (`delta.\`s3a://...\``) | Unity Catalog names |
| Compaction | nightly `OPTIMIZE` (maintenance DAG) | `optimizeWrite` on streaming cluster + predictive optimisation (UC managed tables) |
| Deletion vectors | disabled (delta-rs serving compatibility) | can be enabled; the serving layer moves to Databricks SQL |
| Serving for BI | DuckDB over delta-rs / PostgreSQL export | SQL Warehouse |
| Lineage | documented (`docs/data-contracts.md`, data-flow) | automatic Unity Catalog lineage |
