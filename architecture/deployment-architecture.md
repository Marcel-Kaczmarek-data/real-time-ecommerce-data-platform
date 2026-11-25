# Deployment Architecture

Two deployment targets share one codebase.

## 1. Local development (Docker Compose)

```mermaid
flowchart TB
    subgraph host["Developer machine - Docker Compose network: ecom-net"]
        subgraph core["profile: core (always)"]
            KAFKA["kafka<br/>cp-kafka 7.8 KRaft<br/>:9092"]
            SR["schema-registry<br/>:8081"]
            PG[("postgres 16<br/>:5432<br/>db: ecommerce, airflow")]
            MINIO[("minio<br/>:9000 / console :9001")]
            INIT["minio-init / kafka-init<br/>(one-shot)"]
        end
        subgraph proc["profile: processing"]
            SPARKS["spark-streaming<br/>Bronze stream (local[4])<br/>Spark UI :4040"]
            PRODUCER["event-producer<br/>metrics :8000"]
        end
        subgraph orch["profile: orchestration"]
            AFW["airflow-webserver :8088"]
            AFS["airflow-scheduler<br/>LocalExecutor<br/>Spark jobs in local mode"]
        end
        subgraph bi["profile: dashboard"]
            STL["streamlit :8501"]
        end
        subgraph mon["profile: monitoring"]
            PROM["prometheus :9090"]
            PGW["pushgateway :9091"]
            KEXP["kafka-exporter"]
            STATSD["statsd-exporter"]
            CADV["cadvisor"]
            GRAF["grafana :3000"]
        end
        subgraph cdc["profile: cdc (optional)"]
            KCON["kafka-connect<br/>Debezium :8083"]
        end
    end
    PRODUCER --> KAFKA
    PRODUCER --> SR
    SPARKS --> KAFKA
    SPARKS --> MINIO
    AFS --> MINIO
    AFS --> PG
    STL --> MINIO
    KCON --> PG
    KCON --> KAFKA
    PROM --> KEXP & STATSD & CADV & PGW & MINIO
    GRAF --> PROM
```

| Profile | Services | Purpose |
|---|---|---|
| `core` | kafka, schema-registry, postgres, minio, init jobs | Minimum to run producers and Spark jobs |
| `processing` | spark-streaming, event-producer | Continuous streaming path |
| `orchestration` | airflow-init, airflow-webserver, airflow-scheduler | Scheduled batch, backfills, maintenance |
| `dashboard` | streamlit | Business UI |
| `monitoring` | prometheus, pushgateway, kafka-exporter, statsd-exporter, cadvisor, grafana | Observability |
| `cdc` | kafka-connect (Debezium) | Optional CDC from PostgreSQL |

Spark runs in **local mode inside containers** (one JVM per job). On a single
machine this is the most resource-efficient option. A standalone cluster adds
scheduling overhead without adding parallelism. The `SPARK_MASTER` variable
switches jobs to any cluster URL.

## 2. Optional cloud deployment (AWS + Databricks)

```mermaid
flowchart TB
    subgraph aws["AWS account (Terraform: infrastructure/terraform)"]
        subgraph s3["S3 (SSE-KMS, versioned, public access blocked)"]
            LAND[("landing bucket<br/>lifecycle: 30d → IA, 180d expire")]
            LAKE[("lakehouse bucket<br/>bronze / silver / gold / audit")]
            LOGS[("access-log bucket")]
        end
        KMS["KMS CMK"]
        IAM["IAM roles<br/>databricks-uc-access (least privilege)<br/>ingestion-writer, bi-reader"]
        CW["CloudWatch<br/>log groups, metric alarms, SNS"]
        MSK["Amazon MSK or Confluent Cloud<br/>(not provisioned by default)"]
    end
    subgraph dbx["Databricks workspace"]
        UC["Unity Catalog<br/>catalog ecommerce_dev / _prod"]
        J1["Job: bronze-streaming<br/>continuous"]
        J2["Job: medallion-batch<br/>silver → dq → gold → maintenance"]
        SQLW["SQL Warehouse<br/>(Power BI DirectQuery)"]
    end
    J1 --> MSK
    J1 & J2 --> UC --> LAKE
    IAM --> UC
    KMS --> s3
    LAKE --> LOGS
    SQLW --> UC
    CW -. alarms .-> SNS["SNS email"]
```

* Nothing is provisioned automatically. `terraform plan` is safe to run.
  `apply` is a manual, documented step ([docs/cost-analysis.md](../docs/cost-analysis.md)).
* Databricks jobs are defined as a Databricks Asset Bundle
  (`infrastructure/databricks/databricks.yml`). They call the same modules as
  local runs, so no notebook-only logic exists.
* `dev` and `prod` differ in bucket names, retention, KMS deletion window,
  alarm thresholds and cluster sizes (`infrastructure/terraform/environments/*`).
