# System Architecture

## Context

An international e-commerce company (30 countries, 4 sales channels, multiple
currencies) needs one platform that serves **near-real-time operational
analytics** (minutes) and **trusted historical reporting** (daily). The
platform is built as a **lakehouse** with the medallion pattern on Delta Lake.
It runs fully on a laptop (Docker Compose + MinIO). An optional AWS + Databricks
deployment uses the same code.

## Logical architecture

```mermaid
flowchart TB
    subgraph SRC["Data sources"]
        WEB["Web / mobile storefront<br/>(simulated customer journeys)"]
        OPS[("PostgreSQL<br/>operational DB<br/>ops schema")]
        FILES["Partner files<br/>CSV / JSON / Parquet"]
        API["REST APIs<br/>FX rates, marketing"]
    end

    subgraph ING["Ingestion"]
        PROD["Python Kafka producers<br/>Avro + envelope<br/>idempotent, acks=all"]
        SR["Schema Registry<br/>BACKWARD compatibility"]
        K{{"Apache Kafka (KRaft)<br/>10 topics incl. dead-letter"}}
        KC["Kafka Connect<br/>(Debezium CDC, optional)"]
        BATCH["Batch ingestion framework<br/>watermarks, checksums,<br/>file registry, quarantine"]
    end

    subgraph PROC["Processing - Apache Spark 3.5 + Delta Lake 3.3"]
        BRZ_S["Bronze streaming job<br/>Structured Streaming<br/>foreachBatch, txn-idempotent"]
        BRZ_B["Bronze batch loaders"]
        SLV["Silver jobs<br/>dedupe, MERGE, FX, masking,<br/>sessionisation, quarantine"]
        GLD["Gold jobs<br/>SCD1/SCD2 dims, facts, marts"]
        RT["Real-time aggregate<br/>watermarked stream"]
        DQ["Data quality engine<br/>YAML rules, severity gates"]
    end

    subgraph LAKE["Delta Lake on MinIO / S3"]
        B[("Bronze<br/>raw, append-only")]
        S[("Silver<br/>clean, conformed")]
        G[("Gold<br/>star schema + marts")]
        A[("Audit<br/>dq_results, pipeline_runs,<br/>ingestion_registry")]
        Q[("Quarantine")]
    end

    subgraph SERVE["Serving & BI"]
        DUCK["DuckDB + delta-rs<br/>SQL serving layer"]
        ST["Streamlit dashboard<br/>10 pages"]
        PG_A[("PostgreSQL analytics schema<br/>Power BI source")]
        PBI["Power BI semantic model"]
    end

    subgraph XC["Cross-cutting"]
        AF["Apache Airflow 2.10<br/>14 DAGs"]
        PROM["Prometheus + Pushgateway<br/>+ exporters"]
        GRAF["Grafana<br/>6 dashboards + alerts"]
        TF["Terraform (AWS)"]
        CI["GitHub Actions"]
    end

    WEB --> PROD --> K
    PROD -. register / validate .-> SR
    OPS --> KC --> K
    OPS --> BATCH
    FILES --> BATCH
    API --> BATCH
    K --> BRZ_S --> B
    K --> RT --> G
    BATCH --> BRZ_B --> B
    B --> SLV --> S --> GLD --> G
    SLV --> Q
    DQ --> A
    S -. checks .-> DQ
    G -. checks .-> DQ
    G --> DUCK --> ST
    G --> PG_A --> PBI
    AF -. schedules .-> BRZ_B & SLV & GLD & DQ
    BRZ_S & SLV & GLD & AF & K -. metrics .-> PROM --> GRAF
```

## Component responsibilities

| Component | Responsibility | Runs as |
|---|---|---|
| Event simulator (`data_generator/`) | Generates relationally consistent customer journeys (browse → cart → checkout → payment → fulfilment → returns). The output is deterministic for a given seed. | CLI / `event-producer` container |
| Kafka producers (`ingestion/kafka_producers/`) | Wrap each event in the standard envelope, validate it against the Avro contract, serialise via Schema Registry, publish with `acks=all` and idempotence on, route invalid events to `ecommerce.dead-letter` | Long-running container |
| Batch ingestion (`ingestion/batch/`) | Full or incremental loads from PostgreSQL, files and REST APIs into the landing bucket and Bronze. Tracks watermarks, checksums and a file registry. | Airflow tasks |
| Bronze streaming (`pipelines/bronze/stream_kafka_to_bronze.py`) | Reads all event topics, decodes Confluent Avro using the writer schema from the registry, appends to per-topic Bronze tables with idempotent Delta transactions, and quarantines corrupt records | Long-running `spark-streaming` container (or Databricks job) |
| Silver (`pipelines/silver/`) | Incremental processing (Delta streaming, `availableNow`) with deduplication, MERGE upserts, UTC/FX normalisation, PII masking, referential checks, quarantine and sessionisation | Airflow (bounded `availableNow` runs) or continuous |
| Gold (`pipelines/gold/`, `data_models/`) | Star schema with SCD1 and SCD2 dimensions, incremental facts, KPI marts and Customer 360 | Airflow |
| Data quality (`data_quality/`) | Declarative rules, severity-based gates, results in `audit.dq_results` | Airflow + inline in pipelines |
| Serving (`serving/`) | DuckDB views over Gold Delta tables (delta-rs, no JVM). Marts are also exported to PostgreSQL for Power BI. | Library used by Streamlit |
| Observability (`monitoring/`) | Prometheus scraping (kafka-exporter, MinIO, cAdvisor, statsd-exporter for Airflow) and a Pushgateway for batch job metrics. Grafana dashboards and alert rules. | Containers |

## Key design principles

1. **Raw is immutable.** Bronze keeps the full original payload (`raw_payload`)
   and Kafka coordinates, so any downstream table can be rebuilt by replay.
2. **Idempotency everywhere.** Bronze stream writes use Delta `txnAppId/txnVersion`.
   Silver and Gold use `MERGE` on natural keys. Batch loads are keyed by file
   checksum or watermark. Re-running any job produces the same result.
3. **Correctness over latency for reporting.** The real-time aggregate uses
   event-time watermarks and may drop very late events. The Silver/Gold batch
   path is MERGE-based and always incorporates late data. See
   [data-flow.md](data-flow.md#late-and-out-of-order-events).
4. **Gates, not hopes.** Critical data-quality failures stop promotion to Gold.
5. **Same code, two runtimes.** `pipelines/common/spark.py` detects Databricks
   and switches from path-based tables to Unity Catalog names.

See also: [data-flow.md](data-flow.md), [deployment-architecture.md](deployment-architecture.md),
[data-model.md](data-model.md), [failure-recovery.md](failure-recovery.md) and the ADRs in
[`docs/adr/`](../docs/adr).
