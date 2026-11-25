# Implementation Plan — Real-Time E-Commerce Data Platform

Working principle: **a working end-to-end platform first, advanced features
second.** Every phase ends with tests, updated documentation, a record of
known limitations, and a Git commit. Nothing is pushed to GitHub and no cloud
resource is provisioned without explicit approval.

## Environment baseline (inspected at start)

| Item | Value |
|---|---|
| Host OS | Windows 11 Pro |
| CPU / RAM | Intel i7-10700 (8C/16T), 32 GB |
| Docker | Docker Desktop 29.9, Compose v5.7, 16 vCPU / 15.5 GB allocated to the VM |
| Host Python | 3.13 (system) + 3.11 venv via `uv` for tooling |
| Runtime Python | **3.11** in all containers and CI |
| Java | 17 in containers (Spark 3.5 supported JDK) |

Native Spark on Windows needs `winutils.exe`, and PySpark 3.5 does not support
Python 3.13. All Spark execution therefore happens in containers (and in
Linux CI). Pure-Python modules are tested on the host.

## Version matrix (pinned, mutually compatible)

Pins follow the official Airflow 2.10.5 constraints for Python 3.11, so a
single `requirements.txt` works in the Spark, Airflow and dashboard images.

| Component | Version |
|---|---|
| Apache Spark / PySpark | 3.5.4 (Scala 2.12) |
| Delta Lake (delta-spark) | 3.3.0 |
| delta-rs (`deltalake`) | 0.24.0 |
| Confluent Platform (Kafka KRaft, Schema Registry) | 7.8.0 |
| confluent-kafka-python / fastavro | 2.8.0 / 1.10.0 |
| Apache Airflow | 2.10.5 (LocalExecutor) |
| PostgreSQL | 16 |
| Object storage | SeaweedFS 3.85 (MinIO community builds withdrawn, ADR 0011) |
| Prometheus / Grafana | v3.1.0 / 11.4.0 |
| Streamlit / Plotly / DuckDB | 1.41.1 / 5.24.1 / 1.1.3 |
| Terraform (AWS / Databricks providers) | >= 1.6 (~> 5.80 / ~> 1.60) |

## Phases

Legend: ✅ done · 🟡 partial (see limitations)

| # | Phase | Acceptance criteria | Status |
|---|---|---|---|
| 1 | Repository initialisation, dependencies, architecture docs | Repo layout, pinned deps, config loader, Mermaid diagrams, ADRs, plan | ✅ |
| 2 | Docker infrastructure, PostgreSQL schemas, object storage | Core services healthy; `ops`/`meta`/`analytics` schemas; buckets; Spark writes Delta to S3 and delta-rs reads it | ✅ |
| 3 | Synthetic data generation & business workflows | Deterministic, relationally consistent journeys; traffic patterns; consistency tests | ✅ |
| 4 | Kafka topics, event contracts, streaming producers | Topics from catalogue; Avro + Schema Registry (BACKWARD); idempotent producer, DLQ, rate control | ✅ |
| 5 | Batch ingestion & Bronze pipelines | Idempotent Bronze stream with checkpoint recovery; PostgreSQL / files / REST with watermarks, checksums, registry, quarantine | ✅ |
| 6 | Silver transformations | 23 Silver tables; dedup, MERGE, FX, masking, quarantine, sessions, late/out-of-order; idempotent | ✅ |
| 7 | Gold dimensional model & marts | 10 dims (SCD1/SCD2, unknown/inferred), 8 facts, 13 marts, double-count test | ✅ |
| 8 | Customer 360 & KPIs | Customer 360 with RFM / CLV / segments; KPI catalogue | ✅ |
| 9 | Airflow orchestration & backfills | 14 DAGs, back-off retries, timeouts, callbacks, SLAs, parameterised backfill; DAG tests | 🟡 see Phase 9 note |
| 10 | Data quality, reliability, failure recovery | Rule framework, severities, gate, `audit.dq_results`; failure scenarios tested; runbooks | ✅ |
| 11 | Streamlit dashboards & Power BI integration | 10 pages on live Gold data; Power BI guide, DAX, semantic model | ✅ |
| 12 | Prometheus & Grafana monitoring | Exporters, Pushgateway metrics, 6 provisioned dashboards, 20 alert rules | ✅ |
| 13 | Automated testing & GitHub Actions | Unit / Spark / DQ / integration / e2e suites, coverage, CI workflows | ✅ (CI not yet executed on GitHub) |
| 14 | Performance benchmarking | Load runner for 100k / 1M / 10M; measured results only | ✅ see `benchmarks/README.md` |
| 15 | AWS Terraform & Databricks | Modules (S3, IAM, KMS, CloudWatch, UC), dev/prod, Databricks bundle + jobs | ✅ static validation only; nothing applied |
| 16 | Final documentation, screenshots, review | README with real screenshots and measured benchmarks | ✅ |

## Progress log

### Phase 1 - Repository initialisation ✅
Layout, pinned dependencies (aligned with the Airflow 2.10.5 / py3.11 constraints), environment-aware config, JSON
logging with correlation ids, Mermaid architecture docs, ADR 0001-0011.

### Phase 2 - Infrastructure ✅
Docker Compose with profiles (core, processing, orchestration, dashboard, monitoring, cdc, tools), health checks and
volumes. Findings and fixes:
- MinIO community images and binaries were withdrawn (HTTP 410), so SeaweedFS replaces it (ADR 0011).
- A host HTTP proxy intercepted localhost traffic, so internal hosts are listed in `NO_PROXY`.
- A native PostgreSQL 18 occupies port 5432, so the platform's PostgreSQL publishes on host port 5433.
- Ports 8080 and 3000 were taken, so Airflow uses 8088 and Grafana 3001.

Smoke test: Spark wrote a Delta table to S3 and delta-rs read it back.

### Phase 3 - Data generator ✅
A stateful, deterministic journey simulator (about 47k events/s): browse, cart, checkout, payment (with retries and
failures), stock reservation, multi-warehouse shipments, delivery and delays, returns, refunds and replenishment.
It models Black Friday, weekends and promotions, and injects defects. 22 consistency tests. The seed covers 120
days of history (29,442 orders) and 13,091 customers.

### Phase 4 - Kafka ✅
10 topics and 9 Avro subjects (BACKWARD compatibility), contract validation, an idempotent `acks=all` producer and a
dead-letter queue. The catch-up published 336,413 events, and 86 contract violations went to the DLQ, matching the
injected defects.

### Phase 5 - Bronze & batch ingestion ✅
The Bronze stream wrote 336,327 rows = 336,413 − 86, an exact match. All 16 batch sources loaded. Finding: monthly
exports containing an all-null column broke the Delta schema merge. The loader now re-types all-null columns, and
the affected table was rebuilt with the quarantine requeue tool. Local streaming checkpoints moved to a Docker
volume because of S3A directory semantics on SeaweedFS.

### Phase 6 - Silver ✅
23 tables, order-independent lifecycle folding, row-level quarantine with reasons, FX forward-fill, PII masking and
sessionisation. Finding: "late arrival" was first measured against ingestion time, which flagged the whole
catch-up. It was redefined relative to the stream position and now flags about 0.3% of events, the injected late
rate. A full replay from Bronze reproduced identical row counts, and a second run is a no-op.

### Phases 7-8 - Gold, Customer 360, KPIs ✅
10 dimensions, 8 incremental watermark-driven facts and 13 marts. Findings fixed:
- An SCD2 change count was evaluated lazily after the MERGE (caught by a test).
- The RFM recency score was inverted.
- GMV excluded shipping, making it smaller than gross revenue; it was redefined.
- Refunds converted at the refund-date FX rate made 9 orders look over-refunded. They are now booked at the original
  order rate, and reconciliation reports 0 breach days.

An incremental re-run merges 0 rows.

### Phase 9 - Airflow 🟡
14 DAGs built on a shared factory: exponential back-off, timeouts, failure and SLA callbacks pushed to Pushgateway,
a Spark pool, Datasets for data-aware scheduling, and a parameterised backfill. Every task is a CLI command that was
executed directly during development. DAG import and settings tests run in CI. The local Airflow image build was
stopped by the agent session's host-memory guard, so the DAGs have not yet run inside a local scheduler.

### Phase 10 - Data quality & reliability ✅
63 rules across bronze, silver and gold, a promotion gate, `audit.dq_results` and a Markdown report; quality score
98.73 with one genuine warning. Recovery tools: Bronze replay, quarantined-file requeue and DLQ replay. Tests cover a
crash between write and commit; corrupt, malformed and duplicate files; invalid schemas; and schema evolution.

### Phase 11 - Dashboards ✅
10 Streamlit pages over DuckDB + delta-rs (27-110 ms queries), a validated dark palette, filters, export and refresh.
Real screenshots were captured with Playwright. Power BI: connection guide, semantic model and 40+ DAX measures; Gold
can be published to the PostgreSQL `analytics` schema.

### Phase 12 - Monitoring ✅
Prometheus config and 20 alert rules (validated with promtool), a statsd mapping, and 6 Grafana dashboards generated
as code and provisioned. The end-to-end metric path (job → Pushgateway → Prometheus → Grafana) was verified.

### Phase 13 - Tests & CI ✅
46 unit, Spark and DQ tests plus 9 integration and e2e tests pass. Ruff, mypy (66 files) and sqlfluff are clean. CI
workflows cover lint, types, SQL, unit tests with coverage, DAGs, Terraform, Docker builds and security, plus a
Compose-based integration workflow.

### Phase 14 - Benchmarks ✅
`benchmarks/run.py` runs each configuration on isolated topics and an isolated lakehouse root. Results, hardware and
analysis are in `benchmarks/README.md`.

### Phase 15 - Cloud ✅ (not applied)
Terraform modules (kms, s3_lakehouse, iam, monitoring, databricks_uc) with dev and prod environments; `terraform fmt`
clean. A Databricks Asset Bundle with a continuous Bronze job, a medallion batch job with the DQ gate, and a
maintenance job; SASL_SSL credentials come from a secret scope.

## Known limitations

- Git: this agent session cannot write `.git/index` in this folder (`unable to write new index file`, even outside
  the sandbox), so commits must be created by the user.
- SeaweedFS + S3A: streaming checkpoints live on a local volume, and Delta's periodic log checkpoints log a warning
  locally.
- Single broker and single storage node locally; one writer per Delta table (enforced with an Airflow pool).
- Legacy history covers orders, items and payments only.
- Inventory positions drift when movements are lost to the DLQ (flagged by a DQ warning).
- Airflow DAGs have not been executed inside a local scheduler (see Phase 9).
- Terraform and Databricks are validated statically only; the Debezium CDC profile is configured but not covered by
  the automated tests.
