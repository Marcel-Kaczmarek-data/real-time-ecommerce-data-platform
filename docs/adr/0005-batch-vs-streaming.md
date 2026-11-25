# ADR 0005 — Batch versus streaming

**Status:** Accepted

## Context
The business needs operational KPIs within minutes (orders and revenue per
minute, payment failure spikes) and complete, reconciled numbers for daily
reporting. Most Gold logic (SCD2, cohorts, CLV, snapshots) is inherently batch.

## Decision
* **Streaming** from Kafka to Bronze (continuous, 10 s trigger) and for a small
  **real-time aggregate** (`gold.rt_sales_minute`, watermarked).
* **Incremental micro-batch** for Silver. Jobs read Bronze Delta tables
  incrementally (Delta streaming source + `foreachBatch` MERGE). Airflow runs
  them with `trigger(availableNow=True)`; the same code can also run
  continuously.
* **Scheduled batch** for Gold, DQ, Customer 360, snapshots and maintenance (Airflow).
* Airflow never hosts an unbounded streaming query. Continuous jobs run as
  dedicated services locally, or as Databricks continuous jobs in the cloud.

## Consequences
* Silver switches between "streaming" and "batch" with no code change. Cost
  and latency are tuned by the trigger.
* Gold freshness is bounded by the Airflow schedule. The real-time page uses the RT aggregate.
