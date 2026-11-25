# Business requirements

## Context
An international e-commerce company sells about 10,000 products across 30 countries and 21 currencies,
through four channels: web, mobile app, marketplace partners and social commerce. Data sits in silos:
the storefront emits behavioural events, the order system and payment gateway emit transactions, the
warehouse management system tracks stock, and partners deliver files. Reporting is slow, inconsistent
(every team computes "revenue" differently) and blind to what is happening right now.

## Objectives

| # | Objective | Measure of success | Delivered by |
|---|---|---|---|
| O1 | One trusted definition of every business KPI | KPI catalogue implemented once (Gold marts); Streamlit and Power BI read the same tables | `docs/kpi-definitions.md`, `data_models/marts` |
| O2 | Near-real-time operational visibility | Orders and payment failures visible within minutes | Kafka → Bronze stream, `gold.rt_*` aggregates, live dashboard panel |
| O3 | Complete, reconciled historical reporting | Daily reconciliation of orders vs payments vs refunds within ±1 USD | `financial_reconciliation` DAG |
| O4 | Customer understanding | Customer 360, RFM segments, CLV, cohort retention | `mart_customer_360`, `mart_customer_retention`, `mart_customer_lifetime_value` |
| O5 | Inventory and fulfilment control | Stock-outs, low stock, replenishment quantities, on-time delivery, SLA breaches | `mart_inventory_health`, `mart_order_fulfillment` |
| O6 | Trustworthy data | Automated quality checks; broken data never reaches business tables | DQ framework + promotion gate |
| O7 | Scalable and cost-aware | Design target 10M events/day; runs locally for free; cloud optional | Benchmarks, `docs/cost-analysis.md` |
| O8 | Privacy by design | No raw PII outside restricted layers | Silver masking, Gold / BI without PII |

## Stakeholders and questions

| Stakeholder | Key questions | Pages / marts |
|---|---|---|
| Executive team | How are revenue, orders, AOV and conversion trending vs the previous period? | Executive Overview · `mart_executive_summary` |
| Finance | What is net revenue after refunds? Do captures reconcile with orders? | Sales & Revenue, Payments & Refunds · `financial_reconciliation` |
| Marketing / CRM | Which channels acquire customers profitably (ROAS, CAC)? Who is at risk of churning? | Customer Analytics, Customer 360 · `mart_marketing_performance` |
| E-commerce / product | Where do shoppers drop out of the funnel? Which products convert? | Conversion Funnel, Product Performance |
| Operations / supply chain | What is out of stock? What needs replenishing? Are carriers on time? | Inventory Management, Order Fulfillment |
| Data platform team | Is data fresh, complete and valid? | Data Quality & Pipeline Health, Grafana |

## Functional requirements

1. Ingest storefront, order, payment, inventory, shipment, refund, return and customer events in real time (Kafka).
2. Ingest master data and partner feeds in batch: PostgreSQL (incremental and full), CSV / JSON / Parquet files, and REST APIs (FX rates, marketing spend).
3. Keep raw data immutable and replayable (Bronze), then cleaned and conformed data (Silver), then business-ready models (Gold).
4. Convert every amount to USD with daily FX. Keep the local-currency values.
5. Handle duplicates, late and out-of-order events, schema evolution, corrupt records and missing reference data.
6. Keep history of changing customer and product attributes (SCD2).
7. Provide dashboards with filters for date range, country, channel and category, plus CSV export and auto-refresh.
8. Orchestrate batch workloads with retries, timeouts, SLAs and parameterised backfills.
9. Monitor throughput, lag, latency, freshness, failures and quality, with alerting.

## Non-functional requirements

| Area | Requirement |
|---|---|
| Volume | Design target 10M events/day (≈116 events/s average, ~1,200 events/s peak). Measured results in `benchmarks/README.md` |
| Latency | Operational view < 1 minute (Bronze); Silver every 15 min; Gold hourly / on Silver change |
| Reliability | At-least-once delivery with idempotent processing; no duplicates in business tables; recover from any job crash without manual repair |
| Data quality | Critical checks block promotion; quality score ≥ 95 |
| Security | Secrets only in environment / secret managers; encryption at rest and in transit (cloud); least-privilege IAM; PII masked |
| Portability | Same code locally (Docker) and on Databricks |
| Cost | Local run free; cloud cost documented and budget-alarmed |

## Out of scope
Real payment-card data (synthetic only), ML-based demand forecasting, and multi-region active-active deployment.
These are listed as future improvements in the README.
