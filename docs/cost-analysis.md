# Cost analysis

> **Nothing in this repository provisions paid resources automatically.** `terraform plan` is free;
> `terraform apply` and `databricks bundle deploy` are deliberate, manual steps.
> The figures below are **order-of-magnitude estimates** based on approximate public list prices
> (us-east-1 / eu-central-1, on-demand, without discounts). Prices change, so validate them with the
> [AWS Pricing Calculator](https://calculator.aws/) and your Databricks contract before deploying.

## Local deployment: free
Docker Compose on a workstation. The measured footprint is in `benchmarks/README.md`.

## Volume assumptions (design target)

| Item | Value |
|---|---|
| Events/day | 10M (avg ≈ 116 events/s, Black Friday peak ≈ 10×) |
| Avro event size | ≈ 0.5 KB (measured in the benchmark: payload MB / events) |
| Kafka ingress/day | ≈ 5 GB |
| Bronze/day (Delta + zstd Parquet, incl. raw bytes + JSON payload) | ≈ 2–3 GB |
| Silver + Gold/day | ≈ 1 GB |
| Lakehouse after 1 year (with 90-day Bronze retention) | ≈ 0.6–0.8 TB |

## Monthly estimate (production, one region)

| Component | Sizing | Approx. list price basis | Est. USD / month |
|---|---|---|---|
| S3 storage | ~0.7 TB Standard / Intelligent-Tiering | ~$0.023 per GB-month | 15–20 |
| S3 requests | streaming commits + reads | per 1k PUT/GET | 10–30 |
| KMS | 1 CMK + bucket keys | $1/key + requests | 2–5 |
| CloudWatch | 6 log groups, ~10 GB logs, alarms | ~$0.50/GB ingest | 10–20 |
| Kafka | Confluent Cloud Basic/Standard **or** MSK (3× `kafka.m7g.large`) | throughput + storage, or broker-hours | 150–700 |
| Databricks streaming | continuous job, 2× m6gd.large workers + driver | DBU (jobs compute) + EC2 | 400–700 |
| Databricks batch | medallion job every 30 min, autoscale 1–4 workers, ~6 h/day | DBU + EC2 | 300–600 |
| SQL Warehouse for Power BI | 2X-Small serverless, ~4 h/day, auto-stop | DBU | 150–400 |
| Power BI | Pro licences (per user) | per user / month | per seat |
| **Total (excl. BI licences)** | | | **≈ 1,000 – 2,500** |

The `prod` Terraform environment sets a monthly AWS budget of 1,500 USD with alerts at 80% (forecast) and 100%
(actual). This covers AWS only; DBUs are billed by Databricks.

### Dev environment
Buckets, KMS, alarms and the budget cost under 10 USD/month idle. Run Databricks jobs on demand (all jobs are
deployed **paused**) with small job clusters, and stay under about 50 USD/month (the dev budget).

## Cost levers
1. **Trigger interval:** Bronze is continuous. Silver can run with `availableNow` every 15–30 min instead of
   continuously, which roughly halves streaming compute. This is a configuration change only (ADR 0005).
2. **Spot instances:** the cluster policy enforces `SPOT_WITH_FALLBACK`. Batch jobs tolerate interruption because they are idempotent.
3. **Storage tiering:** landing files go to Infrequent Access after 30 days and expire after 180; Bronze moves to Intelligent-Tiering after 30 days and is deleted after 90.
4. **OPTIMIZE / VACUUM:** nightly compaction cuts request costs and query time (see the benchmark before/after).
5. **Serving:** pre-aggregated marts keep the BI warehouse small; Import mode avoids an always-on warehouse.
6. **Right-sizing:** the measured throughput (benchmarks) shows the 10M/day average fits a small cluster. Size for Black Friday with autoscaling, not permanently.

## Cleanup (avoid surprise bills)
```bash
databricks bundle destroy -t dev                 # jobs, uploaded code
cd infrastructure/terraform/environments/dev
terraform destroy                                # buckets (force_destroy=true in dev only), roles, KMS (7-day deletion window), alarms
```
Production buckets have `force_destroy = false` and versioning on. Empty them deliberately before destroying.
Check for leftovers: Cost Explorer filtered by tag `project = ecom-data-platform`, plus the Databricks account usage page.
