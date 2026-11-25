# Disaster recovery

## Recovery objectives

| Asset | RPO | RTO | Mechanism |
|---|---|---|---|
| Kafka events in flight | 0 for acknowledged events (`acks=all`, ISR ≥ 2 in production) | minutes | Replicated topics; producer retries; streaming checkpoint resume |
| Bronze | 0 (Kafka retention 3–14 days covers any Bronze loss inside that window) | < 1 h | Re-ingest from Kafka offsets / landing files (kept 180 days) |
| Silver / Gold | 0 (fully derivable) | 1–3 h (local scale) | Replay from Bronze (`scripts/replay_bronze.py`), Gold full refresh |
| Bad write / logic bug | last good Delta version | minutes | Delta time travel + `RESTORE` |
| PostgreSQL (operational source) | per backup policy (e.g. 5 min PITR on RDS) | < 1 h | Managed backups / PITR (cloud); `seed` regenerates locally |
| Control plane (`meta.*` watermarks, file registry) | as PostgreSQL | < 1 h | Backups; worst case re-scan landing (duplicates are blocked by checksums) |
| Configuration & code | 0 | minutes | Git; infrastructure as code (Terraform, Databricks bundle) |

## Scenarios

### 1. A pipeline wrote wrong data to a Silver / Gold table
```sql
DESCRIBE HISTORY delta.`s3a://lakehouse/silver/orders`;          -- find the last good version
RESTORE TABLE delta.`s3a://lakehouse/silver/orders` TO VERSION AS OF 41;
```
Then fix the code and re-run the affected jobs. Downstream Gold facts: `python -m pipelines.gold.run --full-refresh
--fact fact_orders`. Time travel is bounded by `delta.deletedFileRetentionDuration` (7 days) and VACUUM retention (168 h).

### 2. Loss of a Silver or Gold table (accidental delete)
Silver and Gold are derived. Rebuild them:
```bash
python scripts/replay_bronze.py --all-streams --yes   # stream-fed Silver tables + checkpoints
python -m pipelines.silver.run                        # reference / master data
python -m pipelines.gold.run --full-refresh
```
The rebuild is deterministic. A replay in this project reproduced identical row counts (see `IMPLEMENTATION_PLAN.md` Phase 6).

### 3. Loss of Bronze
* Stream tables: reset the Bronze checkpoint and re-read from the earliest retained offsets (Kafka retention 3–14 days).
* Batch tables: delete the affected `meta.file_registry` rows; the loader re-reads the retained landing files.
* Older than retention: restore from S3 versioning (bucket versioning is on; noncurrent versions are kept for 30 days) or
  cross-region replication (production recommendation).

### 4. Streaming checkpoint corrupted
Restart with explicit `--starting-offsets` taken from the last good `offsets/<n>` file. Delta txn ids prevent
duplicates for already-committed batches. See the runbook.

### 5. Region / account loss (cloud)
Production recommendations (not provisioned by default, for cost reasons):
* S3 Cross-Region Replication of the lakehouse and landing buckets (KMS multi-region key).
* Terraform state in a separate account / region.
* Kafka: MSK multi-AZ; for a region outage, MirrorMaker 2 / Confluent Cluster Linking to a standby cluster.
* Databricks: redeploy the bundle in the DR workspace (`databricks bundle deploy -t dr`), pointing at the replicated bucket.

## DR test checklist (quarterly)
- [ ] Restore a Silver table to a previous version and validate DQ.
- [ ] Rebuild Silver + Gold from Bronze in a scratch root and compare row counts / revenue totals.
- [ ] Kill the streaming job mid-batch; verify no duplicates (`tests/integration/test_streaming_recovery.py`).
- [ ] Restore PostgreSQL from backup; verify watermarks resume correctly.
- [ ] Verify alert routing (fire a synthetic `PipelineFailed`).
