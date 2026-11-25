# Operational runbook

Every alert in `monitoring/prometheus/alerts.yml` links to a section here. Commands assume the
local Docker deployment; the Databricks equivalents are the jobs in `infrastructure/databricks`.

**General first steps:** open Grafana (`http://localhost:3001`) → the dashboard named in the alert;
check `docker compose ps`; read structured logs with `docker compose logs <service> | grep '"level": "ERROR"'`.
Every log line carries `pipeline` and `correlation_id` (Airflow run id / journey id) for tracing.

---

## High consumer lag
*Alert:* `HighConsumerLag` (Bronze stream > 50k offsets behind).

1. Check the *Spark streaming health* dashboard: is input rate > processed rate (sustained backlog), or did the query stop?
2. If the query is running but slow: check `ecom_stream_batch_duration_seconds`. Common causes are object-storage
   slowness and too few cores. Increase `SPARK_MASTER=local[N]` / cluster size, or raise
   `kafka.consumer.max_offsets_per_trigger` to drain in larger batches.
3. If the backlog comes from a replay or catch-up (producer `catchup` mode), no action is needed. The lag drains; watch the trend.
4. Retention guard: topics keep 3–14 days. If lag approaches retention, scale out immediately (data loss risk;
   `failOnDataLoss=true` in cloud config stops the stream rather than silently skipping).

## Streaming job down
*Alerts:* `StreamingQueryStalled`, `StreamingServiceDown`.

1. `docker compose --profile processing ps spark-streaming` → if restarting in a loop, read `docker compose logs spark-streaming`.
2. Restart: `docker compose --profile processing up -d spark-streaming`. Recovery is automatic: the query resumes from
   its checkpoint (`/opt/checkpoints/bronze_kafka_ingest`), and a micro-batch that was written but not committed is
   skipped by Delta (`txnAppId`/`txnVersion`). No duplicates (tested in `tests/integration/test_streaming_recovery.py`).
3. **Never delete the checkpoint** to "fix" a stream. That restarts from `startingOffsets` and re-ingests the topic
   (Bronze would contain duplicates; Silver would still deduplicate).
4. A corrupted checkpoint (rare): stop the stream, move the checkpoint aside, restart with
   `--starting-offsets` set to the JSON offsets from the last good `offsets/<n>` file.

## Dead-letter queue
*Alerts:* `DeadLetterSpike`, `ExcessiveCorruptRecords`.

1. Inspect messages:
   `docker exec ecom-kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic ecommerce.dead-letter --from-beginning --max-messages 5`
   Each record carries `stage` (`producer_validation`, `delivery`, `bronze_decode`), `error`, `original_topic` and the event.
2. `producer_validation`: an upstream system broke the contract. Fix the producer. The rejected events are
   not lost and can be corrected and republished.
3. `bronze_decode`: undecodable bytes (wrong serializer, unknown schema id). They are also stored in
   `quarantine.bronze_corrupt_events` with the raw bytes.
4. Spike after a deploy → roll back the producer, or register the missing schema version (`admin bootstrap`).

## Data-quality gate blocked
*Alert:* `CriticalDataQualityFailure`. The Gold DAGs fail with `DataQualityGateError`.

1. Read `data_quality/reports/latest.md` or the *Data Quality & Pipeline Health* page. The failing check names the
   table, rule and sample details.
2. Decide: **data problem** (bad source batch) or **rule problem** (a legitimate change in the data).
   * Data problem: fix at the source, or exclude the batch. Bronze is immutable, so re-run Silver after correcting
     the source (`scripts/replay_bronze.py --job <job> --yes` when Silver logic changed).
   * Rule problem: change the rule in `data_quality/rules/*.yaml` through a reviewed PR.
3. Gold keeps serving the last good state meanwhile. Communicate staleness to stakeholders.
4. `--skip-quality-gate` exists for incidents only. Using it requires an incident ticket reference.

## Stale data
*Alert:* `StaleData` (newest record older than 6 h).

1. Which table? Bronze stale → producer, Kafka or stream down. Silver stale → `silver_batch_processing` DAG. Gold
   stale → DQ gate blocked or a Gold DAG failing.
2. Check Airflow (`http://localhost:8088`) for failed or queued runs; check the `spark_jobs` pool (2 slots) for starvation.
3. FX freshness (`fx_rates_freshness`): the reference API is unavailable. The job retries with back-off; Silver
   forward-fills the last known rate, so revenue keeps flowing with the previous day's rate.

## Failed pipeline
*Alerts:* `PipelineFailed`, `AirflowTaskFailed`, `PipelineNotRunRecently`.

1. Airflow task log → structured JSON error with `correlation_id`.
2. All tasks are idempotent: **clear the task** after fixing the cause. Watermarks only advance after successful
   commits, so a re-run neither skips nor duplicates data.
3. Batch file failure: the file is moved to `landing/quarantine/...` and marked `FAILED` in `meta.file_registry`.
   After fixing it, run `python scripts/requeue_quarantined_files.py --source-prefix <prefix>`, then the ingestion DAG.
4. `FinancialReconciliationBreach`: inspect `gold.financial_reconciliation` breach days
   (`orders_without_single_capture`, `over_refunded_orders`, `difference_usd`) and escalate to finance data.

## Kafka broker down
*Alert:* `KafkaBrokerDown`.

1. `docker compose up -d kafka` (local) or the MSK / Confluent status page (cloud).
2. Producers buffer and retry for up to `delivery.timeout.ms` (120 s locally, 300 s cloud). Longer outages send
   events to the dead-letter path; they can be republished from the producer's deterministic timeline
   (`run catchup` resumes from its checkpoint).
3. The streaming query retries the connection and then fails. The container restarts and resumes from the checkpoint.

## Object storage unavailable
*Alerts:* `ObjectStorageDown`, CloudWatch `lakehouse-s3-5xx`.

Delta commits are atomic. A job that fails mid-write leaves no partial data (uncommitted files are ignored and
later removed by VACUUM). Restore the storage service, then clear the failed Airflow tasks. Verify with
`python scripts/lakehouse_status.py`.

## Historical backfill / replay
* REST sources for a date range: trigger the Airflow DAG `historical_backfill` with
  `{"start_date": "...", "end_date": "...", "replay_silver": false}`.
* Rebuild Silver after a logic fix: `{"replay_silver": true}` (drops the stream-fed Silver tables and checkpoints,
  reprocesses Bronze, fully refreshes the Gold facts). Equivalent CLI: `python scripts/replay_bronze.py --all-streams --yes`
  followed by `python -m pipelines.gold.run --full-refresh`.
* Point-in-time inspection: `SELECT * FROM delta.\`s3a://lakehouse/silver/orders\` VERSION AS OF <n>`.
  Roll back a bad write: `RESTORE TABLE delta.\`...\` TO VERSION AS OF <n>` (see `docs/disaster-recovery.md`).

## Routine operations

| Task | Frequency | How |
|---|---|---|
| Delta OPTIMIZE / VACUUM / Bronze retention | nightly | DAG `delta_table_maintenance` |
| Review DQ warnings | daily | Data Quality page / `latest.md` |
| Rotate local credentials | per environment | `.env` (local); Secrets Manager / Databricks secret scope (cloud) |
| Schema change | per change | update `schemas/contracts.py` → `python -m schemas.contracts` → CI compatibility check → deploy producers |
