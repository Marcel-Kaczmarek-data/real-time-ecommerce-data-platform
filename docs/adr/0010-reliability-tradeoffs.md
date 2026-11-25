# ADR 0010 — Reliability trade-offs

**Status:** Accepted

## Decision
Provide **at-least-once delivery with idempotent, deduplicating processing**.
Do not claim end-to-end exactly-once.

* Producers use `enable.idempotence=true` and `acks=all`. This removes
  duplicates caused by internal retries, but not application-level replays.
* Kafka → Bronze: checkpointed offsets plus Delta transaction identifiers
  (`txnAppId`, `txnVersion = batchId`). A micro-batch that runs again after a
  crash is skipped by Delta.
* Bronze → Silver → Gold: MERGE on `event_id` and on business keys.

## Why not Kafka transactions end to end?
The sink is Delta, not Kafka. Kafka's transactional guarantees stop at the
consumer, and Spark's Kafka source does not take part in them. Delta's
idempotent writes achieve the same effect for this path at a fraction of the
complexity. Bronze deliberately keeps duplicates as audit evidence of producer behaviour.

## Trade-offs accepted
* Bronze may contain duplicate events (by design). Business tables may not.
* The real-time aggregate is approximate for events later than the 10-minute watermark.
* One writer per Delta table at a time on S3 (OSS log store). Concurrent
  writers are serialised by orchestration with an Airflow pool.
