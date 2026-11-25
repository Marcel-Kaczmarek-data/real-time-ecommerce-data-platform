# ADR 0001 — Apache Kafka for event ingestion

**Status:** Accepted

## Context
Storefront, payment, warehouse and clickstream systems emit around 10M
events/day (design target), with Black Friday bursts of 5–10×. Several
consumers need the same events: lakehouse ingestion, a real-time aggregate,
and fraud scoring in the future. Events must be replayable after consumer bugs.

## Decision
Use **Apache Kafka** (KRaft mode, Confluent Platform 7.8 images) with the
**Confluent Schema Registry** (Avro, `BACKWARD` compatibility) as the event backbone.

## Consequences
* A durable, replayable log decouples producers from consumers. Each consumer
  group keeps its own offsets.
* Per-key ordering through partition keys (`order_id`, `product_id`, `session_id`).
* Contracts are enforced at publish time. Incompatible schema changes fail at
  registration, not in production consumers.
* Operational cost: brokers, partitions and retention need management. Managed
  options (MSK, Confluent Cloud) are documented for the cloud deployment.

## Alternatives considered
* **Amazon Kinesis**: less to operate, but AWS-only, with no local equivalent
  of the same fidelity, and shard-based scaling is less flexible.
* **RabbitMQ / SQS**: queues rather than logs. Messages cannot be replayed, and consumers compete for them.
* **Direct writes to object storage**: no ordering and no real-time fan-out; produces too many small files.
