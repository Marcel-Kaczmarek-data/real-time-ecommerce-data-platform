# ADR 0006 — Partitioning strategy

**Status:** Accepted

## Kafka
| Topic | Key | Why |
|---|---|---|
| orders, order-items, payments, shipments, refunds, returns | `order_id` | All events of an order go to one partition, in order. Co-partitioning lets a future stateful consumer join them without repartitioning. |
| inventory | `product_id` | Stock deltas for a SKU stay ordered. |
| clickstream | `session_id` | Session events stay ordered and a session never spans partitions. `customer_id` was rejected: anonymous sessions have no customer, and heavy users would create hot partitions. |
| customers | `customer_id` | Profile changes stay ordered. |

Partition counts are 3–12 locally. The design target (10M/day ≈ 116 events/s
average, ~1,200 events/s at the Black Friday peak) needs few partitions for
raw throughput. Counts are chosen for **consumer parallelism** (Spark tasks
per micro-batch) and growth; see `partitions_full` in `config/kafka-topics.yaml`.
Partition counts can only grow. Growing them changes key→partition mapping,
so it is done during a quiet window.

## Delta
* Bronze event tables are partitioned by `processing_date` (ingestion date).
  Appends never touch old partitions, and retention drops whole partitions.
* Silver event-grain tables (clickstream) are partitioned by `event_date`.
* Silver entity tables (orders, customers, ...) are **not partitioned**. MERGE
  relies on file skipping on the key, helped by `OPTIMIZE ZORDER BY (key)`.
* Gold: `fact_clickstream` and `fact_inventory_snapshots` are partitioned by
  date. Other facts are unpartitioned (< 1 GB/year at target volume), because
  over-partitioning creates small files.
* Rule applied: partition only if each partition is ≥ ~1 GB, or queries and
  retention prune by that column.
