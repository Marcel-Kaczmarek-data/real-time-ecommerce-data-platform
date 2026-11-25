# Benchmarks

All numbers below are **measured** by `benchmarks/run.py` on the machine described. None are estimated or
extrapolated. Raw results (JSON + Markdown per run) are in [`results/`](results/). Each configuration runs on
isolated Kafka topics (`bench<run>p<N>.*`), an isolated lakehouse root and its own checkpoints, so benchmarks never
touch the demo data.

```bash
make benchmark EVENTS=1000000                       # or:
docker compose run --rm platform python -m benchmarks.run --events 1000000 --partitions 3 --cores 4 \
    --live-seconds 120 --live-rate 500
```

## Environment

| Item | Value |
|---|---|
| Host | Intel Core i7-10700 (8 cores / 16 threads, 2.9 GHz), 32 GB RAM, Windows 11, SSD |
| Docker | Docker Desktop (WSL2 VM): 16 vCPU / 15.5 GB visible to containers |
| Software | Python 3.11.11 · PySpark 3.5.4 + Delta 3.3.0 (local mode) · Confluent Platform 7.8.0 Kafka (single broker, KRaft, RF=1) · SeaweedFS 3.85 (single node) · DuckDB 1.1.3 + deltalake 0.24 |
| Topology | Every service on the same machine (producer, broker, Spark, object storage, PostgreSQL, monitoring) |
| Producer | Python `confluent-kafka` 2.8, Avro + Schema Registry, `acks=all`, idempotent, zstd, `linger.ms=20` |
| Bronze | Structured Streaming `availableNow` drain (throughput) and 10 s `processingTime` trigger (latency) |

## Results

### Throughput

| Run | Events | Kafka partitions / topic | Producer events/s | Spark cores | Bronze rows/s | Drain time | CPU avg / peak (% of one core) | Peak RSS | Bronze size |
|---|---:|---:|---:|---:|---:|---:|---|---:|---:|
| [1M](results/20261125T021929_1000000.md) | 1,000,000 | 3 | **6,655** | 4 | **4,943** | 202 s | 250 / 1,282 | 4.2 GB | 300.6 MB |
| [100k](results/20261125T014924_100000.md) | 100,000 | 3 | 5,597 | 2 | 1,677 | 60 s | 281 / 625 | 2.6 GB | 23.9 MB |
| [100k](results/20261125T014924_100000.md) | 100,000 | 3 | 5,597 | 4 | 1,571 | 64 s | 315 / 853 | 3.4 GB | 23.9 MB |
| [100k isolated](results/20261125T015533_100000.md) | 100,000 | 12 | 1,021 | 2 | 783 | 128 s | 173 / 619 | 3.0 GB | 23.5 MB |
| [100k isolated](results/20261125T015533_100000.md) | 100,000 | 12 | 1,021 | 4 | 1,017 | 98 s | 229 / 734 | 3.6 GB | 23.5 MB |

* Event generator (pure Python simulation): **42,000–53,500 events/s**.
* Wire size: 258.5 MB of Avro for 1M events ≈ **0.27 KB/event**. Bronze stores ≈ **0.30 KB/event**: raw bytes
  plus decoded JSON payload plus Kafka coordinates, zstd Parquet.
* Drain times **include Spark/JVM start-up (~15 s)**, which dominates the 100k runs. That is why 100k shows
  ~1.6k rows/s while 1M shows ~4.9k rows/s.
* 1M run integrity: 999,768 events acknowledged plus 232 contract violations dead-lettered = 1,000,000.
* An earlier 12-partition 100k run is **excluded**: integration tests ran concurrently and caused producer timeouts.
  It is annotated in its result file and superseded by the isolated run above.

### End-to-end latency (live, steady rate)

The producer publishes at a fixed rate while the Bronze stream runs continuously. Latency is measured per row
as Bronze commit time minus Kafka append time.

| Rate | Duration | Rows committed | p50 | p95 | p99 | max | Producer ack p50 / p95 / p99 |
|---|---|---:|---:|---:|---:|---:|---|
| 500 events/s | 120 s | 59,780 / 60,000 | **18.8 s** | **43.8 s** | **47.9 s** | 49.1 s | 22 / 966 / 1,179 ms |
| 200 events/s* | 90 s | 16,894 / 18,000 | 14.5 s | 29.9 s | 31.9 s | 32.8 s | 25 / 5,328 / 17,211 ms |

\* From the first 100k run. Possible contention from documentation generation running at the same time.

Rows not committed when the stream was stopped are contract violations sent to the dead-letter topic, plus
events still in the final, uncommitted micro-batch.

### Delta OPTIMIZE (before / after)

| Table | Files before → after | Query median before → after | OPTIMIZE time |
|---|---|---|---|
| `bronze.clickstream_events` (1M run) | 15 → 1 | 21.6 → 21.5 ms | 54.4 s |
| `bronze.clickstream_events` (100k run) | 3 → 1 | 2.5 → 2.1 ms | 21.9 s |

### Gold serving queries (DuckDB over Delta via delta-rs, 5 runs each, warm)

| Query | median | p95 |
|---|---:|---:|
| Executive KPIs (30 days, `mart_daily_sales`) | 20–26 ms | 21–27 ms |
| Revenue by country | 17–20 ms | 18–21 ms |
| Top 10 products (scan of `fact_order_items`, 38k rows) | 20–22 ms | 22–25 ms |
| Customer 360 lookup (one customer) | 22–26 ms | 24–28 ms |
| Full scan of `fact_orders` (32k rows) by channel | 23–26 ms | 25–29 ms |

## Analysis

**Against the design target.** 10M events/day averages ≈ 116 events/s, with a Black Friday peak of about
1,200 events/s. On a single laptop with every service co-located, the measured sustained Bronze ingestion is
≈ 4,900 rows/s and the producer reaches ≈ 6,700 events/s. That is about **40× the daily average and about 4× the
assumed peak**, so the throughput side of the target is met with headroom. The **10M-event run itself was not
executed**: the 1M run took ≈ 6 minutes for publish + drain (≈ 10 minutes with all stages), and memory on the shared Docker VM was a constraint.
The README makes no 10M claim beyond the per-second rates measured here.

**Bottlenecks observed**
1. **Micro-batch overhead dominates latency.** Every micro-batch decodes Avro in Python (`mapInPandas`), writes 9
   per-topic Delta tables (9 commits) and computes latency percentiles. With a 10 s trigger, p50 latency is ≈ 15–19 s
   and p95 rises to 30–44 s, because batch duration exceeds the trigger interval at 500 events/s. Throughput is fine;
   latency is a fixed per-batch cost.
2. **Partition count vs batching on a single broker.** Going from 3 to 12 partitions per topic (120 partitions in
   total) cut producer throughput from ≈ 5,600 to ≈ 1,000 events/s. With `linger.ms=20`, per-partition batches stay
   tiny and `acks=all` requests multiply. More partitions only pay off with more brokers and consumers.
3. **Producer tail latency under flood.** When publishing as fast as possible, librdkafka's local queue fills and
   ack latency includes queueing (p95 in seconds). At a steady rate, the p50 ack is ≈ 22 ms. The dead-letter path now
   applies the same back-pressure; the first 1M attempt exposed a `BufferError` there, which was fixed.
4. **Small datasets make OPTIMIZE gains invisible.** File counts drop (15 → 1), but query time barely changes because
   the table fits in a few row groups. The benefit appears at scale, as fewer S3 requests and faster planning.
5. **Co-location.** Broker, Spark, object storage and the producer compete for the same 16 vCPU. Peak CPU reached ≈ 12.8
   cores during the 1M drain.

**Recommendations**
* Latency SLO below 10 s: write all topics to one partitioned Bronze table (one commit per batch), drop the per-batch
  percentile action (sample instead), and decode Avro on the JVM (`from_avro` with a registry-aware wrapper), or
  use Databricks `from_avro` with Schema Registry support.
* Use 3–6 partitions per topic on a single broker. Scale partitions with brokers and Spark executors in production
  (`partitions_full` in the topic catalogue).
* Producer: raise `linger.ms` (50–100 ms) and `batch.size` for bulk loads, keeping a low linger for interactive traffic.
* Keep the nightly `OPTIMIZE` + `VACUUM`. On Databricks, enable optimized writes and auto-compaction instead.
* Re-run on separate nodes (3 brokers, Spark cluster, real S3) before quoting production capacity.

## Minimum hardware (from these runs)

| Mode | Minimum | Recommended |
|---|---|---|
| Core demo (`make demo`, lite scale) | 4 vCPU, 8 GB RAM to Docker, 20 GB disk | 8 vCPU, 12 GB |
| Everything (incl. Airflow + monitoring + streaming) | 8 vCPU, 12 GB | 16 vCPU, 16 GB |
| 1M-event benchmark | 8 vCPU, 12 GB (peak Spark RSS 4.2 GB + broker + storage) | 16 vCPU, 16 GB |

The Airflow image build did not complete during development: the agent session's host-memory guard stopped it
while the other services were running. Start Airflow (`make airflow`) without the monitoring profile on machines
with less than 16 GB.
