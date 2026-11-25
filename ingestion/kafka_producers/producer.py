"""Reliable, contract-validated event publishing.

Guarantees and behaviour
------------------------
* ``enable.idempotence=true`` + ``acks=all``: broker-side de-duplication of
  producer retries and no acknowledged-message loss with ISR >= min.insync.replicas.
* Per-key ordering: idempotent producers keep ordering with up to 5 in-flight
  requests per connection.
* Client-side contract validation (fastavro) BEFORE serialisation: invalid
  events never reach business topics; they go to ``ecommerce.dead-letter``
  together with the validation error.
* Delivery failures after librdkafka's internal retries (``delivery.timeout.ms``)
  are also routed to the dead-letter topic.
* Schema ids come from Schema Registry (``auto.register.schemas=false``):
  producers cannot publish an unregistered contract.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastavro import parse_schema
from fastavro.validation import ValidationError, validate
from prometheus_client import Counter, Gauge, Histogram

from data_generator.events import DEAD_LETTER_TOPIC, TOPICS, Event
from data_generator.sinks import to_json
from pipelines.common.config import Config, load_config
from pipelines.common.logs import get_logger
from schemas.contracts import avro_schema

log = get_logger(__name__)

EVENTS_PRODUCED = Counter(
    "ecom_producer_events_total", "Events acknowledged by Kafka", ["topic", "event_type"]
)
EVENTS_FAILED = Counter("ecom_producer_delivery_failures_total", "Events that failed delivery", ["topic"])
DEAD_LETTERS = Counter(
    "ecom_dead_letter_events_total", "Events routed to the dead-letter topic", ["stage", "topic"]
)
PRODUCE_LATENCY = Histogram(
    "ecom_producer_ack_latency_seconds",
    "Time from produce() to broker acknowledgement",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
EVENT_LAG = Gauge(
    "ecom_producer_event_time_lag_seconds", "Wall clock minus event time of the last published event"
)
SIM_CLOCK = Gauge("ecom_producer_simulated_clock_timestamp_seconds", "Emit time of the last published event")


class ContractValidator:
    def __init__(self) -> None:
        self._schemas: dict[str, Any] = {}

    def schema(self, entity: str) -> Any:
        if entity not in self._schemas:
            self._schemas[entity] = parse_schema(avro_schema(entity))
        return self._schemas[entity]

    def validate(self, event: Event) -> str | None:
        """Return None when valid, otherwise a human-readable error."""
        try:
            validate(event.envelope(), self.schema(event.entity), raise_errors=True)
        except ValidationError as exc:
            return "; ".join(str(e) for e in exc.errors)[:2000]
        except (TypeError, ValueError, KeyError) as exc:
            return f"{type(exc).__name__}: {exc}"
        return None


def producer_config(cfg: Config) -> dict[str, Any]:
    conf: dict[str, Any] = {
        "bootstrap.servers": cfg.get_path("kafka.bootstrap_servers"),
        "client.id": "ecom-event-producer",
    }
    conf.update(cfg.get_path("kafka.producer") or {})
    if cfg.get_path("kafka.security_protocol") not in (None, "PLAINTEXT"):
        conf["security.protocol"] = cfg.get_path("kafka.security_protocol")
        conf["sasl.mechanisms"] = cfg.get_path("kafka.sasl_mechanism")
        conf["sasl.username"] = os.environ["KAFKA_API_KEY"]
        conf["sasl.password"] = os.environ["KAFKA_API_SECRET"]
    return conf


class EventPublisher:
    def __init__(
        self,
        cfg: Config | None = None,
        on_ack: Callable[[Event], None] | None = None,
        record_latencies: bool = False,
    ) -> None:
        self.cfg = cfg or load_config()
        self.producer = Producer(producer_config(self.cfg))
        self.registry = SchemaRegistryClient({"url": self.cfg.get_path("kafka.schema_registry_url")})
        self.validator = ContractValidator()
        self._serializers: dict[str, AvroSerializer] = {}
        self.on_ack = on_ack
        self.sent = 0
        self.acked = 0
        self.dead_lettered = 0
        self.latencies: list[float] | None = [] if record_latencies else None
        self.bytes_sent = 0

    def _serializer(self, entity: str) -> AvroSerializer:
        if entity not in self._serializers:
            self._serializers[entity] = AvroSerializer(
                self.registry,
                json.dumps(avro_schema(entity)),
                conf={"auto.register.schemas": False, "use.latest.version": False},
            )
        return self._serializers[entity]

    # ------------------------------------------------------------ publish --
    def publish(self, event: Event) -> bool:
        error = self.validator.validate(event)
        if error:
            self.dead_letter(event, "producer_validation", error)
            return False
        topic = TOPICS[event.entity]
        value = self._serializer(event.entity)(
            event.envelope(), SerializationContext(topic, MessageField.VALUE)
        )
        headers = [
            ("event_type", event.event_type.encode()),
            ("correlation_id", event.correlation_id.encode()),
            ("content-type", b"application/vnd.confluent.avro"),
        ]
        sent_at = time.perf_counter()

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                EVENTS_FAILED.labels(topic).inc()
                log.error(
                    "delivery failed", extra={"topic": topic, "error": str(err), "event_id": event.event_id}
                )
                self.dead_letter(event, "delivery", str(err), from_callback=True)
                return
            self.acked += 1
            elapsed = time.perf_counter() - sent_at
            PRODUCE_LATENCY.observe(elapsed)
            if self.latencies is not None:
                self.latencies.append(elapsed)
            EVENTS_PRODUCED.labels(topic, event.event_type).inc()
            if self.on_ack:
                self.on_ack(event)

        while True:
            try:
                self.producer.produce(
                    topic,
                    key=event.key.encode(),
                    value=value,
                    headers=headers,
                    timestamp=int(time.time() * 1000),
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                # Local queue full: serve delivery callbacks and retry (back-pressure).
                self.producer.poll(0.5)
        self.sent += 1
        self.bytes_sent += len(value)
        self.producer.poll(0)
        now = datetime.now(UTC)
        EVENT_LAG.set(max(0.0, (now - event.event_timestamp).total_seconds()))
        SIM_CLOCK.set((event.emit_at or event.event_timestamp).timestamp())
        return True

    def dead_letter(self, event: Event, stage: str, error: str, from_callback: bool = False) -> None:
        topic = TOPICS.get(event.entity, "unknown")
        record = {
            "failed_at": datetime.now(UTC).isoformat(),
            "stage": stage,
            "error": error,
            "original_topic": topic,
            "key": event.key,
            "event": json.loads(to_json(event.envelope())),
        }
        for _attempt in range(120):
            try:
                self.producer.produce(
                    DEAD_LETTER_TOPIC,
                    key=event.event_id.encode(),
                    value=json.dumps(record).encode(),
                    headers=[("stage", stage.encode()), ("original_topic", topic.encode())],
                )
                break
            except BufferError:
                if from_callback:
                    # poll() must not be re-entered from a delivery callback: record the loss loudly.
                    log.error("dead-letter queue full inside callback", extra={"event_id": event.event_id})
                    DEAD_LETTERS.labels("dlq_dropped", topic).inc()
                    return
                self.producer.poll(0.5)  # back-pressure: serve deliveries until the local queue drains
        self.dead_lettered += 1
        DEAD_LETTERS.labels(stage, topic).inc()

    def flush(self, timeout: float = 60.0) -> int:
        remaining = int(self.producer.flush(timeout))
        if remaining:
            log.warning("messages still in flight after flush", extra={"remaining": remaining})
        return remaining
