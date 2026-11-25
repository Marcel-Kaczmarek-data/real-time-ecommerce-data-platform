"""Contract validation, dead-letter routing and schema artefact drift (no Kafka needed)."""

from __future__ import annotations

import copy
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from fastavro import parse_schema, schemaless_reader, schemaless_writer

from data_generator.events import TOPICS, Event
from data_generator.settings import get_settings
from data_generator.simulator import JourneySimulator
from data_generator.world import build_world
from ingestion.kafka_producers.producer import ContractValidator, EventPublisher
from ingestion.kafka_producers.run import RateLimiter
from schemas.contracts import EVENT_TYPES, PAYLOADS, avro_schema, render_all, spark_ddl

START = datetime(2026, 11, 2, tzinfo=UTC)


@pytest.fixture(scope="module")
def sample_events() -> dict[str, Event]:
    settings = get_settings("test", 21)
    sim = JourneySimulator(build_world(settings, START), START, inject_errors=False)
    out: dict[str, Event] = {}
    for e in sim.advance(START + timedelta(days=6)):
        out.setdefault(e.entity, e)
    # Every topic except possibly returns (needs weeks) should be represented.
    assert {"orders", "order_items", "payments", "clickstream", "inventory", "shipments"} <= set(out)
    return out


def test_committed_schema_files_match_contracts() -> None:
    for path, expected in render_all().items():
        assert path.exists(), f"missing generated schema {path} - run `python -m schemas.contracts`"
        assert path.read_text(encoding="utf-8") == expected, (
            f"{path} is stale - run `python -m schemas.contracts`"
        )


def test_every_entity_has_topic_event_types_and_parsable_avro() -> None:
    for entity in PAYLOADS:
        assert entity in TOPICS
        assert EVENT_TYPES[entity]
        parse_schema(avro_schema(entity))
        assert spark_ddl(entity).count(",") == len(PAYLOADS[entity]) - 1


def test_valid_events_pass_and_roundtrip_through_avro(sample_events: dict[str, Event]) -> None:
    validator = ContractValidator()
    for entity, event in sample_events.items():
        assert validator.validate(event) is None, entity
        schema = validator.schema(entity)
        import io

        buf = io.BytesIO()
        schemaless_writer(buf, schema, event.envelope())
        buf.seek(0)
        decoded: dict[str, Any] = schemaless_reader(buf, schema)  # type: ignore[assignment]
        assert decoded["event_id"] == event.event_id
        assert decoded["event_timestamp"] == event.event_timestamp.replace(
            microsecond=event.event_timestamp.microsecond // 1000 * 1000
        )


@pytest.mark.parametrize(
    ("mutation", "fragment"),
    [
        (lambda p: p.pop("order_id"), "order_id"),
        (lambda p: p.__setitem__("total_amount", "12.50"), "total_amount"),
        (lambda p: p.__setitem__("item_count", None), "item_count"),
    ],
)
def test_contract_violations_are_detected(
    sample_events: dict[str, Event], mutation: Any, fragment: str
) -> None:
    bad = copy.deepcopy(sample_events["orders"])
    mutation(bad.payload)
    error = ContractValidator().validate(bad)
    assert error is not None and fragment in error


class _FakeProducer:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.messages.append({"topic": topic, **kwargs})

    def poll(self, _timeout: float) -> int:
        return 0

    def flush(self, _timeout: float) -> int:
        return 0


def test_invalid_event_is_routed_to_dead_letter_topic(sample_events: dict[str, Event]) -> None:
    publisher = EventPublisher()
    fake = _FakeProducer()
    publisher.producer = fake  # type: ignore[assignment]
    bad = copy.deepcopy(sample_events["payments"])
    bad.payload.pop("payment_amount")
    assert publisher.publish(bad) is False
    assert publisher.dead_lettered == 1
    (msg,) = fake.messages
    assert msg["topic"] == "ecommerce.dead-letter"
    body = json.loads(msg["value"])
    assert body["stage"] == "producer_validation"
    assert body["original_topic"] == "ecommerce.payments"
    assert body["event"]["event_id"] == bad.event_id
    assert "payment_amount" in body["error"]


def test_rate_limiter_enforces_rate() -> None:
    limiter = RateLimiter(rate=200, burst=1)
    t0 = time.monotonic()
    for _ in range(50):
        limiter.acquire()
    elapsed = time.monotonic() - t0
    assert 0.2 <= elapsed < 1.5  # lower bound is the contract; upper bound tolerates coarse OS timers
    unlimited = RateLimiter(rate=0)
    t0 = time.monotonic()
    for _ in range(10_000):
        unlimited.acquire()
    assert time.monotonic() - t0 < 0.5


def test_partition_keys_follow_catalogue(sample_events: dict[str, Event]) -> None:
    from pipelines.common.config import topic_for_entity

    for entity, event in sample_events.items():
        key_field = topic_for_entity(entity)["key"]
        assert event.key == str(event.payload[key_field])
