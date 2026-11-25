"""Kafka + Schema Registry + PostgreSQL + object storage integration tests."""

from __future__ import annotations

import copy
import json
import time
import uuid
from datetime import UTC, datetime

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture()
def test_topics(monkeypatch: pytest.MonkeyPatch):  # type: ignore[no-untyped-def]
    """Route the 'orders' entity to an isolated, freshly created test topic."""
    from confluent_kafka.admin import AdminClient, NewTopic

    from data_generator import events as ev

    topic = f"test.orders.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": "kafka:29092"})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic].result()
    monkeypatch.setitem(ev.TOPICS, "orders", topic)
    yield topic
    admin.delete_topics([topic])


def _order_event():  # type: ignore[no-untyped-def]
    from data_generator.events import Event

    now = datetime.now(UTC).replace(microsecond=0)
    return Event(
        entity="orders",
        event_type="order_created",
        event_id=str(uuid.uuid4()),
        event_timestamp=now,
        correlation_id=str(uuid.uuid4()),
        emit_at=now,
        payload={
            "order_id": f"ORD-IT-{uuid.uuid4().hex[:8]}",
            "customer_id": "CUST-0000001",
            "order_timestamp": now,
            "order_status": "created",
            "status_reason": None,
            "sales_channel": "web",
            "currency": "EUR",
            "subtotal_amount": 100.0,
            "discount_amount": 0.0,
            "shipping_amount": 4.99,
            "tax_amount": 19.95,
            "total_amount": 124.94,
            "promotion_id": None,
            "country": "DE",
            "session_id": None,
            "item_count": 1,
        },
    )


def test_produce_consume_avro_roundtrip_with_schema_registry(test_topics: str) -> None:
    from confluent_kafka import Consumer

    from ingestion.kafka_producers.producer import EventPublisher
    from ingestion.kafka_producers.schema_registry import ensure_schema, registry_client
    from pipelines.bronze.avro_decoder import TOPIC_ENTITY, decode_message

    ensure_schema(registry_client("http://schema-registry:8081"), "orders")
    publisher = EventPublisher()
    event = _order_event()
    assert publisher.publish(event)
    assert publisher.flush(30) == 0
    assert publisher.acked == 1

    consumer = Consumer(
        {
            "bootstrap.servers": "kafka:29092",
            "group.id": f"it-{uuid.uuid4().hex[:6]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([test_topics])
    msg = None
    deadline = time.time() + 30
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            break
    consumer.close()
    assert msg is not None and msg.error() is None
    assert msg.key().decode() == event.payload["order_id"]  # partition key = order_id
    TOPIC_ENTITY[test_topics] = "orders"
    decoded = decode_message(msg.value(), test_topics)
    assert decoded["decode_error"] is None
    assert decoded["event_id"] == event.event_id
    assert json.loads(decoded["raw_payload"])["total_amount"] == 124.94


def test_invalid_event_goes_to_dead_letter(test_topics: str) -> None:
    from ingestion.kafka_producers.producer import EventPublisher

    publisher = EventPublisher()
    bad = _order_event()
    bad = copy.deepcopy(bad)
    bad.payload.pop("customer_id")
    assert publisher.publish(bad) is False
    assert publisher.flush(30) == 0
    assert publisher.dead_lettered == 1


def test_schema_registry_rejects_incompatible_change() -> None:
    from ingestion.kafka_producers.schema_registry import (
        IncompatibleSchemaError,
        ensure_schema,
        registry_client,
    )
    from schemas.contracts import avro_schema

    client = registry_client("http://schema-registry:8081")
    ensure_schema(client, "orders")  # current contract is registered / compatible
    # Adding an OPTIONAL field with a default is BACKWARD compatible...
    evolved = avro_schema("orders")
    payload = evolved["fields"][-1]["type"]["fields"]
    payload.append({"name": "gift_wrap", "type": ["null", "boolean"], "default": None})
    from confluent_kafka.schema_registry import Schema

    assert client.test_compatibility("ecommerce.orders-value", Schema(json.dumps(evolved), "AVRO"))
    # ...adding a REQUIRED field without default breaks consumers reading old data.
    broken = avro_schema("orders")
    broken["fields"][-1]["type"]["fields"].append({"name": "loyalty_points", "type": "int"})
    with pytest.raises(IncompatibleSchemaError):
        ensure_schema(client, "orders", broken)


def test_watermark_two_phase_commit_and_file_registry() -> None:
    from ingestion.batch.registry import EPOCH, Registry
    from pipelines.common.db import connect

    conn = connect()
    reg = Registry(conn)
    source = f"it_source_{uuid.uuid4().hex[:6]}"
    try:
        assert reg.get_watermark(source) == EPOCH
        reg.stage_watermark(source, "updated_at", datetime(2026, 1, 2, tzinfo=UTC))
        assert reg.get_watermark(source) == EPOCH  # not visible until the load commits
        reg.commit_watermark(source, rows=10)
        assert reg.get_watermark(source) == datetime(2026, 1, 2, tzinfo=UTC)
        first = reg.register_file(source, "a/file1.csv", "x" * 64, 10, "csv")
        reg.set_file_status(first.file_id, "LOADED", 10, 0)
        dup = reg.register_file(source, "b/file1_copy.csv", "x" * 64, 10, "csv")
        assert dup.status == "DUPLICATE"  # same content under a different key
        assert "a/file1.csv" in reg.known_keys(source)
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM meta.file_registry WHERE source_name = %s", (source,))
            cur.execute("DELETE FROM meta.ingestion_watermarks WHERE source_name = %s", (source,))
        conn.commit()
        conn.close()


def test_object_storage_roundtrip() -> None:
    from pipelines.common.storage import s3_client

    s3 = s3_client()
    key = f"_it/{uuid.uuid4().hex}.txt"
    s3.put_object(Bucket="landing", Key=key, Body=b"hello")
    assert s3.get_object(Bucket="landing", Key=key)["Body"].read() == b"hello"
    s3.delete_object(Bucket="landing", Key=key)


def test_reference_api_requires_key_and_serves_rates() -> None:
    import requests

    base = "http://reference-api:8000"
    assert requests.get(f"{base}/v1/fx-rates", params={"date": "2026-11-01"}, timeout=5).status_code == 401
    resp = requests.get(
        f"{base}/v1/fx-rates",
        params={"date": "2026-11-01"},
        headers={"X-API-Key": "local-dev-key"},
        timeout=5,
    )
    assert resp.status_code == 200 and resp.json()["rates"]["USD"] == 1.0
