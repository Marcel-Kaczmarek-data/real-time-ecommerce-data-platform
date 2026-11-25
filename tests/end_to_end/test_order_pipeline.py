"""End-to-end: generate an order -> publish to Kafka -> Bronze -> Silver -> Gold -> verify business metrics.

Uses the same CLIs as production (subprocesses), against the running services:
    docker compose run --rm platform pytest tests/end_to_end -m e2e
"""

from __future__ import annotations

import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.integration]


def _run(*args: str) -> None:
    subprocess.run([sys.executable, "-m", *args], check=True, timeout=3600)


def _events(order_id: str, customer_id: str, product_id: str):  # type: ignore[no-untyped-def]
    from data_generator.events import Event

    t0 = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=5)
    corr = str(uuid.uuid4())
    order = {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_timestamp": t0,
        "sales_channel": "web",
        "currency": "EUR",
        "subtotal_amount": 200.0,
        "discount_amount": 20.0,
        "shipping_amount": 0.0,
        "tax_amount": 34.2,
        "total_amount": 214.2,
        "promotion_id": None,
        "country": "DE",
        "session_id": None,
        "item_count": 2,
    }

    def ev(entity: str, event_type: str, ts: datetime, payload: dict) -> Event:  # type: ignore[type-arg]
        return Event(
            entity=entity,
            event_type=event_type,
            event_id=str(uuid.uuid4()),
            event_timestamp=ts,
            correlation_id=corr,
            payload=payload,
            emit_at=ts,
        )

    pay = {
        "payment_id": f"PAY-{order_id}",
        "order_id": order_id,
        "payment_method": "paypal",
        "payment_amount": 214.2,
        "currency": "EUR",
        "attempt_number": 1,
        "failure_reason": None,
        "provider": "paypal",
    }
    events = [
        ev("orders", "order_created", t0, {**order, "order_status": "created", "status_reason": None}),
        ev(
            "order_items",
            "order_item_added",
            t0,
            {
                "order_item_id": f"OI-{order_id}",
                "order_id": order_id,
                "product_id": product_id,
                "quantity": 2,
                "unit_price": 100.0,
                "discount_amount": 20.0,
                "currency": "EUR",
                "line_number": 1,
            },
        ),
        ev(
            "payments",
            "payment_authorized",
            t0 + timedelta(seconds=5),
            {**pay, "payment_status": "authorized", "payment_timestamp": t0 + timedelta(seconds=5)},
        ),
        ev(
            "payments",
            "payment_captured",
            t0 + timedelta(seconds=8),
            {**pay, "payment_status": "captured", "payment_timestamp": t0 + timedelta(seconds=5)},
        ),
        ev(
            "orders",
            "order_confirmed",
            t0 + timedelta(seconds=10),
            {**order, "order_status": "confirmed", "status_reason": None},
        ),
    ]
    # Deliberate duplicate delivery of the capture event (same event_id) - must not double count.
    events.append(events[3])
    return events


@pytest.mark.timeout(5400)
def test_order_flows_from_kafka_to_gold_metrics() -> None:
    import duckdb
    from deltalake import DeltaTable

    from ingestion.kafka_producers.producer import EventPublisher
    from pipelines.common.storage import delta_rs_storage_options

    order_id = f"ORD-E2E-{uuid.uuid4().hex[:10].upper()}"
    publisher = EventPublisher()
    for event in _events(order_id, "CUST-0000001", "PRD-000001"):
        assert publisher.publish(event)
    assert publisher.flush(60) == 0

    _run("pipelines.bronze.stream_kafka_to_bronze", "--once", "--no-realtime")
    _run("pipelines.silver.run", "--job", "orders", "--job", "order_items", "--job", "payments")
    _run(
        "pipelines.gold.run",
        "--stage",
        "facts",
        "--fact",
        "fact_orders",
        "--fact",
        "fact_order_items",
        "--fact",
        "fact_payments",
    )

    opts = delta_rs_storage_options()
    con = duckdb.connect()

    def load(path: str, name: str) -> None:
        con.register(name, DeltaTable(f"s3://lakehouse/{path}", storage_options=opts).to_pyarrow_dataset())

    load("bronze/payments_events", "bronze_payments")
    load("silver/orders", "silver_orders")
    load("gold/fact_orders", "fact_orders")
    load("gold/fact_order_items", "fact_order_items")
    bronze_dups = con.sql(
        f"SELECT count(*) FROM bronze_payments WHERE raw_payload LIKE '%{order_id}%'"
    ).fetchone()[0]
    assert bronze_dups == 3, "Bronze keeps the duplicate delivery (2 payment events + 1 replay)"
    so = con.sql(
        f"SELECT order_status, confirmed_at IS NOT NULL, fx_rate_to_usd FROM silver_orders "
        f"WHERE order_id = '{order_id}'"
    ).fetchone()
    assert so is not None and so[0] == "confirmed" and so[1]
    fx = so[2]
    fo = con.sql(
        f"SELECT is_paid, payment_attempts, captured_amount_usd, gross_revenue_usd, net_revenue_usd "
        f"FROM fact_orders WHERE order_id = '{order_id}'"
    ).fetchall()
    assert len(fo) == 1, "exactly one fact row per order"
    is_paid, attempts, captured_usd, gross_usd, net_usd = fo[0]
    assert is_paid and attempts == 1, "duplicate capture event must not create a second attempt"
    expected_gross = round((200.0 - 20.0 + 0.0) / fx, 2)
    assert gross_usd == pytest.approx(expected_gross, abs=0.02)
    assert net_usd == pytest.approx(expected_gross, abs=0.02)  # no refunds
    assert captured_usd == pytest.approx(214.2 / fx, abs=0.02)
    line = con.sql(
        f"SELECT line_net_amount_usd FROM fact_order_items WHERE order_id = '{order_id}'"
    ).fetchone()
    assert line is not None and line[0] == pytest.approx(180.0 / fx, abs=0.02)
