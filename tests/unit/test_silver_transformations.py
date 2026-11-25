"""Silver transformation tests: dedup, out-of-order folding, idempotent MERGE,
late arrivals, validation, FX, masking and sessionisation."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest

pytestmark = pytest.mark.spark

T0 = datetime(2026, 11, 20, 10, 0, 0)


def _order_event(spark, event_id, status, ts, offset, total=110.0, partition=0, reason=None):  # type: ignore[no-untyped-def]
    payload = {
        "order_id": "ORD-1",
        "customer_id": "CUST-1",
        "order_timestamp": T0.isoformat(),
        "order_status": status,
        "status_reason": reason,
        "sales_channel": "web",
        "currency": "EUR",
        "subtotal_amount": 100.0,
        "discount_amount": 10.0,
        "shipping_amount": 0.0,
        "tax_amount": 20.0,
        "total_amount": total,
        "promotion_id": None,
        "country": "DE",
        "session_id": "S1",
        "item_count": 2,
    }
    return (
        event_id,
        f"order_{status}",
        1,
        ts,
        "corr",
        ts + timedelta(seconds=5),
        "ecommerce.orders",
        partition,
        offset,
        json.dumps(payload),
    )


BRONZE_COLS = (
    "event_id STRING, event_type STRING, event_version INT, event_timestamp TIMESTAMP, "
    "correlation_id STRING, ingestion_timestamp TIMESTAMP, kafka_topic STRING, kafka_partition INT, "
    "kafka_offset BIGINT, raw_payload STRING"
)


def test_fold_orders_is_order_independent_and_deduplicates(spark) -> None:  # type: ignore[no-untyped-def]
    from pipelines.silver import transforms as T

    events = [
        _order_event(spark, "e1", "created", T0, 1),
        _order_event(spark, "e2", "confirmed", T0 + timedelta(minutes=1), 2),
        _order_event(spark, "e3", "shipped", T0 + timedelta(hours=10), 3),
        _order_event(spark, "e3", "shipped", T0 + timedelta(hours=10), 4),  # duplicate delivery
    ]
    in_order = spark.createDataFrame(events, BRONZE_COLS)
    shuffled = spark.createDataFrame(list(reversed(events)), BRONZE_COLS)
    a = T.fold_orders(T.parse_events(in_order, "orders")).collect()[0]
    b = T.fold_orders(T.parse_events(shuffled, "orders")).collect()[0]
    assert a.asDict() == b.asDict()
    assert a.order_status == "shipped" and a.status_rank == 3
    assert a.event_count == 3  # duplicate removed
    assert a.confirmed_at == T0 + timedelta(minutes=1)
    assert a.delivered_at is None


def test_late_arrival_is_relative_to_stream_position(spark) -> None:  # type: ignore[no-untyped-def]
    from pipelines.silver import transforms as T

    events = [
        _order_event(spark, "e1", "created", T0, 1),
        _order_event(spark, "e2", "confirmed", T0 + timedelta(hours=5), 2),
        # Arrives after the partition already moved 5h ahead -> late.
        _order_event(spark, "e3", "created", T0 + timedelta(minutes=30), 3),
    ]
    parsed = T.parse_events(spark.createDataFrame(events, BRONZE_COLS), "orders")
    flags = {r.event_id: r.is_late_arrival for r in parsed.collect()}
    assert flags == {"e1": False, "e2": False, "e3": True}


def test_merge_is_idempotent_and_never_regresses_status(spark, lakehouse_root) -> None:  # type: ignore[no-untyped-def]
    from pipelines.common.delta_utils import merge_with_set
    from pipelines.common.tables import read_table, table
    from pipelines.silver import transforms as T

    ref = table("silver.test_orders_merge")
    merge_set = T.lifecycle_merge_set(
        "status_rank", "order_status", ["created_at", "confirmed_at", "shipped_at", "delivered_at"]
    )

    def apply(rows):  # type: ignore[no-untyped-def]
        folded = T.fold_orders(T.parse_events(spark.createDataFrame(rows, BRONZE_COLS), "orders"))
        folded = folded.select(
            "order_id",
            "status_rank",
            "order_status",
            "created_at",
            "confirmed_at",
            "shipped_at",
            "delivered_at",
            "last_event_at",
        )
        from pyspark.sql import functions as F

        merge_with_set(
            spark,
            folded.withColumn("_silver_updated_at", F.current_timestamp()),
            ref,
            ["order_id"],
            merge_set,
        )

    apply(
        [
            _order_event(spark, "e1", "created", T0, 1),
            _order_event(spark, "e4", "delivered", T0 + timedelta(days=2), 2),
        ]
    )
    # A late, out-of-order 'confirmed' arrives after 'delivered': fills the timestamp, keeps the status.
    apply([_order_event(spark, "e2", "confirmed", T0 + timedelta(minutes=2), 3)])
    first = read_table(spark, ref).collect()[0]
    # Replaying everything again changes nothing.
    apply(
        [
            _order_event(spark, "e1", "created", T0, 1),
            _order_event(spark, "e2", "confirmed", T0 + timedelta(minutes=2), 3),
            _order_event(spark, "e4", "delivered", T0 + timedelta(days=2), 2),
        ]
    )
    rows = read_table(spark, ref).collect()
    assert len(rows) == 1
    row = rows[0]
    assert row.order_status == "delivered"
    assert row.confirmed_at == T0 + timedelta(minutes=2)
    assert row.created_at == T0
    assert {k: v for k, v in row.asDict().items() if k != "_silver_updated_at"} == {
        k: v for k, v in first.asDict().items() if k != "_silver_updated_at"
    }


def test_validation_rules_reject_with_reasons(spark) -> None:  # type: ignore[no-untyped-def]
    from pipelines.silver import transforms as T

    events = [
        _order_event(spark, "ok", "created", T0, 1),
        _order_event(spark, "neg", "created", T0, 2, total=-5.0),
        _order_event(spark, "bad", "created", T0, 3, total=999.0),
    ]
    parsed = T.parse_events(spark.createDataFrame(events, BRONZE_COLS), "orders")
    checked = T.apply_rules(
        parsed,
        [
            ("non_negative_total", "total_amount >= 0"),
            (
                "total_reconciles",
                "abs(total_amount - (subtotal_amount - discount_amount + shipping_amount + tax_amount)) "
                "<= 0.05",
            ),
            ("null_is_invalid", "promotion_id IS NOT NULL OR true"),
        ],
    )
    good, bad = T.split_valid(checked)
    assert [r.event_id for r in good.collect()] == ["ok"]
    reasons = {r.event_id: sorted(r._reject_reasons) for r in bad.collect()}
    assert reasons == {"neg": ["non_negative_total", "total_reconciles"], "bad": ["total_reconciles"]}
    rejects = T.to_rejects(bad, "orders", "order_id", "kafka").collect()
    assert all(r.entity == "orders" and r.record_key == "ORD-1" and r.reasons for r in rejects)


def test_fx_daily_forward_and_back_fills(spark) -> None:  # type: ignore[no-untyped-def]
    from pipelines.silver import transforms as T

    rates = spark.createDataFrame(
        [(date(2026, 1, 2), "EUR", 0.9), (date(2026, 1, 5), "EUR", 0.95)],
        "rate_date DATE, currency_code STRING, rate_per_usd DOUBLE",
    )
    daily = {
        r.rate_date: (r.rate_per_usd, r.fx_rate_date)
        for r in T.fx_daily(rates, spark, until="2026-01-06").collect()
    }
    assert daily[date(2026, 1, 3)] == (0.9, date(2026, 1, 2))  # forward fill over a weekend
    assert daily[date(2026, 1, 6)] == (0.95, date(2026, 1, 5))
    orders = spark.createDataFrame(
        [("O1", datetime(2026, 1, 4, 12), "EUR", 90.0)],
        "order_id STRING, ts TIMESTAMP, currency STRING, amount DOUBLE",
    )
    usd = T.with_usd(
        orders, T.fx_daily(rates, spark, until="2026-01-06"), "ts", "currency", ["amount"]
    ).collect()[0]
    assert usd.amount_usd == pytest.approx(100.0)
    assert usd.fx_rate_date == date(2026, 1, 2)


def test_customer_pii_is_masked(spark) -> None:  # type: ignore[no-untyped-def]
    from pipelines.silver import transforms as T

    df = spark.createDataFrame(
        [("C1", "Anna", "Schmidt", "anna.schmidt.1@example.com")],
        "customer_id STRING, first_name STRING, last_name STRING, email STRING",
    )
    row = T.mask_customers(df, "salt").collect()[0]
    assert "email" not in row.asDict() and "last_name" not in row.asDict()
    assert row.email_masked == "a***@example.com"
    assert row.last_name_initial == "S."
    assert len(row.email_hash) == 64
    other_salt = T.mask_customers(df, "other").collect()[0].email_hash
    assert other_salt != row.email_hash


def test_session_reconstruction_splits_on_inactivity_and_stitches_identity(spark) -> None:  # type: ignore[no-untyped-def]
    from pipelines.silver import transforms as T

    cols = (
        "event_id STRING, event_type STRING, event_timestamp TIMESTAMP, customer_id STRING, anonymous_id STRING, "
        "session_id STRING, device_type STRING, browser STRING, traffic_source STRING, campaign_id STRING, "
        "country STRING, sales_channel STRING"
    )
    base = ("mobile", "Safari", "social", None, "DE", "web")
    clicks = spark.createDataFrame(
        [
            ("1", "page_view", T0, None, "A1", "S1", *base),
            ("2", "add_to_cart", T0 + timedelta(minutes=5), None, "A1", "S1", *base),
            ("3", "checkout_started", T0 + timedelta(minutes=8), "CUST-9", "A1", "S1", *base),  # logs in
            (
                "4",
                "page_view",
                T0 + timedelta(minutes=50),
                None,
                "A1",
                "S1",
                *base,
            ),  # > 30 min gap -> new session
        ],
        cols,
    )
    sessions = {r.session_key: r for r in T.reconstruct_sessions(clicks).collect()}
    assert set(sessions) == {"S1", "S1-2"}
    first = sessions["S1"]
    assert first.customer_id == "CUST-9" and first.is_identified
    assert first.event_count == 3 and first.reached_checkout and not first.converted
    assert first.is_cart_abandoned and first.session_duration_seconds == 480
    assert sessions["S1-2"].is_bounce
