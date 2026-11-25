"""Business-consistency tests for the journey simulator (deterministic, fast)."""

from __future__ import annotations

import itertools
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

import pytest

from data_generator.events import Event
from data_generator.orders import price_order
from data_generator.reference import (
    COUNTRIES,
    COUNTRY_BY_CODE,
    black_friday,
    campaigns_for_date,
    demand_multiplier,
    fx_rate,
    promotions_for_year,
)
from data_generator.settings import ErrorInjection, get_settings
from data_generator.simulator import JourneySimulator, poisson
from data_generator.world import build_world

START = datetime(2026, 11, 20, tzinfo=UTC)  # spans Black Friday 2026-11-27


def _run(
    days: int = 3, seed: int = 11, chunks: int = 1, errors: bool = False
) -> tuple[list[Event], JourneySimulator]:
    settings = get_settings("test", seed)
    if errors:
        from dataclasses import replace

        settings = replace(settings, errors=ErrorInjection(0.01, 0.01, 0.02, 0.01))
    world = build_world(settings, START)
    sim = JourneySimulator(world, START, inject_errors=errors)
    end = START + timedelta(days=days)
    events: list[Event] = []
    step = (end - START) / chunks
    for i in range(1, chunks + 1):
        events.extend(sim.advance(START + step * i))
    return events, sim


@pytest.fixture(scope="module")
def clean_run() -> tuple[list[Event], JourneySimulator]:
    return _run(days=8)


def test_reference_scale_matches_spec_counts() -> None:
    assert len(COUNTRIES) == 30
    full = get_settings("full", 1)
    assert (full.customers, full.products, full.categories, full.suppliers, full.warehouses) == (
        100_000,
        10_000,
        50,
        500,
        20,
    )


def test_simulation_is_deterministic_and_chunk_independent() -> None:
    a, _ = _run(days=2, seed=5)
    b, _ = _run(days=2, seed=5, chunks=7)
    assert [e.event_id for e in a] == [e.event_id for e in b]
    c, _ = _run(days=2, seed=6)
    assert [e.event_id for e in a] != [e.event_id for e in c]


def test_events_are_emitted_in_time_order(clean_run: tuple[list[Event], JourneySimulator]) -> None:
    events, _ = clean_run
    emits = [e.emit_at for e in events]
    assert emits == sorted(emits)
    assert all(e.event_timestamp.tzinfo is not None for e in events)


def test_referential_integrity(clean_run: tuple[list[Event], JourneySimulator]) -> None:
    events, sim = clean_run
    world = sim.world
    orders = {e.payload["order_id"]: e for e in events if e.event_type == "order_created"}
    assert orders, "simulation should create orders"
    for e in events:
        p = e.payload
        if e.entity == "orders":
            assert p["customer_id"] in world.customer_by_id
            assert p["order_id"] in orders
        elif e.entity == "order_items":
            assert p["order_id"] in orders
            assert p["product_id"] in world.product_by_id
            assert p["quantity"] > 0
        elif e.entity in {"payments", "shipments", "refunds", "returns"}:
            assert p["order_id"] in orders
        elif e.entity == "clickstream" and p["product_id"]:
            assert p["product_id"] in world.product_by_id
        elif e.entity == "inventory":
            assert (p["warehouse_id"], p["product_id"]) in world.inventory.levels


def test_customers_registered_before_their_events(clean_run: tuple[list[Event], JourneySimulator]) -> None:
    events, sim = clean_run
    registered = {
        e.payload["customer_id"]: e.event_timestamp for e in events if e.event_type == "customer_registered"
    }
    for e in events:
        if e.event_type == "order_created" and e.payload["customer_id"] in registered:
            assert registered[e.payload["customer_id"]] <= e.event_timestamp


def test_order_amount_identities_and_payment_reconciliation(
    clean_run: tuple[list[Event], JourneySimulator],
) -> None:
    events, _ = clean_run
    items = defaultdict(list)
    for e in events:
        if e.entity == "order_items":
            items[e.payload["order_id"]].append(e.payload)
    captured = {e.payload["order_id"]: e.payload for e in events if e.event_type == "payment_captured"}
    for e in events:
        if e.event_type != "order_created":
            continue
        o = e.payload
        assert o["total_amount"] == pytest.approx(
            o["subtotal_amount"] - o["discount_amount"] + o["shipping_amount"] + o["tax_amount"], abs=0.011
        )
        lines = items[o["order_id"]]
        assert o["subtotal_amount"] == pytest.approx(
            sum(li["quantity"] * li["unit_price"] for li in lines), abs=0.011
        )
        assert o["discount_amount"] == pytest.approx(sum(li["discount_amount"] for li in lines), abs=0.011)
        if o["order_id"] in captured:
            assert captured[o["order_id"]]["payment_amount"] == pytest.approx(o["total_amount"])


def test_order_lifecycle_is_consistent(clean_run: tuple[list[Event], JourneySimulator]) -> None:
    events, _ = clean_run
    by_order: dict[str, list[str]] = defaultdict(list)
    for e in events:
        if e.entity == "orders":
            by_order[e.payload["order_id"]].append(e.payload["order_status"])
    allowed_next = {
        "created": {"confirmed", "cancelled"},
        "confirmed": {"shipped", "cancelled"},
        "shipped": {"delivered"},
        "delivered": {"returned"},
        "cancelled": set(),
        "returned": set(),
    }
    for order_id, statuses in by_order.items():
        assert statuses[0] == "created", order_id
        for prev, nxt in itertools.pairwise(statuses):
            assert nxt in allowed_next[prev], (order_id, statuses)
    shipped = {e.payload["order_id"] for e in events if e.event_type == "shipment_in_transit"}
    confirmed = {o for o, s in by_order.items() if "confirmed" in s}
    assert shipped <= confirmed


def test_failed_payments_cancel_orders_and_release_stock(
    clean_run: tuple[list[Event], JourneySimulator],
) -> None:
    events, _ = clean_run
    cancelled_for_payment = {
        e.payload["order_id"]
        for e in events
        if e.event_type == "order_cancelled" and e.payload["status_reason"] == "payment_failed"
    }
    captured = {e.payload["order_id"] for e in events if e.event_type == "payment_captured"}
    released = {e.payload["reference_id"] for e in events if e.event_type == "inventory_released"}
    assert not (cancelled_for_payment & captured)
    assert cancelled_for_payment <= released


def test_inventory_never_negative_and_deltas_reconcile(
    clean_run: tuple[list[Event], JourneySimulator],
) -> None:
    events, sim = clean_run
    settings = get_settings("test", 11)
    opening = build_world(settings, START).inventory.levels
    delta: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    # State mutations for already-scheduled future events (e.g. shipments in 2 days) are
    # applied at generation time, so include events still waiting in the queue.
    pending = [s.item for s in sim._queue]
    for e in events + pending:
        if e.entity == "inventory":
            p = e.payload
            assert p["available_quantity"] >= 0
            assert p["reserved_quantity"] >= 0
            key = (p["warehouse_id"], p["product_id"])
            delta[key][0] += p["quantity_delta_available"]
            delta[key][1] += p["quantity_delta_reserved"]
    for key, (d_avail, d_res) in delta.items():
        final = sim.world.inventory.levels[key]
        assert opening[key].available + d_avail == final.available
        assert opening[key].reserved + d_res == final.reserved


def test_funnel_rates_are_realistic(clean_run: tuple[list[Event], JourneySimulator]) -> None:
    _, sim = clean_run
    conv = sim.stats["orders"] / sim.stats["sessions"]
    assert 0.01 < conv < 0.08
    assert sim.stats["abandoned_carts"] > 0
    assert sim.stats["clickstream.add_to_cart"] > sim.stats["clickstream.checkout_started"] > 0


def test_black_friday_traffic_spike() -> None:
    bf = black_friday(2026)
    assert bf == date(2026, 11, 27)
    assert demand_multiplier(bf) > 3 * demand_multiplier(bf - timedelta(days=10))
    assert demand_multiplier(date(2026, 11, 21)) > demand_multiplier(date(2026, 11, 19))  # weekend > weekday


def test_injected_defects_are_flagged() -> None:
    events, sim = _run(days=2, errors=True)
    defects = {e.injected_defect for e in events if e.injected_defect}
    assert {"duplicate", "late", "out_of_order"} <= defects
    dup_ids = [e.event_id for e in events if e.injected_defect == "duplicate"]
    ids = [e.event_id for e in events]
    assert all(ids.count(d) >= 2 for d in dup_ids[:20])
    late = [e for e in events if e.injected_defect == "late"]
    assert all(e.emit_at - e.event_timestamp >= timedelta(hours=1) for e in late)


def test_synthetic_emails_use_reserved_domains(clean_run: tuple[list[Event], JourneySimulator]) -> None:
    _, sim = clean_run
    assert all(
        c.email.split("@")[1] in {"example.com", "example.org", "example.net"} for c in sim.world.customers
    )


def test_pricing_applies_promotion_tax_and_free_shipping() -> None:
    settings = get_settings("test", 3)
    world = build_world(settings, START)
    product = next(p for p in world.products if p.department == "Electronics" and p.unit_price > 100)
    wh = world.warehouses[0]
    bf = black_friday(2026)
    promos = [p for p in promotions_for_year(2026) if p.is_active(bf)]
    priced = price_order(
        [("OI-1", product, 2, wh.warehouse_id)],
        COUNTRY_BY_CODE["US"],
        {wh.warehouse_id: wh.country_code},
        "web",
        promos,
        bf,
    )
    assert priced.promotion_id == "PRM-2026-BFRIDAY"
    assert priced.discount_amount == pytest.approx(priced.subtotal_amount * 0.25, abs=0.02)
    assert priced.shipping_amount == 0.0  # above free-shipping threshold
    assert priced.tax_amount == pytest.approx(
        (priced.subtotal_amount - priced.discount_amount) * 0.07, abs=0.011
    )


def test_fx_rates_are_deterministic_and_plausible() -> None:
    d = date(2026, 6, 1)
    assert fx_rate("USD", d) == 1.0
    assert fx_rate("EUR", d) == fx_rate("EUR", d)
    assert 0.8 < fx_rate("EUR", d) < 1.05
    assert fx_rate("JPY", d) > 100
    assert fx_rate("AED", d) == pytest.approx(3.6725)


def test_campaign_spend_scales_with_demand() -> None:
    bf = black_friday(2026)
    spend_bf = sum(float(c["spend_usd"]) for c in campaigns_for_date(bf))  # type: ignore[arg-type]
    spend_normal = sum(float(c["spend_usd"]) for c in campaigns_for_date(bf - timedelta(days=12)))  # type: ignore[arg-type]
    assert spend_bf > 2 * spend_normal


def test_poisson_mean() -> None:
    import random

    rng = random.Random(1)
    for lam in (0.5, 8.0, 250.0):
        mean = sum(poisson(rng, lam) for _ in range(4000)) / 4000
        assert mean == pytest.approx(lam, rel=0.08)
