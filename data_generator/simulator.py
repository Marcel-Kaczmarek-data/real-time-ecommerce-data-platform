"""Deterministic customer-journey simulator.

The simulator advances a clock in one-hour buckets. At the start of each
bucket it generates the sessions (and marketplace orders) that start inside the
bucket. Each session is simulated end to end: browsing, cart, checkout,
payment, reservation, fulfilment, delivery, returns and refunds. Every
resulting event is scheduled on a priority queue by *emit time*.
``advance(until)`` yields events in emit order.

Determinism: all randomness comes from one seeded ``random.Random``. State
mutations happen in bucket order, so the event sequence is identical no
matter how ``advance`` is chunked (catch-up, live tailing, restarts).
"""

from __future__ import annotations

import copy
import heapq
import itertools
import math
import random
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

from data_generator.clickstream import (
    SessionContext,
    choose_channel,
    choose_device_and_source,
    is_paid,
    page_url,
    session_steps,
    think_time_seconds,
)
from data_generator.customers import Customer, loyalty_for_spend
from data_generator.events import Event, IdFactory
from data_generator.orders import PricedOrder, price_order
from data_generator.payments import attempt_payment, choose_method
from data_generator.products import Product
from data_generator.reference import (
    CANCEL_REASONS,
    COUNTRIES,
    COUNTRY_BY_CODE,
    HOURLY_PROFILE,
    RETURN_REASONS,
    SEARCH_TERMS,
    black_friday,
    campaign_id,
    demand_multiplier,
    fx_rate,
    is_black_friday,
    round_money,
)
from data_generator.shipments import plan_shipment
from data_generator.world import World

HOUR = timedelta(hours=1)


@dataclass(order=True)
class _Scheduled:
    at: datetime
    seq: int
    item: Any = field(compare=False)


@dataclass
class SupplierDelivery:
    delivery_id: str
    supplier_id: str
    warehouse_id: str
    product_id: str
    quantity: int
    ordered_at: datetime
    expected_at: datetime
    delivered_at: datetime | None
    status: str

    def record(self) -> dict[str, Any]:
        return dict(self.__dict__)


def poisson(rng: random.Random, lam: float) -> int:
    """Poisson sample (Knuth for small lambda, normal approximation for large)."""
    if lam <= 0:
        return 0
    if lam > 60:
        return max(0, round(rng.gauss(lam, math.sqrt(lam))))
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


class JourneySimulator:
    def __init__(
        self,
        world: World,
        start: datetime,
        clickstream_from: datetime | None = None,
        sessions_per_day: int | None = None,
        inject_errors: bool = True,
    ) -> None:
        if start.tzinfo is None:
            raise ValueError("start must be timezone-aware (UTC)")
        self.world = world
        self.settings = world.settings
        self.rng = random.Random(self.settings.seed * 7919 + 17)
        self.ids = IdFactory(self.rng)
        self.clock = start.replace(minute=0, second=0, microsecond=0)
        self.clickstream_from = clickstream_from or self.clock
        self.sessions_per_day = sessions_per_day or self.settings.sessions_per_day
        self.inject_errors = inject_errors
        self._queue: list[_Scheduled] = []
        self._state_queue: list[_Scheduled] = []
        self._seq = itertools.count()
        self.stats: Counter[str] = Counter()
        self.supplier_deliveries: list[SupplierDelivery] = []
        self.customer_changes: list[Customer] = []  # customers registered / updated (for ops DB sync)
        self._country_share = {
            c.code: c.traffic_weight / sum(x.traffic_weight for x in COUNTRIES) for c in COUNTRIES
        }
        self._wh_country = {w.warehouse_id: w.country_code for w in world.warehouses}

    # ================================================================ API ==
    def advance(self, until: datetime) -> Iterator[Event]:
        """Yield all events with emit time < ``until`` in emit order."""
        while self.clock < until:
            yield from self._drain(self.clock)
            self._apply_state(self.clock + HOUR)
            self._generate_bucket(self.clock)
            self.clock += HOUR
        yield from self._drain(until)

    def pending_events(self) -> int:
        return len(self._queue)

    # ========================================================= scheduling ==
    def _drain(self, until: datetime) -> Iterator[Event]:
        while self._queue and self._queue[0].at < until:
            yield heapq.heappop(self._queue).item

    def _apply_state(self, until: datetime) -> None:
        while self._state_queue and self._state_queue[0].at < until:
            heapq.heappop(self._state_queue).item()

    def _defer_state(self, at: datetime, fn: Callable[[], None]) -> None:
        heapq.heappush(self._state_queue, _Scheduled(at, next(self._seq), fn))

    def _emit(
        self, entity: str, event_type: str, ts: datetime, payload: dict[str, Any], correlation_id: str
    ) -> None:
        event = Event(
            entity=entity,
            event_type=event_type,
            event_id=self.ids.uuid(),
            event_timestamp=ts,
            correlation_id=correlation_id,
            payload=payload,
            emit_at=ts,
        )
        errors = self.settings.errors
        if self.inject_errors:
            r = self.rng.random()
            if r < errors.late_rate:
                event.emit_at = ts + timedelta(seconds=self.rng.uniform(3600, 6 * 3600))
                event.injected_defect = "late"
            elif r < errors.late_rate + errors.out_of_order_rate:
                event.emit_at = ts + timedelta(seconds=self.rng.uniform(30, 600))
                event.injected_defect = "out_of_order"
            elif r < errors.late_rate + errors.out_of_order_rate + errors.invalid_rate:
                event = self._corrupt(event)
            if self.rng.random() < errors.duplicate_rate:
                dup = copy.deepcopy(event)
                dup.emit_at = (event.emit_at or ts) + timedelta(seconds=self.rng.uniform(1, 120))
                dup.injected_defect = "duplicate"
                self._push(dup)
                self.stats["defect_duplicate"] += 1
        if event.injected_defect:
            self.stats[f"defect_{event.injected_defect}"] += 1
        self.stats[f"{entity}.{event_type}"] += 1
        self._push(event)

    def _push(self, event: Event) -> None:
        heapq.heappush(
            self._queue, _Scheduled(event.emit_at or event.event_timestamp, next(self._seq), event)
        )

    def _corrupt(self, event: Event) -> Event:
        """Create a contract-violating (DLQ) or semantically invalid (quarantine) copy."""
        bad = copy.deepcopy(event)
        payload = bad.payload
        if self.rng.random() < 0.5:
            # Schema violation: drop a required field -> rejected by producer validation.
            required = [k for k, v in payload.items() if v is not None]
            payload.pop(self.rng.choice(required), None)
            bad.injected_defect = "invalid_schema"
        else:
            # Semantic violation: valid types, impossible values -> Silver quarantine.
            for key in ("quantity", "payment_amount", "total_amount", "refund_amount", "unit_price"):
                if isinstance(payload.get(key), int | float):
                    payload[key] = -abs(payload[key]) - 1
                    break
            else:
                if "currency" in payload:
                    payload["currency"] = "XXX"
                elif "country" in payload:
                    payload["country"] = "ZZ"
            bad.injected_defect = "invalid_value"
        return bad

    # ========================================================== buckets ===
    def _generate_bucket(self, bucket_start: datetime) -> None:
        rng = self.rng
        d = bucket_start.date()
        if bucket_start.hour == 0:
            self._daily_housekeeping(bucket_start)
        demand = demand_multiplier(d)
        starts: list[tuple[datetime, str]] = []
        for country in COUNTRIES:
            local_hour = int((bucket_start.hour + country.utc_offset) % 24)
            lam = (
                self.sessions_per_day
                * self._country_share[country.code]
                * demand
                * HOURLY_PROFILE[local_hour]
            )
            for _ in range(poisson(rng, lam)):
                starts.append((bucket_start + timedelta(seconds=rng.uniform(0, 3599.999)), country.code))
        starts.sort()
        marketplace_lam = (
            len(starts)
            * 0.032
            * self.settings.marketplace_order_share
            / (1 - self.settings.marketplace_order_share)
        )
        for _ in range(poisson(rng, marketplace_lam)):
            starts.append((bucket_start + timedelta(seconds=rng.uniform(0, 3599.999)), "__marketplace__"))
        for ts, country_code in starts:
            if country_code == "__marketplace__":
                self._marketplace_order(ts)
            else:
                self._session(ts, country_code)

    def _daily_housekeeping(self, day_start: datetime) -> None:
        """Customer profile changes and inventory cycle-count adjustments."""
        rng = self.rng
        n_updates = poisson(rng, len(self.world.customers) * self.settings.profile_update_prob_per_day)
        for _ in range(n_updates):
            customer = self.world.pick_returning_customer(rng)
            if customer is None:
                continue
            ts = day_start + timedelta(seconds=rng.uniform(0, 86_399))
            change = rng.random()
            if change < 0.45:
                customer.marketing_opt_in = not customer.marketing_opt_in
            elif change < 0.75:
                customer.customer_segment = rng.choice(["consumer", "small_business", "corporate"])
            elif change < 0.90:
                customer.email = customer.email.replace("@", f".{rng.randint(10, 99)}@", 1)
            else:
                customer.country = rng.choice(COUNTRIES).code  # relocation (SCD2 attribute)
            customer.updated_at = ts
            self._emit_customer(customer, "customer_profile_updated", ts, self.ids.uuid())
        # Cycle counts: shrinkage / found stock on a handful of SKUs.
        levels = self.world.inventory.levels
        keys = list(levels)
        for _ in range(min(len(keys), 3 + poisson(rng, 5))):
            wid, pid = rng.choice(keys)
            delta = rng.choice([-3, -2, -1, -1, -1, 1, 2])
            level = levels[(wid, pid)]
            if level.available + delta < 0:
                continue
            ts = day_start + timedelta(hours=rng.uniform(1, 5))
            self._inventory_move(
                "inventory_adjusted", "adjust", wid, pid, delta, 0, ts, f"CC-{ts:%Y%m%d}", self.ids.uuid()
            )

    # =========================================================== sessions ==
    def _session(self, t0: datetime, country_code: str) -> None:
        rng, world, st = self.rng, self.world, self.settings
        corr = self.ids.uuid()
        customer: Customer | None = None
        if rng.random() < st.returning_visitor_share:
            customer = world.pick_returning_customer(rng, country_code)
        device, browser, source = choose_device_and_source(
            rng, customer is not None, bool(customer and customer.marketing_opt_in)
        )
        ctx = SessionContext(
            session_id=self.ids.uuid(),
            anonymous_id=self.ids.uuid(),
            customer_id=customer.customer_id if customer else None,
            country=country_code,
            device_type=device,
            browser=browser,
            traffic_source=source,
            channel=choose_channel(rng, device, source),
            campaign_id=campaign_id(source, t0.date()) if is_paid(source) else None,
        )
        clicks = t0 >= self.clickstream_from
        self.stats["sessions"] += 1
        t = t0
        landing = rng.choice(
            ["home", "home", "category", "deals", "search_results"]
            if source != "paid_search"
            else ["product", "search_results"]
        )
        preferred = customer.category_affinity if customer else []
        cart: dict[str, int] = {}
        products_seen: dict[str, Product] = {}
        if clicks:
            self._click(ctx, "page_view", t, corr, page_type=landing)
        for _ in range(session_steps(rng)):
            t += timedelta(seconds=think_time_seconds(rng))
            r = rng.random()
            if r < 0.14:
                if clicks:
                    self._click(
                        ctx,
                        "search",
                        t,
                        corr,
                        page_type="search_results",
                        search_query=rng.choice(SEARCH_TERMS),
                    )
                continue
            if r < 0.32:
                if clicks:
                    self._click(
                        ctx, "page_view", t, corr, page_type=rng.choice(["category", "home", "deals"])
                    )
                continue
            dept = rng.choice(preferred) if preferred and rng.random() < 0.65 else None
            product = world.pick_product(rng, dept)
            products_seen[product.product_id] = product
            if clicks:
                self._click(ctx, "product_view", t, corr, page_type="product", product_id=product.product_id)
            in_stock = product.is_sellable and world.inventory.total_available(product.product_id) > 0
            if not in_stock:
                self.stats["out_of_stock_views"] += 1
                continue
            boost = 1.6 if product.department in preferred else 1.0
            if is_black_friday(t.date()):
                boost *= 1.5
            if rng.random() < st.add_to_cart_prob * boost:
                qty = 1 if rng.random() < 0.85 else rng.randint(2, 3)
                cart[product.product_id] = cart.get(product.product_id, 0) + qty
                t += timedelta(seconds=rng.uniform(2, 20))
                if clicks:
                    self._click(
                        ctx,
                        "add_to_cart",
                        t,
                        corr,
                        page_type="product",
                        product_id=product.product_id,
                        cart_quantity=qty,
                    )
            if cart and rng.random() < st.remove_from_cart_prob / 3:
                pid = rng.choice(sorted(cart))
                t += timedelta(seconds=rng.uniform(2, 30))
                if clicks:
                    self._click(
                        ctx,
                        "remove_from_cart",
                        t,
                        corr,
                        page_type="cart",
                        product_id=pid,
                        cart_quantity=cart[pid],
                    )
                del cart[pid]
        if not cart or rng.random() >= st.checkout_prob:
            if cart:
                self.stats["abandoned_carts"] += 1
            return
        if customer is None:
            # Guests must register (or sign in) to check out.
            if rng.random() >= 0.5:
                self.stats["abandoned_carts"] += 1
                return
            t += timedelta(seconds=rng.uniform(30, 120))
            customer = world.register_customer(t, country_code)
            self._emit_customer(customer, "customer_registered", t, corr)
            ctx.customer_id = customer.customer_id
        t += timedelta(seconds=rng.uniform(5, 40))
        if clicks:
            self._click(ctx, "checkout_started", t, corr, page_type="checkout")
        if rng.random() < st.checkout_abandon_prob:
            self.stats["abandoned_checkouts"] += 1
            return
        t += timedelta(seconds=rng.uniform(20, 180))
        lines = [(products_seen[pid], qty) for pid, qty in sorted(cart.items())]
        placed = self._place_order(t, customer, ctx.channel, lines, corr, ctx.session_id)
        if placed and clicks:
            self._click(ctx, "purchase", placed + timedelta(seconds=2), corr, page_type="checkout")

    def _marketplace_order(self, t: datetime) -> None:
        rng = self.rng
        customer = self.world.pick_returning_customer(rng)
        if customer is None:
            return
        n_lines = 1 if rng.random() < 0.7 else 2
        lines = [(self.world.pick_product(rng), 1) for _ in range(n_lines)]
        lines = [(p, q) for p, q in lines if p.is_sellable]
        if lines:
            self._place_order(t, customer, "marketplace", lines, self.ids.uuid(), None)

    # ============================================================= orders ==
    def _place_order(
        self,
        t: datetime,
        customer: Customer,
        channel: str,
        lines: list[tuple[Product, int]],
        corr: str,
        session_id: str | None,
    ) -> datetime | None:
        """Create the order and schedule its whole lifecycle. Returns confirmation time."""
        rng, world = self.rng, self.world
        country = COUNTRY_BY_CODE[customer.country]
        allocated: list[tuple[str, Product, int, str]] = []
        for product, qty in lines:
            wid = world.inventory.pick_warehouse(product.product_id, country.code, qty)
            if wid is None:
                self.stats["out_of_stock_at_checkout"] += 1
                continue
            allocated.append((self.ids.sequence("OI", 11), product, qty, wid))
        if not allocated:
            self.stats["orders_lost_out_of_stock"] += 1
            return None
        d = t.date()
        priced = price_order(allocated, country, self._wh_country, channel, world.active_promotions(d), d)
        order_id = self.ids.sequence("ORD", 10)
        self.stats["orders"] += 1
        base_order = {
            "order_id": order_id,
            "customer_id": customer.customer_id,
            "order_timestamp": t,
            "sales_channel": channel,
            "currency": priced.currency,
            "subtotal_amount": priced.subtotal_amount,
            "discount_amount": priced.discount_amount,
            "shipping_amount": priced.shipping_amount,
            "tax_amount": priced.tax_amount,
            "total_amount": priced.total_amount,
            "promotion_id": priced.promotion_id,
            "country": country.code,
            "session_id": session_id,
            "item_count": sum(pl.quantity for pl in priced.lines),
        }

        def order_event(event_type: str, status: str, ts: datetime, reason: str | None = None) -> None:
            self._emit(
                "orders",
                event_type,
                ts,
                {**base_order, "order_status": status, "status_reason": reason},
                corr,
            )

        order_event("order_created", "created", t)
        for n, pl in enumerate(priced.lines, start=1):
            self._emit(
                "order_items",
                "order_item_added",
                t,
                {
                    "order_item_id": pl.order_item_id,
                    "order_id": order_id,
                    "product_id": pl.product.product_id,
                    "quantity": pl.quantity,
                    "unit_price": pl.unit_price,
                    "discount_amount": pl.discount_amount,
                    "currency": priced.currency,
                    "line_number": n,
                },
                corr,
            )
        t_res = t + timedelta(seconds=1)
        for pl in priced.lines:
            self._inventory_move(
                "inventory_reserved",
                "reserve",
                pl.warehouse_id,
                pl.product.product_id,
                -pl.quantity,
                pl.quantity,
                t_res,
                order_id,
                corr,
            )

        # ---- payment
        is_new = customer.order_count == 0
        peak = is_black_friday(d)
        method = choose_method(rng, country.code)
        t_pay = t + timedelta(seconds=rng.uniform(2, 15))
        attempt = attempt_payment(rng, method, priced.total_usd, is_new, peak)
        payment_id = self.ids.sequence("PAY", 10)
        attempts = 1
        if not attempt.success:
            self._payment(
                payment_id,
                order_id,
                attempt.method,
                "failed",
                priced,
                t_pay,
                attempts,
                attempt.failure_reason,
                attempt.provider,
                corr,
            )
            if rng.random() < 0.45:
                attempts += 1
                t_pay += timedelta(seconds=rng.uniform(45, 400))
                method = choose_method(rng, country.code, exclude=attempt.method)
                attempt = attempt_payment(rng, method, priced.total_usd, is_new, peak)
                payment_id = self.ids.sequence("PAY", 10)
                if not attempt.success:
                    self._payment(
                        payment_id,
                        order_id,
                        attempt.method,
                        "failed",
                        priced,
                        t_pay,
                        attempts,
                        attempt.failure_reason,
                        attempt.provider,
                        corr,
                    )
        if not attempt.success:
            t_cancel = t_pay + timedelta(seconds=rng.uniform(5, 60))
            order_event("order_cancelled", "cancelled", t_cancel, "payment_failed")
            for pl in priced.lines:
                self._inventory_move(
                    "inventory_released",
                    "release",
                    pl.warehouse_id,
                    pl.product.product_id,
                    pl.quantity,
                    -pl.quantity,
                    t_cancel,
                    order_id,
                    corr,
                )
            self.stats["orders_payment_failed"] += 1
            return None
        self._payment(
            payment_id,
            order_id,
            attempt.method,
            "authorized",
            priced,
            t_pay,
            attempts,
            None,
            attempt.provider,
            corr,
        )
        t_capture = t_pay + timedelta(seconds=rng.uniform(1, 8))
        self._payment(
            payment_id,
            order_id,
            attempt.method,
            "captured",
            priced,
            t_capture,
            attempts,
            None,
            attempt.provider,
            corr,
        )
        t_confirm = t_capture + timedelta(seconds=rng.uniform(1, 5))
        order_event("order_confirmed", "confirmed", t_confirm)

        # ---- customer value & loyalty
        customer.order_count += 1
        customer.lifetime_spend_usd += priced.net_revenue_usd
        new_tier = loyalty_for_spend(customer.lifetime_spend_usd)
        if new_tier != customer.loyalty_status:
            customer.loyalty_status = new_tier
            customer.updated_at = t_confirm + timedelta(minutes=1)
            self._emit_customer(customer, "customer_profile_updated", customer.updated_at, corr)

        # ---- customer cancellation before shipment
        if rng.random() < self.settings.cancel_before_ship_prob:
            t_cx = t_confirm + timedelta(hours=rng.uniform(0.3, 6))
            order_event("order_cancelled", "cancelled", t_cx, rng.choice(CANCEL_REASONS))
            for pl in priced.lines:
                self._inventory_move(
                    "inventory_released",
                    "release",
                    pl.warehouse_id,
                    pl.product.product_id,
                    pl.quantity,
                    -pl.quantity,
                    t_cx,
                    order_id,
                    corr,
                )
            self._refund(
                order_id,
                payment_id,
                None,
                priced.total_amount,
                priced.tax_amount,
                priced.currency,
                "order_cancelled",
                t_cx + timedelta(hours=rng.uniform(1, 24)),
                corr,
            )
            customer.lifetime_spend_usd -= priced.net_revenue_usd
            self.stats["orders_cancelled_by_customer"] += 1
            return t_confirm

        self._fulfil(order_id, priced, country.code, channel, t_confirm, corr, order_event, payment_id)
        return t_confirm

    def _fulfil(
        self,
        order_id: str,
        priced: PricedOrder,
        dest: str,
        channel: str,
        t_confirm: datetime,
        corr: str,
        order_event: Callable[..., None],
        payment_id: str,
    ) -> None:
        rng = self.rng
        bf = black_friday(t_confirm.year)
        peak = bf - timedelta(days=2) <= t_confirm.date() <= bf + timedelta(days=10) or (
            t_confirm.month == 12 and t_confirm.day <= 22
        )
        groups: dict[str, list[Any]] = {}
        for pl in priced.lines:
            groups.setdefault(pl.warehouse_id, []).append(pl)
        deliveries: list[datetime] = []
        first_ship: datetime | None = None
        for wid, wlines in sorted(groups.items()):
            warehouse = self.world.inventory.warehouses[wid]
            plan = plan_shipment(
                rng, t_confirm, warehouse.region, warehouse.country_code != dest, channel, peak
            )
            shipment_id = self.ids.sequence("SHP", 10)
            base = {
                "shipment_id": shipment_id,
                "order_id": order_id,
                "warehouse_id": wid,
                "carrier": plan.carrier,
                "estimated_delivery": plan.estimated_delivery,
                "tracking_number": f"{plan.carrier[:3].upper()}{self.rng.randint(10**11, 10**12 - 1)}",
                "destination_country": dest,
            }
            self._emit(
                "shipments",
                "shipment_created",
                plan.label_created_at,
                {**base, "shipment_status": "created", "shipped_at": None, "actual_delivery": None},
                corr,
            )
            self._emit(
                "shipments",
                "shipment_in_transit",
                plan.shipped_at,
                {
                    **base,
                    "shipment_status": "in_transit",
                    "shipped_at": plan.shipped_at,
                    "actual_delivery": None,
                },
                corr,
            )
            for pl in wlines:
                self._inventory_move(
                    "inventory_shipped",
                    "ship",
                    wid,
                    pl.product.product_id,
                    0,
                    -pl.quantity,
                    plan.shipped_at,
                    order_id,
                    corr,
                )
            if plan.is_delayed:
                self._emit(
                    "shipments",
                    "shipment_delayed",
                    plan.estimated_delivery,
                    {
                        **base,
                        "shipment_status": "delayed",
                        "shipped_at": plan.shipped_at,
                        "actual_delivery": None,
                    },
                    corr,
                )
            self._emit(
                "shipments",
                "shipment_delivered",
                plan.actual_delivery,
                {
                    **base,
                    "shipment_status": "delivered",
                    "shipped_at": plan.shipped_at,
                    "actual_delivery": plan.actual_delivery,
                },
                corr,
            )
            first_ship = plan.shipped_at if first_ship is None else min(first_ship, plan.shipped_at)
            deliveries.append(plan.actual_delivery)
        if first_ship is None:  # pragma: no cover - every order has at least one line
            raise RuntimeError(f"order {order_id} has no shipments")
        order_event("order_shipped", "shipped", first_ship)
        delivered_at = max(deliveries)
        order_event("order_delivered", "delivered", delivered_at)
        self._maybe_return(order_id, priced, dest, delivered_at, corr, order_event, payment_id)

    def _maybe_return(
        self,
        order_id: str,
        priced: PricedOrder,
        dest: str,
        delivered_at: datetime,
        corr: str,
        order_event: Callable[..., None],
        payment_id: str,
    ) -> None:
        rng, world = self.rng, self.world
        vat = COUNTRY_BY_CODE[dest].vat_rate
        returned_lines = 0
        last_received = delivered_at
        for pl in priced.lines:
            rate = world.category_of(pl.product).return_rate
            if rng.random() >= rate:
                continue
            returned_lines += 1
            return_id = self.ids.sequence("RET", 10)
            reason = rng.choice(RETURN_REASONS)
            requested = delivered_at + timedelta(days=rng.uniform(1, 20))
            received = requested + timedelta(days=rng.uniform(3, 10))
            base = {
                "return_id": return_id,
                "order_id": order_id,
                "order_item_id": pl.order_item_id,
                "product_id": pl.product.product_id,
                "quantity": pl.quantity,
                "return_reason": reason,
                "requested_at": requested,
                "warehouse_id": pl.warehouse_id,
            }
            self._emit(
                "returns",
                "return_requested",
                requested,
                {**base, "return_status": "requested", "received_at": None},
                corr,
            )
            self._emit(
                "returns",
                "return_received",
                received,
                {**base, "return_status": "received", "received_at": received},
                corr,
            )
            if reason not in {"damaged", "defective"}:
                self._inventory_move(
                    "inventory_replenished",
                    "return_restock",
                    pl.warehouse_id,
                    pl.product.product_id,
                    pl.quantity,
                    0,
                    received,
                    return_id,
                    corr,
                )
            net = pl.net
            tax = round_money(net * vat, priced.currency)
            self._refund(
                order_id,
                payment_id,
                return_id,
                round_money(net + tax, priced.currency),
                tax,
                priced.currency,
                "product_return",
                received + timedelta(days=rng.uniform(0, 2)),
                corr,
            )
            last_received = max(last_received, received)
        if returned_lines and returned_lines == len(priced.lines):
            order_event("order_returned", "returned", last_received + timedelta(minutes=5))
        if returned_lines:
            self.stats["orders_with_returns"] += 1

    # ============================================================ helpers ==
    def _payment(
        self,
        payment_id: str,
        order_id: str,
        method: str,
        status: str,
        priced: PricedOrder,
        ts: datetime,
        attempt: int,
        reason: str | None,
        provider: str,
        corr: str,
    ) -> None:
        event_type = {
            "authorized": "payment_authorized",
            "captured": "payment_captured",
            "failed": "payment_failed",
        }[status]
        self._emit(
            "payments",
            event_type,
            ts,
            {
                "payment_id": payment_id,
                "order_id": order_id,
                "payment_method": method,
                "payment_status": status,
                "payment_amount": priced.total_amount,
                "currency": priced.currency,
                "payment_timestamp": ts,
                "attempt_number": attempt,
                "failure_reason": reason,
                "provider": provider,
            },
            corr,
        )

    def _refund(
        self,
        order_id: str,
        payment_id: str,
        return_id: str | None,
        amount: float,
        tax: float,
        currency: str,
        reason: str,
        ts: datetime,
        corr: str,
    ) -> None:
        self._emit(
            "refunds",
            "refund_issued",
            ts,
            {
                "refund_id": self.ids.sequence("RFD", 10),
                "order_id": order_id,
                "payment_id": payment_id,
                "return_id": return_id,
                "refund_amount": amount,
                "refund_tax_amount": tax,
                "currency": currency,
                "refund_reason": reason,
                "refund_status": "completed",
                "refund_timestamp": ts,
            },
            corr,
        )

    def _emit_customer(self, customer: Customer, event_type: str, ts: datetime, corr: str) -> None:
        self.customer_changes.append(customer)
        self._emit("customers", event_type, ts, customer.record(), corr)

    def _click(
        self,
        ctx: SessionContext,
        event_type: str,
        ts: datetime,
        corr: str,
        page_type: str,
        product_id: str | None = None,
        search_query: str | None = None,
        cart_quantity: int | None = None,
    ) -> None:
        self._emit(
            "clickstream",
            event_type,
            ts,
            {
                "customer_id": ctx.customer_id,
                "anonymous_id": ctx.anonymous_id,
                "session_id": ctx.session_id,
                "event_type": event_type,
                "product_id": product_id,
                "event_timestamp": ts,
                "device_type": ctx.device_type,
                "browser": ctx.browser,
                "traffic_source": ctx.traffic_source,
                "country": ctx.country,
                "page_url": page_url(page_type, product_id),
                "search_query": search_query,
                "cart_quantity": cart_quantity,
                "campaign_id": ctx.campaign_id,
                "sales_channel": ctx.channel,
            },
            corr,
        )

    def _inventory_move(
        self,
        event_type: str,
        movement: str,
        wid: str,
        pid: str,
        d_avail: int,
        d_res: int,
        ts: datetime,
        reference_id: str,
        corr: str,
    ) -> None:
        inv = self.world.inventory
        level = inv.apply(wid, pid, d_avail, d_res, ts)
        self._emit(
            "inventory",
            event_type,
            ts,
            {
                "movement_id": self.ids.sequence("MOV", 11),
                "warehouse_id": wid,
                "product_id": pid,
                "movement_type": movement,
                "quantity_delta_available": d_avail,
                "quantity_delta_reserved": d_res,
                "available_quantity": level.available,
                "reserved_quantity": level.reserved,
                "reorder_threshold": level.reorder_threshold,
                "reference_id": reference_id,
                "updated_at": ts,
            },
            corr,
        )
        if (
            movement == "reserve"
            and level.available < level.reorder_threshold
            and not level.replenishment_pending
        ):
            self._order_replenishment(wid, pid, ts)

    def _order_replenishment(self, wid: str, pid: str, ts: datetime) -> None:
        rng, world = self.rng, self.world
        level = world.inventory.levels[(wid, pid)]
        level.replenishment_pending = True
        product = world.product_by_id[pid]
        supplier = world.supplier_by_id[product.supplier_id]
        expected = ts + timedelta(days=supplier.lead_time_days)
        late = rng.random() > supplier.reliability_score
        delivered = expected + timedelta(days=rng.uniform(1, 6) if late else -rng.uniform(0, 1))
        delivery = SupplierDelivery(
            delivery_id=self.ids.sequence("DEL", 9),
            supplier_id=supplier.supplier_id,
            warehouse_id=wid,
            product_id=pid,
            quantity=level.reorder_quantity,
            ordered_at=ts,
            expected_at=expected,
            delivered_at=delivered,
            status="delivered",
        )
        self.supplier_deliveries.append(delivery)
        corr = self.ids.uuid()

        def receive() -> None:
            level.replenishment_pending = False
            self._inventory_move(
                "inventory_replenished",
                "replenish",
                wid,
                pid,
                delivery.quantity,
                0,
                delivered,
                delivery.delivery_id,
                corr,
            )

        self._defer_state(delivered, receive)


def utc(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=UTC)


def day_start(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def fx(currency: str, d: date) -> float:  # re-export for writers
    return fx_rate(currency, d)
