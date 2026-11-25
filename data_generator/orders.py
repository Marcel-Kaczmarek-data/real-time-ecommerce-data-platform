"""Order pricing: localisation, promotions, shipping and tax.

Amount identities (all in the order currency, rounded to minor units):
    subtotal = sum(quantity * unit_price)
    discount = sum(line discounts)
    total    = subtotal - discount + shipping + tax
    tax      = round((subtotal - discount + shipping) * vat_rate)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from data_generator.products import Product
from data_generator.reference import Country, Promotion, fx_rate, round_money

FREE_SHIPPING_THRESHOLD_USD = 50.0
DOMESTIC_SHIPPING_USD = 4.99
INTERNATIONAL_SHIPPING_USD = 12.99


@dataclass
class PricedLine:
    order_item_id: str
    product: Product
    quantity: int
    unit_price: float
    discount_amount: float
    warehouse_id: str

    @property
    def gross(self) -> float:
        return self.unit_price * self.quantity

    @property
    def net(self) -> float:
        return self.gross - self.discount_amount


@dataclass
class PricedOrder:
    currency: str
    lines: list[PricedLine]
    subtotal_amount: float
    discount_amount: float
    shipping_amount: float
    tax_amount: float
    total_amount: float
    promotion_id: str | None
    fx_rate: float

    @property
    def total_usd(self) -> float:
        return self.total_amount / self.fx_rate

    @property
    def net_revenue_usd(self) -> float:
        return (self.total_amount - self.tax_amount) / self.fx_rate


def localized_price(product: Product, currency: str, d: date) -> float:
    """List price in local currency, using psychological price points."""
    raw = product.unit_price * fx_rate(currency, d)
    minor = 0 if currency in {"JPY", "KRW"} else 2
    if minor == 0:
        return float(max(1, round(raw, -1 if raw > 1000 else 0)))
    return round(max(int(raw) + 0.99 if raw >= 1 else raw, 0.5), 2)


def choose_promotion(promos: list[Promotion], channel: str, departments: set[str]) -> Promotion | None:
    eligible = [
        p
        for p in promos
        if (p.channel is None or p.channel == channel)
        and (p.department is None or p.department in departments)
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda p: (p.discount_pct, p.promotion_id))


def price_order(
    lines: list[tuple[str, Product, int, str]],
    country: Country,
    warehouse_countries: dict[str, str],
    channel: str,
    promos: list[Promotion],
    d: date,
) -> PricedOrder:
    """Price an order. ``lines`` = (order_item_id, product, quantity, warehouse_id)."""
    currency = country.currency
    rate = fx_rate(currency, d)
    promo = choose_promotion(promos, channel, {p.department for _, p, _, _ in lines})
    priced: list[PricedLine] = []
    for item_id, product, qty, wh in lines:
        unit = localized_price(product, currency, d)
        disc = 0.0
        if (
            promo
            and promo.discount_pct > 0
            and (promo.department is None or promo.department == product.department)
        ):
            disc = round_money(unit * qty * promo.discount_pct, currency)
        priced.append(PricedLine(item_id, product, qty, unit, disc, wh))
    subtotal = round_money(sum(pl.gross for pl in priced), currency)
    discount = round_money(sum(pl.discount_amount for pl in priced), currency)
    merchandise_usd = (subtotal - discount) / rate
    international = any(warehouse_countries[pl.warehouse_id] != country.code for pl in priced)
    if (promo and promo.promotion_type == "free_shipping") or merchandise_usd >= FREE_SHIPPING_THRESHOLD_USD:
        shipping = 0.0
    else:
        base = INTERNATIONAL_SHIPPING_USD if international else DOMESTIC_SHIPPING_USD
        shipping = round_money(base * rate, currency)
    tax = round_money((subtotal - discount + shipping) * country.vat_rate, currency)
    total = round_money(subtotal - discount + shipping + tax, currency)
    return PricedOrder(
        currency=currency,
        lines=priced,
        subtotal_amount=subtotal,
        discount_amount=discount,
        shipping_amount=shipping,
        tax_amount=tax,
        total_amount=total,
        promotion_id=promo.promotion_id
        if promo and (discount > 0 or promo.promotion_type == "free_shipping")
        else None,
        fx_rate=rate,
    )
