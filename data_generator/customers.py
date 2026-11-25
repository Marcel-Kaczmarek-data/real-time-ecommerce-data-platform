"""Synthetic customer master data.

All personal data is fake (Faker) and every e-mail address uses the reserved
``example.*`` domains (RFC 2606), so no generated record can reach a real
person.
"""

from __future__ import annotations

import random
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from faker import Faker

from data_generator.reference import COUNTRIES, COUNTRY_BY_CODE, Country

SEGMENTS = {"consumer": 0.86, "small_business": 0.11, "corporate": 0.03}
LOYALTY_TIERS = ("none", "bronze", "silver", "gold", "platinum")
# Lifetime spend (USD) required to reach each tier.
LOYALTY_THRESHOLDS = {"bronze": 150.0, "silver": 600.0, "gold": 1_800.0, "platinum": 5_000.0}
EXAMPLE_DOMAINS = ("example.com", "example.org", "example.net")


@dataclass
class Customer:
    customer_id: str
    first_name: str
    last_name: str
    email: str
    country: str
    registration_date: datetime
    customer_segment: str
    loyalty_status: str
    marketing_opt_in: bool
    updated_at: datetime
    # Simulation state (not part of the published record).
    activity_weight: float = 1.0
    lifetime_spend_usd: float = 0.0
    order_count: int = 0
    category_affinity: list[str] = field(default_factory=list)

    def record(self) -> dict[str, object]:
        return {
            "customer_id": self.customer_id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": self.email,
            "country": self.country,
            "registration_date": self.registration_date,
            "customer_segment": self.customer_segment,
            "loyalty_status": self.loyalty_status,
            "marketing_opt_in": self.marketing_opt_in,
            "updated_at": self.updated_at,
        }


def _ascii_slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", normalized.lower())


class CustomerFactory:
    """Creates customers deterministically; Faker instances are cached per locale."""

    def __init__(self, rng: random.Random, department_names: list[str]) -> None:
        self.rng = rng
        self.departments = department_names
        self._fakers: dict[str, Faker] = {}
        self._country_weights = [c.traffic_weight for c in COUNTRIES]

    def _faker(self, locale: str) -> Faker:
        if locale not in self._fakers:
            fk = Faker(locale)
            fk.seed_instance(self.rng.getrandbits(32))
            self._fakers[locale] = fk
        return self._fakers[locale]

    def pick_country(self) -> Country:
        return self.rng.choices(COUNTRIES, weights=self._country_weights, k=1)[0]

    def create(self, number: int, registered_at: datetime, country: Country | None = None) -> Customer:
        rng = self.rng
        country = country or self.pick_country()
        fk = self._faker(country.locale)
        first, last = fk.first_name(), fk.last_name()
        local = ".".join(p for p in (_ascii_slug(first), _ascii_slug(last)) if p) or "customer"
        email = f"{local}.{number}@{EXAMPLE_DOMAINS[number % len(EXAMPLE_DOMAINS)]}"
        segment = rng.choices(list(SEGMENTS), weights=list(SEGMENTS.values()), k=1)[0]
        # Heavy-tailed activity: a minority of customers generate most sessions.
        activity = min(rng.paretovariate(1.6), 40.0)
        return Customer(
            customer_id=f"CUST-{number:07d}",
            first_name=first,
            last_name=last,
            email=email,
            country=country.code,
            registration_date=registered_at,
            customer_segment=segment,
            loyalty_status="none",
            marketing_opt_in=rng.random() < 0.55,
            updated_at=registered_at,
            activity_weight=activity,
            category_affinity=rng.sample(self.departments, k=2),
        )


def generate_customers(
    rng: random.Random, count: int, department_names: list[str], start: datetime
) -> tuple[list[Customer], CustomerFactory]:
    """Customers registered over the 3 years before `start`, with existing spend history."""
    factory = CustomerFactory(rng, department_names)
    customers: list[Customer] = []
    span_days = 3 * 365
    for n in range(1, count + 1):
        # More recent registrations are more likely (growing business).
        age_days = int(span_days * (rng.random() ** 1.6))
        registered = start - timedelta(days=age_days, seconds=rng.randint(0, 86_399))
        customer = factory.create(n, registered)
        # Seed prior spend so loyalty tiers are realistic from day one.
        prior_spend = rng.expovariate(1 / 140) * customer.activity_weight * (age_days / 365 + 0.2)
        customer.lifetime_spend_usd = round(prior_spend, 2)
        customer.loyalty_status = loyalty_for_spend(customer.lifetime_spend_usd)
        customer.updated_at = registered + timedelta(days=rng.randint(0, max(age_days, 0)))
        customer.updated_at = min(customer.updated_at, start)
        customers.append(customer)
    return customers, factory


def loyalty_for_spend(spend_usd: float) -> str:
    tier = "none"
    for name, threshold in LOYALTY_THRESHOLDS.items():
        if spend_usd >= threshold:
            tier = name
    return tier


def country_of(customer: Customer) -> Country:
    return COUNTRY_BY_CODE[customer.country]
