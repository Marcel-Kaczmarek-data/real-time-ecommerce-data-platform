"""The simulated company: reference/master data plus mutable simulation state."""

from __future__ import annotations

import bisect
import itertools
import random
from dataclasses import dataclass
from datetime import date, datetime

from data_generator.customers import Customer, CustomerFactory, generate_customers
from data_generator.inventory import InventoryState, Warehouse, generate_warehouses
from data_generator.products import (
    Category,
    Product,
    Supplier,
    generate_categories,
    generate_products,
    generate_suppliers,
)
from data_generator.reference import (
    COUNTRIES,
    CURRENCIES,
    DEPARTMENTS,
    Promotion,
    promotions_between,
)
from data_generator.settings import GeneratorSettings


@dataclass
class World:
    settings: GeneratorSettings
    as_of: datetime
    categories: list[Category]
    suppliers: list[Supplier]
    products: list[Product]
    warehouses: list[Warehouse]
    customers: list[Customer]
    customer_factory: CustomerFactory
    inventory: InventoryState
    rng: random.Random

    def __post_init__(self) -> None:
        self.category_by_id = {c.category_id: c for c in self.categories}
        self.product_by_id = {p.product_id: p for p in self.products}
        self.supplier_by_id = {s.supplier_id: s for s in self.suppliers}
        self.customer_by_id = {c.customer_id: c for c in self.customers}
        self._products_by_department: dict[str, list[Product]] = {}
        for p in self.products:
            if p.product_status in {"active", "out_of_stock"}:
                self._products_by_department.setdefault(p.department, []).append(p)
        self._dept_cum: dict[str, list[float]] = {
            d: list(itertools.accumulate(p.popularity for p in prods))
            for d, prods in self._products_by_department.items()
        }
        self._rebuild_customer_weights()
        self._promo_cache: dict[date, list[Promotion]] = {}

    # ---------------------------------------------------------- customers --
    def _rebuild_customer_weights(self) -> None:
        self._cust_cum = list(itertools.accumulate(c.activity_weight for c in self.customers))
        self._by_country: dict[str, list[Customer]] = {}
        for c in self.customers:
            self._by_country.setdefault(c.country, []).append(c)
        self._country_cum = {
            code: list(itertools.accumulate(c.activity_weight for c in custs))
            for code, custs in self._by_country.items()
        }

    def pick_returning_customer(self, rng: random.Random, country_code: str | None = None) -> Customer | None:
        if country_code is not None:
            custs = self._by_country.get(country_code)
            if not custs:
                return None
            cum = self._country_cum[country_code]
            return custs[bisect.bisect_left(cum, rng.random() * cum[-1])]
        x = rng.random() * self._cust_cum[-1]
        return self.customers[bisect.bisect_left(self._cust_cum, x)]

    def register_customer(self, registered_at: datetime, country_code: str) -> Customer:
        from data_generator.reference import COUNTRY_BY_CODE

        number = len(self.customers) + 1
        customer = self.customer_factory.create(number, registered_at, COUNTRY_BY_CODE[country_code])
        self.customers.append(customer)
        self.customer_by_id[customer.customer_id] = customer
        self._cust_cum.append(self._cust_cum[-1] + customer.activity_weight)
        bucket = self._by_country.setdefault(customer.country, [])
        cum = self._country_cum.setdefault(customer.country, [])
        bucket.append(customer)
        cum.append((cum[-1] if cum else 0.0) + customer.activity_weight)
        return customer

    # ----------------------------------------------------------- products --
    def pick_product(self, rng: random.Random, department: str | None = None) -> Product:
        if department is None or department not in self._products_by_department:
            department = rng.choice(list(self._products_by_department))
        prods = self._products_by_department[department]
        cum = self._dept_cum[department]
        return prods[bisect.bisect_left(cum, rng.random() * cum[-1])]

    def category_of(self, product: Product) -> Category:
        return self.category_by_id[product.category_id]

    # --------------------------------------------------------- promotions --
    def active_promotions(self, d: date) -> list[Promotion]:
        if d not in self._promo_cache:
            self._promo_cache[d] = [p for p in promotions_between(d, d) if p.is_active(d)]
        return self._promo_cache[d]

    # ------------------------------------------------------ static tables --
    @staticmethod
    def country_records() -> list[dict[str, object]]:
        return [
            {
                "country_code": c.code,
                "country_name": c.name,
                "region": c.region,
                "currency_code": c.currency,
                "vat_rate": c.vat_rate,
                "timezone": c.timezone,
            }
            for c in COUNTRIES
        ]

    @staticmethod
    def currency_records() -> list[dict[str, object]]:
        return [
            {"currency_code": code, "currency_name": name, "minor_units": minor}
            for code, (name, minor, _, _) in CURRENCIES.items()
        ]


def build_world(settings: GeneratorSettings, as_of: datetime) -> World:
    """Build master data deterministically from ``settings.seed``.

    ``as_of`` is the point in time the master data represents (start of the simulation).
    """
    rng = random.Random(settings.seed)
    categories = generate_categories(settings.categories)
    suppliers = generate_suppliers(random.Random(rng.getrandbits(64)), settings.suppliers)
    products = generate_products(
        random.Random(rng.getrandbits(64)), settings.products, categories, suppliers, as_of
    )
    warehouses = generate_warehouses(settings.warehouses)
    customers, factory = generate_customers(
        random.Random(rng.getrandbits(64)), settings.customers, list(DEPARTMENTS), as_of
    )
    inventory = InventoryState(warehouses)
    inventory.seed(random.Random(rng.getrandbits(64)), products, as_of)
    return World(
        settings=settings,
        as_of=as_of,
        categories=categories,
        suppliers=suppliers,
        products=products,
        warehouses=warehouses,
        customers=customers,
        customer_factory=factory,
        inventory=inventory,
        rng=random.Random(rng.getrandbits(64)),
    )
