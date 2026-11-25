"""Product catalogue, categories, brands and suppliers."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime

from faker import Faker

from data_generator.reference import ADJECTIVES, COUNTRIES, DEPARTMENTS, PRODUCT_NOUNS

PRODUCT_STATUS_WEIGHTS = {"active": 0.92, "discontinued": 0.04, "out_of_stock": 0.02, "draft": 0.02}
SUPPLIER_SUFFIXES = ("Trading", "Manufacturing", "Distribution", "Goods")
SUPPLIER_COUNTRIES = (
    "US",
    "DE",
    "GB",
    "FR",
    "IT",
    "ES",
    "NL",
    "PL",
    "CZ",
    "JP",
    "KR",
    "IN",
    "TR",
    "MX",
    "BR",
)


@dataclass
class Category:
    category_id: str
    category_name: str
    department: str
    parent_category: str | None
    price_range: tuple[float, float]
    return_rate: float
    margin: float

    def record(self) -> dict[str, object]:
        return {
            "category_id": self.category_id,
            "category_name": self.category_name,
            "department": self.department,
            "parent_category": self.parent_category,
        }


@dataclass
class Supplier:
    supplier_id: str
    supplier_name: str
    country_code: str
    lead_time_days: int
    reliability_score: float
    departments: tuple[str, ...]
    is_active: bool = True

    def record(self) -> dict[str, object]:
        return {
            "supplier_id": self.supplier_id,
            "supplier_name": self.supplier_name,
            "country_code": self.country_code,
            "lead_time_days": self.lead_time_days,
            "reliability_score": self.reliability_score,
            "is_active": self.is_active,
        }


@dataclass
class Product:
    product_id: str
    product_name: str
    category_id: str
    supplier_id: str
    brand: str
    unit_price: float  # catalogue (list) price in USD
    unit_cost: float
    currency: str
    product_status: str
    weight_kg: float
    department: str
    popularity: float
    updated_at: datetime

    def record(self) -> dict[str, object]:
        return {
            "product_id": self.product_id,
            "product_name": self.product_name,
            "category_id": self.category_id,
            "supplier_id": self.supplier_id,
            "brand": self.brand,
            "unit_price": self.unit_price,
            "unit_cost": self.unit_cost,
            "currency": self.currency,
            "product_status": self.product_status,
            "weight_kg": self.weight_kg,
            "updated_at": self.updated_at,
        }

    @property
    def is_sellable(self) -> bool:
        return self.product_status == "active"


def generate_categories(count: int = 50) -> list[Category]:
    categories: list[Category] = []
    n = 0
    for department, (names, price_range, return_rate, margin) in DEPARTMENTS.items():
        for name in names:
            n += 1
            categories.append(
                Category(
                    category_id=f"CAT-{n:03d}",
                    category_name=name,
                    department=department,
                    parent_category=department,
                    price_range=price_range,
                    return_rate=return_rate,
                    margin=margin,
                )
            )
    return categories[:count]


def generate_suppliers(rng: random.Random, count: int) -> list[Supplier]:
    fk = Faker("en_US")
    fk.seed_instance(rng.getrandbits(32))
    departments = list(DEPARTMENTS)
    suppliers = []
    for n in range(1, count + 1):
        suppliers.append(
            Supplier(
                supplier_id=f"SUP-{n:04d}",
                supplier_name=f"{fk.company()} {rng.choice(SUPPLIER_SUFFIXES)}",
                country_code=rng.choice(SUPPLIER_COUNTRIES),
                lead_time_days=rng.randint(3, 21),
                reliability_score=round(rng.uniform(0.82, 0.995), 3),
                departments=tuple(rng.sample(departments, k=rng.randint(1, 3))),
                is_active=rng.random() > 0.03,
            )
        )
    return suppliers


def _brands(rng: random.Random) -> dict[str, list[str]]:
    fk = Faker("en_US")
    fk.seed_instance(rng.getrandbits(32))
    brands: dict[str, list[str]] = {}
    for department in DEPARTMENTS:
        names: set[str] = set()
        while len(names) < 8:
            names.add(
                fk.last_name() + rng.choice(["", " & Co", "ware", "ly", " Labs", " Home", "tek", " Studio"])
            )
        brands[department] = sorted(names)
    return brands


def generate_products(
    rng: random.Random, count: int, categories: list[Category], suppliers: list[Supplier], as_of: datetime
) -> list[Product]:
    brands = _brands(rng)
    by_department: dict[str, list[Supplier]] = {}
    for s in suppliers:
        for d in s.departments:
            by_department.setdefault(d, []).append(s)
    products: list[Product] = []
    statuses = list(PRODUCT_STATUS_WEIGHTS)
    status_weights = list(PRODUCT_STATUS_WEIGHTS.values())
    for n in range(1, count + 1):
        category = categories[(n - 1) % len(categories)] if n <= len(categories) else rng.choice(categories)
        lo, hi = category.price_range
        # Log-uniform price inside the category range, skewed toward cheaper items.
        price = round(lo * (hi / lo) ** (rng.random() ** 1.7), 2)
        price = max(price, 1.0)
        candidates = by_department.get(category.department) or suppliers
        supplier = rng.choice(candidates)
        brand = rng.choice(brands[category.department])
        noun = rng.choice(PRODUCT_NOUNS.get(category.category_name, ("Item",)))
        products.append(
            Product(
                product_id=f"PRD-{n:06d}",
                product_name=f"{brand} {rng.choice(ADJECTIVES)} {noun}",
                category_id=category.category_id,
                supplier_id=supplier.supplier_id,
                brand=brand,
                unit_price=price,
                unit_cost=round(price * (1 - category.margin) * rng.uniform(0.9, 1.1), 2),
                currency="USD",
                product_status=rng.choices(statuses, weights=status_weights, k=1)[0],
                weight_kg=round(rng.uniform(0.05, 25.0) ** 0.8, 3),
                department=category.department,
                popularity=rng.paretovariate(1.2),
                updated_at=as_of,
            )
        )
    return products


def valid_supplier_countries() -> set[str]:
    return {c.code for c in COUNTRIES}
