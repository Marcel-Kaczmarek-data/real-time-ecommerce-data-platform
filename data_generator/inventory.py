"""Warehouses and stateful inventory with reservations and replenishment.

Stock model per (warehouse, product):
    available  - sellable units
    reserved   - units allocated to unshipped orders
Movements:
    reserve     available -> reserved   (order placed)
    release     reserved  -> available  (payment failed / cancelled)
    ship        reserved  -> out        (shipment created)
    replenish   in        -> available  (supplier delivery received)
    restock     in        -> available  (customer return received)
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime

from data_generator.products import Product
from data_generator.reference import COUNTRY_BY_CODE

# (country, city, capacity units)
WAREHOUSE_SITES: tuple[tuple[str, str, int], ...] = (
    ("US", "Newark", 900_000),
    ("US", "Dallas", 800_000),
    ("US", "Reno", 700_000),
    ("US", "Atlanta", 600_000),
    ("CA", "Toronto", 300_000),
    ("MX", "Monterrey", 250_000),
    ("BR", "Sao Paulo", 300_000),
    ("GB", "Coventry", 500_000),
    ("DE", "Leipzig", 600_000),
    ("DE", "Dortmund", 450_000),
    ("FR", "Lyon", 350_000),
    ("ES", "Zaragoza", 300_000),
    ("IT", "Milan", 280_000),
    ("NL", "Venlo", 400_000),
    ("PL", "Poznan", 380_000),
    ("SE", "Gothenburg", 200_000),
    ("JP", "Chiba", 350_000),
    ("IN", "Bhiwandi", 300_000),
    ("SG", "Tuas", 220_000),
    ("AU", "Sydney", 260_000),
)


@dataclass
class Warehouse:
    warehouse_id: str
    warehouse_name: str
    country_code: str
    city: str
    capacity_units: int

    @property
    def region(self) -> str:
        return COUNTRY_BY_CODE[self.country_code].region

    def record(self) -> dict[str, object]:
        return {
            "warehouse_id": self.warehouse_id,
            "warehouse_name": self.warehouse_name,
            "country_code": self.country_code,
            "city": self.city,
            "capacity_units": self.capacity_units,
        }


def generate_warehouses(count: int = 20) -> list[Warehouse]:
    return [
        Warehouse(f"WH-{i:02d}", f"{city} Fulfilment Centre", country, city, capacity)
        for i, (country, city, capacity) in enumerate(WAREHOUSE_SITES[:count], start=1)
    ]


@dataclass
class StockLevel:
    available: int
    reserved: int
    reorder_threshold: int
    reorder_quantity: int
    replenishment_pending: bool = False
    updated_at: datetime | None = None


class InventoryState:
    def __init__(self, warehouses: list[Warehouse]) -> None:
        self.warehouses = {w.warehouse_id: w for w in warehouses}
        self.levels: dict[tuple[str, str], StockLevel] = {}
        self.by_product: dict[str, list[str]] = {}
        # country -> warehouses ordered by preference (same country, same region, rest)
        self._preference: dict[str, list[str]] = {}
        for code, country in COUNTRY_BY_CODE.items():
            same_country = [w.warehouse_id for w in warehouses if w.country_code == code]
            same_region = [
                w.warehouse_id for w in warehouses if w.region == country.region and w.country_code != code
            ]
            rest = [w.warehouse_id for w in warehouses if w.warehouse_id not in same_country + same_region]
            self._preference[code] = same_country + same_region + rest

    def seed(self, rng: random.Random, products: list[Product], as_of: datetime) -> None:
        wh_ids = list(self.warehouses)
        regional_hubs: dict[str, list[str]] = {}
        for wid, w in self.warehouses.items():
            regional_hubs.setdefault(w.region, []).append(wid)
        for p in products:
            # Every product is stocked in one hub per region (with some gaps) plus a few extra sites.
            sites = {rng.choice(hubs) for hubs in regional_hubs.values() if rng.random() < 0.85}
            sites.update(rng.sample(wh_ids, k=rng.randint(1, 3)))
            base = max(5, int(40 * min(p.popularity, 12)))
            for wid in sorted(sites):
                qty = 0 if p.product_status == "out_of_stock" else int(base * rng.uniform(0.5, 1.6))
                threshold = max(3, int(base * 0.2))
                self.levels[(wid, p.product_id)] = StockLevel(
                    available=qty,
                    reserved=0,
                    reorder_threshold=threshold,
                    reorder_quantity=max(10, int(base * 1.2)),
                    updated_at=as_of,
                )
                self.by_product.setdefault(p.product_id, []).append(wid)

    def total_available(self, product_id: str) -> int:
        return sum(self.levels[(w, product_id)].available for w in self.by_product.get(product_id, []))

    def pick_warehouse(self, product_id: str, country_code: str, quantity: int) -> str | None:
        """Closest warehouse with enough available stock, or None (out of stock)."""
        stocked = set(self.by_product.get(product_id, []))
        for wid in self._preference[country_code]:
            if wid in stocked and self.levels[(wid, product_id)].available >= quantity:
                return wid
        return None

    def apply(
        self, warehouse_id: str, product_id: str, d_available: int, d_reserved: int, ts: datetime
    ) -> StockLevel:
        level = self.levels[(warehouse_id, product_id)]
        level.available += d_available
        level.reserved += d_reserved
        level.updated_at = ts
        return level

    def records(self) -> list[dict[str, object]]:
        return [
            {
                "warehouse_id": wid,
                "product_id": pid,
                "available_quantity": lvl.available,
                "reserved_quantity": lvl.reserved,
                "reorder_threshold": lvl.reorder_threshold,
                "updated_at": lvl.updated_at,
            }
            for (wid, pid), lvl in self.levels.items()
        ]
