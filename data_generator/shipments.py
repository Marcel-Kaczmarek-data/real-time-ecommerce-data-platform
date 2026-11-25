"""Fulfilment planning: warehouse processing, carrier SLAs and delivery delays."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, time, timedelta

from data_generator.reference import CARRIERS

CARRIER_BY_REGION = {
    "North America": ("UPS", "FedEx", "DHL"),
    "Latin America": ("DHL", "FedEx"),
    "Europe": ("DHL", "DPD", "UPS", "Royal Mail"),
    "Asia Pacific": ("DHL", "FedEx", "Japan Post", "Australia Post"),
    "Middle East & Africa": ("DHL", "FedEx"),
}


@dataclass
class ShipmentPlan:
    carrier: str
    label_created_at: datetime
    shipped_at: datetime
    estimated_delivery: datetime
    actual_delivery: datetime
    is_delayed: bool


def plan_shipment(
    rng: random.Random,
    ordered_at: datetime,
    origin_region: str,
    international: bool,
    channel: str,
    peak_season: bool,
) -> ShipmentPlan:
    carrier = rng.choice(CARRIER_BY_REGION.get(origin_region, ("DHL",)))
    domestic_sla, intl_sla, delay_p = CARRIERS[carrier]
    sla_days = intl_sla if international else domestic_sla
    processing_h = rng.lognormvariate(2.2, 0.6)  # median ~9h
    if channel == "marketplace":
        processing_h *= 1.4
    if peak_season:
        processing_h *= 1.8
        delay_p *= 2.0
    if ordered_at.weekday() >= 5:
        processing_h += 18  # reduced weekend picking shifts
    shipped_at = ordered_at + timedelta(hours=processing_h)
    label_created_at = shipped_at - timedelta(minutes=rng.randint(20, 180))
    if label_created_at < ordered_at:
        label_created_at = ordered_at + timedelta(minutes=5)
    eta_day = (shipped_at + timedelta(days=sla_days)).date()
    estimated = datetime.combine(eta_day, time(20, 0), tzinfo=shipped_at.tzinfo)
    delayed = rng.random() < delay_p
    if delayed:
        actual = estimated + timedelta(days=rng.randint(1, 4), hours=rng.uniform(-6, 0))
    else:
        transit_days = max(0.6, sla_days - rng.choice([0, 0, 0, 1]) + rng.uniform(-0.4, 0.2))
        actual = min(shipped_at + timedelta(days=transit_days), estimated - timedelta(minutes=5))
    return ShipmentPlan(carrier, label_created_at, shipped_at, estimated, actual, delayed)
