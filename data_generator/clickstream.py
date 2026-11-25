"""Session-level browsing behaviour (device, source, landing page, steps)."""

from __future__ import annotations

import random
from dataclasses import dataclass

from data_generator.reference import BROWSERS, DEVICES, PAID_SOURCES, TRAFFIC_SOURCES, weighted_choice

PAGE_TYPES = ("home", "category", "search_results", "deals", "product")


@dataclass
class SessionContext:
    session_id: str
    anonymous_id: str
    customer_id: str | None
    country: str
    device_type: str
    browser: str
    traffic_source: str
    channel: str
    campaign_id: str | None


def choose_device_and_source(
    rng: random.Random, known_customer: bool, opted_in: bool
) -> tuple[str, str, str]:
    device = weighted_choice(rng, DEVICES)
    browser = weighted_choice(rng, BROWSERS[device])
    sources = dict(TRAFFIC_SOURCES)
    if not (known_customer and opted_in):
        sources.pop("email")
    source = weighted_choice(rng, sources)
    return device, browser, source


def choose_channel(rng: random.Random, device: str, source: str) -> str:
    if device == "mobile" and rng.random() < 0.55:
        return "mobile_app"
    if source == "social" and rng.random() < 0.35:
        return "social_commerce"
    return "web"


def session_steps(rng: random.Random) -> int:
    """Number of interactions after landing: ~40% bounce, otherwise geometric (mean ~6)."""
    if rng.random() < 0.40:
        return 0
    steps = 1
    while rng.random() < 0.83 and steps < 40:
        steps += 1
    return steps


def think_time_seconds(rng: random.Random) -> float:
    return min(rng.expovariate(1 / 38.0) + 2.0, 900.0)


def is_paid(source: str) -> bool:
    return source in PAID_SOURCES


def page_url(page_type: str, ref: str | None = None) -> str:
    if page_type == "product" and ref:
        return f"/p/{ref.lower()}"
    if page_type == "category" and ref:
        return f"/c/{ref.lower()}"
    if page_type == "search_results":
        return "/search"
    return {"home": "/", "deals": "/deals", "cart": "/cart", "checkout": "/checkout"}.get(page_type, "/")
