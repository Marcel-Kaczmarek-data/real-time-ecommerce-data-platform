"""Event model shared by the simulator, producers and file writers."""

from __future__ import annotations

import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

SOURCE_SYSTEMS = {
    "customers": "customer-service",
    "orders": "ecommerce-platform",
    "order_items": "ecommerce-platform",
    "payments": "payment-gateway",
    "inventory": "warehouse-management",
    "shipments": "fulfilment-service",
    "clickstream": "web-analytics",
    "refunds": "payment-gateway",
    "returns": "returns-service",
}

# Optional prefix isolates benchmark / test runs from the main topics (e.g. "bench.").
TOPIC_PREFIX = os.environ.get("KAFKA_TOPIC_PREFIX", "")

# Topic per entity (mirrors config/kafka-topics.yaml).
_BASE_TOPICS = {
    "customers": "ecommerce.customers",
    "orders": "ecommerce.orders",
    "order_items": "ecommerce.order-items",
    "payments": "ecommerce.payments",
    "inventory": "ecommerce.inventory",
    "shipments": "ecommerce.shipments",
    "clickstream": "ecommerce.clickstream",
    "refunds": "ecommerce.refunds",
    "returns": "ecommerce.returns",
}
TOPICS = {entity: f"{TOPIC_PREFIX}{name}" for entity, name in _BASE_TOPICS.items()}
DEAD_LETTER_TOPIC = f"{TOPIC_PREFIX}ecommerce.dead-letter"

PARTITION_KEYS = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_id",
    "payments": "order_id",
    "inventory": "product_id",
    "shipments": "order_id",
    "clickstream": "session_id",
    "refunds": "order_id",
    "returns": "order_id",
}

EVENT_VERSION = 1


@dataclass(order=False)
class Event:
    entity: str
    event_type: str
    event_id: str
    event_timestamp: datetime
    correlation_id: str
    payload: dict[str, Any]
    event_version: int = EVENT_VERSION
    # When the event becomes visible to the pipeline (>= event_timestamp for late events).
    emit_at: datetime | None = None
    injected_defect: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def topic(self) -> str:
        return TOPICS[self.entity]

    @property
    def key(self) -> str:
        return str(self.payload.get(PARTITION_KEYS[self.entity]) or "")

    @property
    def source_system(self) -> str:
        return SOURCE_SYSTEMS[self.entity]

    def envelope(self) -> dict[str, Any]:
        """The standard event envelope (see docs/data-contracts.md)."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "event_timestamp": self.event_timestamp,
            "source_system": self.source_system,
            "correlation_id": self.correlation_id,
            "payload": self.payload,
        }


class IdFactory:
    """Deterministic identifiers derived from a seeded RNG."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.counters: dict[str, int] = {}

    def uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def sequence(self, prefix: str, width: int = 9) -> str:
        n = self.counters.get(prefix, 0) + 1
        self.counters[prefix] = n
        return f"{prefix}-{n:0{width}d}"
