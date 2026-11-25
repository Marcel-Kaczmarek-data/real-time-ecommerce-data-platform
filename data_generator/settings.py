"""Generator scale profiles and behavioural parameters."""

from __future__ import annotations

import os
from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class ErrorInjection:
    """Controlled data defects used to exercise DQ, dedup and DLQ paths."""

    duplicate_rate: float = 0.002  # same event (same event_id) emitted twice
    late_rate: float = 0.003  # event published 1-6 hours after its event time
    out_of_order_rate: float = 0.01  # event published a few minutes late (reordering)
    invalid_rate: float = 0.0005  # contract-violating event (routed to DLQ by producer)


@dataclass(frozen=True)
class GeneratorSettings:
    scale: str = "lite"
    seed: int = 42
    customers: int = 5_000
    products: int = 1_000
    categories: int = 50
    suppliers: int = 100
    warehouses: int = 20
    sessions_per_day: int = 6_000
    # Probabilities driving the journey funnel.
    returning_visitor_share: float = 0.62
    anonymous_registration_rate: float = 0.06
    add_to_cart_prob: float = 0.055
    remove_from_cart_prob: float = 0.12
    checkout_prob: float = 0.58
    checkout_abandon_prob: float = 0.38
    marketplace_order_share: float = 0.08
    cancel_before_ship_prob: float = 0.025
    profile_update_prob_per_day: float = 0.002
    errors: ErrorInjection = field(default_factory=ErrorInjection)

    @property
    def is_full(self) -> bool:
        return self.scale == "full"


SCALES: dict[str, GeneratorSettings] = {
    # Laptop-friendly: ~6k sessions/day (~40k events/day).
    "lite": GeneratorSettings(),
    # Unit tests: tiny and fast.
    "test": GeneratorSettings(
        scale="test",
        customers=300,
        products=120,
        suppliers=25,
        sessions_per_day=400,
        errors=ErrorInjection(0.0, 0.0, 0.0, 0.0),
    ),
    # Reference scale from the specification; sized for the 10M events/day design target.
    "full": GeneratorSettings(
        scale="full",
        customers=100_000,
        products=10_000,
        suppliers=500,
        sessions_per_day=1_450_000,
    ),
}


def get_settings(scale: str | None = None, seed: int | None = None, **overrides: object) -> GeneratorSettings:
    scale = scale or os.environ.get("PLATFORM_SCALE", "lite")
    if scale not in SCALES:
        raise ValueError(f"Unknown scale '{scale}'. Choose from {sorted(SCALES)}")
    settings = SCALES[scale]
    seed = seed if seed is not None else int(os.environ.get("GENERATOR_SEED", settings.seed))
    clean = {k: v for k, v in overrides.items() if v is not None}
    return replace(settings, seed=seed, **clean)  # type: ignore[arg-type]
