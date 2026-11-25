"""Payment method selection and authorisation outcomes."""

from __future__ import annotations

import random
from dataclasses import dataclass

from data_generator.reference import FAILURE_REASONS, PAYMENT_METHODS

PROVIDERS = {
    "credit_card": "stripe",
    "debit_card": "adyen",
    "paypal": "paypal",
    "apple_pay": "stripe",
    "google_pay": "adyen",
    "klarna": "klarna",
    "bank_transfer": "adyen",
}
# Country-specific method preferences (multipliers on the global share).
COUNTRY_METHOD_BIAS: dict[str, dict[str, float]] = {
    "DE": {"paypal": 2.0, "klarna": 2.0, "bank_transfer": 2.5, "credit_card": 0.6},
    "NL": {"bank_transfer": 3.0, "klarna": 1.5},
    "SE": {"klarna": 3.0},
    "JP": {"credit_card": 1.4, "paypal": 0.4},
    "IN": {"debit_card": 2.0, "google_pay": 2.5},
    "BR": {"credit_card": 1.5, "bank_transfer": 1.5},
    "US": {"apple_pay": 1.4},
}


@dataclass
class PaymentAttempt:
    method: str
    success: bool
    failure_reason: str | None
    provider: str


def choose_method(rng: random.Random, country_code: str, exclude: str | None = None) -> str:
    bias = COUNTRY_METHOD_BIAS.get(country_code, {})
    methods = [m for m in PAYMENT_METHODS if m != exclude]
    weights = [PAYMENT_METHODS[m][0] * bias.get(m, 1.0) for m in methods]
    return rng.choices(methods, weights=weights, k=1)[0]


def attempt_payment(
    rng: random.Random, method: str, amount_usd: float, is_new_customer: bool, peak_day: bool
) -> PaymentAttempt:
    p_fail = PAYMENT_METHODS[method][1]
    if amount_usd > 500:
        p_fail *= 1.6  # fraud screening on large baskets
    if is_new_customer:
        p_fail *= 1.3
    if peak_day:
        p_fail *= 1.4  # gateway saturation / timeouts on peak days
    if rng.random() < p_fail:
        reason = rng.choice(FAILURE_REASONS if not peak_day else (*FAILURE_REASONS, "timeout", "timeout"))
        return PaymentAttempt(method, False, reason, PROVIDERS[method])
    return PaymentAttempt(method, True, None, PROVIDERS[method])
