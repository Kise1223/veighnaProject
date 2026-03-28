"""Deterministic helpers for M13 benchmark and attribution analytics."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

from libs.analytics.portfolio_normalize import (
    compute_hhi_concentration as _compute_hhi_concentration,
)
from libs.analytics.portfolio_normalize import (
    compute_top_concentration as _compute_top_concentration,
)

_ZERO = Decimal("0")
_WEIGHT = Decimal("0.000001")
_MONEY = Decimal("0.01")
_RETURN = Decimal("0.000001")


def quantize_weight(value: Decimal) -> Decimal:
    return value.quantize(_WEIGHT, rounding=ROUND_HALF_UP)


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(_MONEY, rounding=ROUND_HALF_UP)


def quantize_return(value: Decimal) -> Decimal:
    return value.quantize(_RETURN, rounding=ROUND_HALF_UP)


def safe_decimal_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= _ZERO:
        return _ZERO.quantize(_WEIGHT)
    return quantize_return(numerator / denominator)


def compute_active_share(*, left_weights: dict[str, Decimal], right_weights: dict[str, Decimal]) -> Decimal:
    keys = set(left_weights) | set(right_weights)
    total = sum((abs(left_weights.get(key, _ZERO) - right_weights.get(key, _ZERO)) for key in keys), _ZERO)
    return quantize_weight(total / Decimal("2"))


def compute_top_concentration(weights: list[Decimal], top_n: int) -> Decimal:
    return _compute_top_concentration(weights, top_n)


def compute_hhi_concentration(weights: list[Decimal]) -> Decimal:
    return _compute_hhi_concentration(weights)
