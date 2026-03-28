"""Deterministic metric helpers for M12 portfolio analytics."""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

_ZERO = Decimal("0")
_WEIGHT = Decimal("0.000001")


def quantize_weight(value: Decimal) -> Decimal:
    return value.quantize(_WEIGHT, rounding=ROUND_HALF_UP)


def safe_weight_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= _ZERO:
        return _ZERO.quantize(_WEIGHT)
    return quantize_weight(numerator / denominator)


def safe_float_ratio(numerator: Decimal, denominator: Decimal) -> float:
    if denominator <= _ZERO:
        return 0.0
    return round(float(numerator / denominator), 4)


def compute_top_concentration(weights: list[Decimal], top_n: int) -> Decimal:
    ordered = sorted((weight for weight in weights if weight > _ZERO), reverse=True)
    return quantize_weight(sum(ordered[:top_n], _ZERO))


def compute_hhi_concentration(weights: list[Decimal]) -> Decimal:
    return quantize_weight(sum((weight * weight for weight in weights if weight > _ZERO), _ZERO))


def compute_tracking_error_proxy(total_weight_drift_l1: Decimal) -> Decimal:
    return quantize_weight(total_weight_drift_l1 / Decimal("2"))


def compute_realized_turnover(*, filled_notional_total: Decimal, net_liquidation_start: Decimal) -> Decimal:
    if net_liquidation_start <= _ZERO:
        return _ZERO.quantize(_WEIGHT)
    return quantize_weight(filled_notional_total / net_liquidation_start)
