"""Normalization helpers for M11 execution analytics inputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

from libs.analytics.schemas import ExecutionSourceType, SessionEndStatus

_ZERO = Decimal("0")
_CENT = Decimal("0.01")


@dataclass(frozen=True)
class NormalizedExecutionFill:
    order_id: str
    fill_dt: datetime
    quantity: int
    price: Decimal
    notional: Decimal
    realized_cost_total: Decimal


@dataclass(frozen=True)
class NormalizedExecutionOrder:
    order_id: str
    instrument_key: str
    symbol: str
    exchange: str
    side: str
    requested_quantity: int
    reference_price: Decimal
    previous_close: Decimal | None
    estimated_cost_total: Decimal
    session_end_status: SessionEndStatus
    replay_mode: str | None
    fill_model_name: str | None
    time_in_force: str | None
    fills: list[NormalizedExecutionFill]
    partial_fill_count: int


@dataclass(frozen=True)
class NormalizedExecutionSource:
    source_type: ExecutionSourceType
    source_run_id: str
    trade_date: date
    account_id: str
    basket_id: str
    execution_task_id: str
    strategy_run_id: str
    prediction_run_id: str
    source_qlib_export_run_id: str | None
    source_standard_build_run_id: str | None
    replay_mode: str | None
    fill_model_name: str | None
    time_in_force: str | None
    orders: list[NormalizedExecutionOrder]


def quantize_money(value: Decimal) -> Decimal:
    return value.quantize(_CENT, rounding=ROUND_HALF_UP)


def compute_fill_rate(*, requested_quantity: int, filled_quantity: int) -> float:
    if requested_quantity <= 0:
        return 0.0
    return round(filled_quantity / requested_quantity, 4)


def compute_avg_fill_price(*, filled_notional: Decimal, filled_quantity: int) -> Decimal | None:
    if filled_quantity <= 0:
        return None
    return (filled_notional / Decimal(filled_quantity)).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP
    )


def compute_implementation_shortfall(
    *,
    side: str,
    reference_price: Decimal,
    avg_fill_price: Decimal | None,
    filled_quantity: int,
    realized_cost_total: Decimal,
) -> Decimal:
    if filled_quantity <= 0 or avg_fill_price is None:
        return _ZERO.quantize(_CENT)
    quantity = Decimal(filled_quantity)
    if side == "BUY":
        raw = (avg_fill_price - reference_price) * quantity + realized_cost_total
    else:
        raw = (reference_price - avg_fill_price) * quantity + realized_cost_total
    return quantize_money(raw)
