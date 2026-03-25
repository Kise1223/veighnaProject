"""Itemized cost calculation shared by simulation and live execution."""

from __future__ import annotations

from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from libs.schemas.master_data import CostProfile, Instrument
from libs.schemas.trading import CostBreakdown, OrderSide

CENT = Decimal("0.01")


def calc_cost(
    *,
    trade_date: date,
    instrument: Instrument,
    cost_profile: CostProfile,
    side: OrderSide,
    quantity: int,
    price: Decimal,
) -> CostBreakdown:
    """Return an itemized trade-cost breakdown."""

    notional = Decimal(quantity) * price
    commission = _quantize(
        max(notional * cost_profile.commission_rate, cost_profile.commission_min)
    )
    stamp_duty = _quantize(
        notional * cost_profile.tax_sell_rate if side == OrderSide.SELL else Decimal("0")
    )
    handling_fee = _quantize(notional * cost_profile.handling_fee_rate)
    transfer_fee = _quantize(notional * cost_profile.transfer_fee_rate)
    reg_fee = _quantize(notional * cost_profile.reg_fee_rate)
    total = _quantize(commission + stamp_duty + handling_fee + transfer_fee + reg_fee)
    return CostBreakdown(
        instrument_key=instrument.instrument_key,
        instrument_type=instrument.instrument_type,
        exchange=instrument.exchange,
        side=side,
        quantity=quantity,
        notional=_quantize(notional),
        commission=commission,
        stamp_duty=stamp_duty,
        handling_fee=handling_fee,
        transfer_fee=transfer_fee,
        reg_fee=reg_fee,
        total=total,
        effective_date=trade_date,
    )


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)
