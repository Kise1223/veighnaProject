"""Order validation against the M1 rules and account state."""

from __future__ import annotations

from decimal import Decimal

from libs.rules_engine.market_rules import PriceLimit, RulesRepository
from libs.schemas.master_data import Instrument
from libs.schemas.trading import (
    AccountSnapshot,
    MarketSnapshot,
    OrderRequest,
    OrderSide,
    PositionSnapshot,
    ValidationResult,
)


def validate_order(
    order: OrderRequest,
    account_snapshot: AccountSnapshot,
    market_snapshot: MarketSnapshot,
    instrument: Instrument,
    rules: RulesRepository,
    position_snapshot: PositionSnapshot | None = None,
) -> ValidationResult:
    """Validate an order request against trading phase, quantities, and price bands."""

    reasons: list[str] = []
    phase = rules.get_trading_phase(order.order_ts, instrument)

    if not rules.is_order_accepting(order.order_ts, instrument):
        reasons.append("order_not_accepted_in_current_phase")

    if market_snapshot.is_paused:
        reasons.append("instrument_paused")

    if order.side == OrderSide.BUY and order.quantity % instrument.min_buy_lot != 0:
        reasons.append("buy_quantity_must_match_min_lot")

    if order.side == OrderSide.SELL and instrument.odd_lot_sell_only and position_snapshot:
        total_qty = position_snapshot.sellable_quantity
        odd_lot = total_qty % instrument.min_buy_lot
        if odd_lot and order.quantity != odd_lot:
            reasons.append("odd_lot_sell_must_match_remaining_odd_lot")

    if (
        order.side == OrderSide.SELL
        and position_snapshot
        and order.quantity > position_snapshot.sellable_quantity
    ):
        reasons.append("sell_quantity_exceeds_sellable")

    required_cash = Decimal(order.quantity) * order.price
    if order.side == OrderSide.BUY and required_cash > account_snapshot.available_cash:
        reasons.append("insufficient_cash")

    try:
        price_limit = rules.get_price_limit(
            order.order_ts.date(),
            instrument=instrument,
            last_close=market_snapshot.previous_close,
            open_price=market_snapshot.last_price,
            status_flags=set(),
        )
    except ValueError:
        reasons.append("previous_close_required_for_price_limit_validation")
    else:
        _validate_price_band(order.price, price_limit, reasons)

    return ValidationResult(
        accepted=not reasons,
        phase=phase,
        reasons=reasons,
        next_actionable_time=None
        if not reasons
        else rules.next_actionable_time(order.order_ts, instrument),
    )


def _validate_price_band(price: Decimal, limits: PriceLimit, reasons: list[str]) -> None:
    if limits.upper_limit is not None and price > limits.upper_limit:
        reasons.append("price_above_upper_limit")
    if limits.lower_limit is not None and price < limits.lower_limit:
        reasons.append("price_below_lower_limit")
