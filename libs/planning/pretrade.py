"""Pre-trade bridge helpers for M6 dry-run planning."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

from libs.common.time import CN_TZ
from libs.rules_engine import calc_cost, validate_order
from libs.rules_engine.market_rules import RulesRepository
from libs.schemas.master_data import CostProfile, Instrument
from libs.schemas.trading import (
    AccountSnapshot,
    CostBreakdown,
    MarketSnapshot,
    OrderRequest,
    OrderSide,
    PositionSnapshot,
)
from scripts.load_master_data import BootstrapPayload


def select_cost_profile(
    payload: BootstrapPayload,
    *,
    trade_date: date,
    instrument: Instrument,
    broker: str,
) -> CostProfile:
    candidates = [
        profile
        for profile in payload.cost_profiles
        if profile.broker == broker
        and profile.instrument_type == instrument.instrument_type
        and (profile.exchange is None or profile.exchange == instrument.exchange)
        and profile.effective_from <= trade_date
        and (profile.effective_to is None or profile.effective_to >= trade_date)
    ]
    if not candidates:
        raise KeyError(
            f"no cost profile for broker={broker} instrument_type={instrument.instrument_type.value}"
        )
    return max(
        candidates,
        key=lambda item: (item.exchange is not None, item.effective_from),
    )


def reference_price_from_snapshot(snapshot: MarketSnapshot, field_name: str) -> Decimal | None:
    if field_name == "previous_close":
        return snapshot.previous_close
    if field_name == "last_price":
        return snapshot.last_price
    raise ValueError(f"unsupported reference price field: {field_name}")


def build_order_request(
    *,
    account_id: str,
    strategy_run_id: str,
    instrument: Instrument,
    side: OrderSide,
    quantity: int,
    price: Decimal,
    order_ts: datetime,
    reference: str,
) -> OrderRequest:
    return OrderRequest(
        account_id=account_id,
        instrument_key=instrument.instrument_key,
        exchange=instrument.exchange,
        symbol=instrument.symbol,
        side=side,
        quantity=quantity,
        price=price,
        reference=reference,
        strategy_run_id=strategy_run_id,
        order_ts=order_ts,
        exchange_ts=order_ts,
        received_ts=order_ts,
    )


def evaluate_pretrade(
    *,
    order: OrderRequest,
    account_snapshot: AccountSnapshot,
    position_snapshot: PositionSnapshot | None,
    market_snapshot: MarketSnapshot,
    instrument: Instrument,
    rules_repo: RulesRepository,
    cost_profile: CostProfile,
) -> tuple[bool, list[str], CostBreakdown]:
    result = validate_order(
        order,
        account_snapshot,
        market_snapshot,
        instrument,
        rules_repo,
        position_snapshot=position_snapshot,
    )
    cost = calc_cost(
        trade_date=order.order_ts.date(),
        instrument=instrument,
        cost_profile=cost_profile,
        side=order.side,
        quantity=order.quantity,
        price=order.price,
    )
    reasons = list(result.reasons)
    if order.side == OrderSide.BUY:
        cash_required = cost.notional + cost.total
        if cash_required > account_snapshot.available_cash:
            reasons.append("insufficient_cash_for_planned_buy")
    return not reasons, reasons, cost


def planning_datetime(trade_date: date, planning_time: time) -> datetime:
    return datetime.combine(trade_date, planning_time, tzinfo=CN_TZ)
