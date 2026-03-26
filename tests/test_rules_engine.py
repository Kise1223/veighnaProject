from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from libs.common.time import CN_TZ
from libs.rules_engine import (
    calc_cost,
    get_lot_size,
    get_price_limit,
    get_trading_phase,
    is_cancel_allowed,
    is_order_accepting,
    is_t0_allowed,
    is_trade_day,
    next_actionable_time,
    validate_order,
)
from libs.schemas.trading import (
    AccountSnapshot,
    MarketSnapshot,
    OrderRequest,
    OrderSide,
    PositionSnapshot,
)
from tests.bootstrap_helpers import bootstrap_payload, bootstrap_rules


def _instrument(instrument_key: str):
    payload = bootstrap_payload()
    return next(item for item in payload.instruments if item.instrument_key == instrument_key)


def test_trade_day_uses_exchange_calendar() -> None:
    calendars = bootstrap_rules()._calendars
    assert is_trade_day(date(2026, 3, 26), _instrument("EQ_SH_600000").exchange, calendars)
    assert not is_trade_day(date(2026, 3, 28), _instrument("EQ_SH_600000").exchange, calendars)


def test_shenzhen_cancel_window_is_blocked() -> None:
    repo = bootstrap_rules()
    instrument = _instrument("EQ_SZ_000001")
    blocked = datetime(2026, 3, 26, 9, 21, tzinfo=CN_TZ)
    assert not is_cancel_allowed(repo, blocked, instrument)


def test_phase_and_next_actionable_time_cover_after_hours_and_gaps() -> None:
    repo = bootstrap_rules()
    gem = _instrument("EQ_SZ_300750")
    closed_gap = datetime(2026, 3, 26, 9, 26, tzinfo=CN_TZ)
    assert not is_order_accepting(repo, closed_gap, gem)
    assert next_actionable_time(repo, closed_gap, gem) == datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ)
    after_hours = datetime(2026, 3, 26, 15, 10, tzinfo=CN_TZ)
    assert get_trading_phase(repo, after_hours, gem).value == "AFTER_HOURS_FIXED"


def test_lot_size_and_t0_t1_are_data_driven() -> None:
    repo = bootstrap_rules()
    stock = _instrument("EQ_SH_600000")
    etf_t0 = _instrument("ETF_SH_513100")
    assert get_lot_size(repo, stock, "BUY") == 100
    assert not is_t0_allowed(repo, stock)
    assert is_t0_allowed(repo, etf_t0)


def test_price_limit_and_cost_breakdown_are_itemized() -> None:
    repo = bootstrap_rules()
    stock = _instrument("EQ_SH_600000")
    limits = get_price_limit(repo, date(2026, 3, 26), stock, Decimal("10"), Decimal("10"))
    assert limits.upper_limit == Decimal("11.00")
    assert limits.lower_limit == Decimal("9.00")

    payload = bootstrap_payload()
    profile = next(
        item for item in payload.cost_profiles if item.cost_profile_id == "CN_EQ_DEFAULT"
    )
    breakdown = calc_cost(
        trade_date=date(2026, 3, 26),
        instrument=stock,
        cost_profile=profile,
        side=OrderSide.SELL,
        quantity=100,
        price=Decimal("10"),
    )
    assert breakdown.commission == Decimal("5.00")
    assert breakdown.stamp_duty == Decimal("1.00")
    assert breakdown.total > breakdown.stamp_duty


def test_validate_order_enforces_lot_price_and_sellable_rules() -> None:
    repo = bootstrap_rules()
    stock = _instrument("EQ_SZ_000001")
    market = MarketSnapshot(
        instrument_key=stock.instrument_key,
        last_price=Decimal("10"),
        previous_close=Decimal("10"),
        upper_limit=Decimal("11"),
        lower_limit=Decimal("9"),
        is_paused=False,
        exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
        received_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
    )
    invalid_buy = OrderRequest(
        account_id="acct",
        instrument_key=stock.instrument_key,
        exchange=stock.exchange,
        symbol=stock.symbol,
        side=OrderSide.BUY,
        quantity=50,
        price=Decimal("12"),
        reference="ref",
        strategy_run_id="run",
        order_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
    )
    buy_result = validate_order(
        invalid_buy,
        AccountSnapshot(account_id="acct", available_cash=Decimal("1000")),
        market,
        stock,
        repo,
    )
    assert not buy_result.accepted
    assert "buy_quantity_must_match_min_lot" in buy_result.reasons
    assert "price_above_upper_limit" in buy_result.reasons

    sell_request = OrderRequest(
        account_id="acct",
        instrument_key=stock.instrument_key,
        exchange=stock.exchange,
        symbol=stock.symbol,
        side=OrderSide.SELL,
        quantity=250,
        price=Decimal("10"),
        reference="ref",
        strategy_run_id="run",
        order_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
    )
    sell_result = validate_order(
        sell_request,
        AccountSnapshot(account_id="acct", available_cash=Decimal("0")),
        market,
        stock,
        repo,
        position_snapshot=PositionSnapshot(
            instrument_key=stock.instrument_key, total_quantity=250, sellable_quantity=250
        ),
    )
    assert not sell_result.accepted
    assert "odd_lot_sell_must_match_remaining_odd_lot" in sell_result.reasons
