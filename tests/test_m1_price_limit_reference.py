from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from libs.common.time import CN_TZ
from libs.rules_engine import validate_order
from libs.rules_engine.market_rules import get_price_limit
from libs.schemas.trading import AccountSnapshot, MarketSnapshot, OrderRequest, OrderSide
from tests.bootstrap_helpers import bootstrap_payload, bootstrap_rules


def _instrument(instrument_key: str):
    payload = bootstrap_payload()
    return next(item for item in payload.instruments if item.instrument_key == instrument_key)


def _order_for(instrument_key: str, symbol: str, exchange, price: str) -> OrderRequest:  # type: ignore[no-untyped-def]
    return OrderRequest(
        account_id="acct",
        instrument_key=instrument_key,
        exchange=exchange,
        symbol=symbol,
        side=OrderSide.BUY,
        quantity=100,
        price=Decimal(price),
        reference="ref",
        strategy_run_id="run",
        order_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
    )


def test_price_limit_uses_previous_close_not_last_price() -> None:
    repo = bootstrap_rules()
    stock = _instrument("EQ_SH_600000")
    order = _order_for(stock.instrument_key, stock.symbol, stock.exchange, "10.95")
    account = AccountSnapshot(account_id="acct", available_cash=Decimal("100000"))

    result_a = validate_order(
        order,
        account,
        MarketSnapshot(
            instrument_key=stock.instrument_key,
            last_price=Decimal("9.70"),
            previous_close=Decimal("10.00"),
            exchange_ts=order.order_ts,
            received_ts=order.order_ts,
        ),
        stock,
        repo,
    )
    result_b = validate_order(
        order,
        account,
        MarketSnapshot(
            instrument_key=stock.instrument_key,
            last_price=Decimal("10.80"),
            previous_close=Decimal("10.00"),
            exchange_ts=order.order_ts,
            received_ts=order.order_ts,
        ),
        stock,
        repo,
    )
    assert result_a.accepted
    assert result_b.accepted


def test_missing_previous_close_has_explicit_validation_error() -> None:
    repo = bootstrap_rules()
    stock = _instrument("EQ_SZ_000001")
    result = validate_order(
        _order_for(stock.instrument_key, stock.symbol, stock.exchange, "10.00"),
        AccountSnapshot(account_id="acct", available_cash=Decimal("100000")),
        MarketSnapshot(
            instrument_key=stock.instrument_key,
            last_price=Decimal("10.00"),
            previous_close=None,
            exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
            received_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
        ),
        stock,
        repo,
    )
    assert not result.accepted
    assert "previous_close_required_for_price_limit_validation" in result.reasons


def test_etf_and_no_limit_scenarios_are_handled_explicitly() -> None:
    repo = bootstrap_rules()
    etf = _instrument("ETF_SH_510300")
    etf_limits = get_price_limit(repo, date(2026, 3, 26), etf, Decimal("2.000"), None)
    assert etf_limits.upper_limit == Decimal("2.200")
    assert etf_limits.lower_limit == Decimal("1.800")

    ipo_stock = _instrument("EQ_SH_600000").model_copy(update={"list_date": date(2026, 3, 24)})
    free_limit_result = validate_order(
        _order_for(ipo_stock.instrument_key, ipo_stock.symbol, ipo_stock.exchange, "12.50"),
        AccountSnapshot(account_id="acct", available_cash=Decimal("100000")),
        MarketSnapshot(
            instrument_key=ipo_stock.instrument_key,
            last_price=Decimal("12.00"),
            previous_close=None,
            exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
            received_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
        ),
        ipo_stock,
        repo,
    )
    assert free_limit_result.accepted
