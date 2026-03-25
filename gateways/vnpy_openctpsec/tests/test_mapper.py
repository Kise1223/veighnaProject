from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from gateways.vnpy_openctpsec.mapper import (
    AdapterOrderEvent,
    AdapterTradeEvent,
    make_vt_orderid,
    make_vt_symbol,
    order_dedupe_key,
    to_order_data,
    to_trade_data,
    trade_dedupe_key,
)
from libs.common.time import CN_TZ


def test_identity_helpers_follow_contract() -> None:
    assert make_vt_symbol("600000", "SSE") == "600000.SSE"
    assert make_vt_orderid("OPENCTPSEC", "000001") == "OPENCTPSEC.000001"


def test_order_and_trade_dedupe_keys_are_stable() -> None:
    event = AdapterOrderEvent(
        local_orderid="000001",
        broker_orderid="BROKER-1",
        symbol="600000",
        exchange="SSE",
        direction="BUY",
        price=Decimal("10"),
        volume=100,
        traded=0,
        status="NOTTRADED",
        reference="ref",
        exchange_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
        received_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
    )
    assert order_dedupe_key("OPENCTPSEC", event) == "OPENCTPSEC:order:BROKER-1:NOTTRADED:0"
    assert (
        trade_dedupe_key(
            "OPENCTPSEC",
            type(
                "TradeLike",
                (),
                {"tradeid": "T1"},
            )(),
        )
        == "OPENCTPSEC:trade:T1"
    )


def test_runtime_metadata_is_attached_after_vnpy_object_construction() -> None:
    order_event = AdapterOrderEvent(
        local_orderid="000001",
        broker_orderid="BROKER-1",
        symbol="600000",
        exchange="SSE",
        direction="BUY",
        price=Decimal("10"),
        volume=100,
        traded=0,
        status="NOTTRADED",
        reference="ref",
        exchange_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
        received_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
    )
    trade_event = AdapterTradeEvent(
        local_orderid="000001",
        broker_orderid="BROKER-1",
        tradeid="TRADE-1",
        symbol="600000",
        exchange="SSE",
        direction="BUY",
        price=Decimal("10"),
        volume=100,
        exchange_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
        received_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
    )

    order = to_order_data("OPENCTPSEC", "000001", order_event)
    trade = to_trade_data("OPENCTPSEC", "000001", trade_event)

    assert order.broker_orderid == "BROKER-1"
    assert order.exchange_ts.tzinfo == CN_TZ
    assert order.received_ts.tzinfo == CN_TZ
    assert trade.broker_orderid == "BROKER-1"
    assert trade.exchange_ts.tzinfo == CN_TZ
    assert trade.received_ts.tzinfo == CN_TZ
