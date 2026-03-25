from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from gateways.vnpy_openctpsec.compat import Status
from gateways.vnpy_openctpsec.errors import StateTransitionError
from gateways.vnpy_openctpsec.mapper import AdapterOrderEvent, AdapterTradeEvent
from gateways.vnpy_openctpsec.state import OrderStateMachine
from libs.common.time import CN_TZ


def _ts() -> datetime:
    return datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ)


def test_state_machine_handles_duplicates_and_terminal_immutability() -> None:
    machine = OrderStateMachine("OPENCTPSEC")
    machine.register_local_order(
        local_orderid="000001",
        symbol="600000",
        exchange="SSE",
        direction="BUY",
        price=Decimal("10"),
        volume=100,
        reference="ref",
        received_ts=_ts(),
    )
    ack = AdapterOrderEvent(
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
        exchange_ts=_ts(),
        received_ts=_ts(),
    )
    assert machine.apply_order_event(ack) is not None
    assert machine.apply_order_event(ack) is None

    trade = AdapterTradeEvent(
        local_orderid="000001",
        broker_orderid="BROKER-1",
        tradeid="TRADE-1",
        symbol="600000",
        exchange="SSE",
        direction="BUY",
        price=Decimal("10"),
        volume=100,
        exchange_ts=_ts(),
        received_ts=_ts(),
    )
    order, trade_obj = machine.apply_trade_event(trade)
    assert order is not None and order.status == Status.ALLTRADED
    assert trade_obj is not None

    cancelled = AdapterOrderEvent(
        local_orderid="000001",
        broker_orderid="BROKER-1",
        symbol="600000",
        exchange="SSE",
        direction="BUY",
        price=Decimal("10"),
        volume=100,
        traded=100,
        status="CANCELLED",
        reference="ref",
        exchange_ts=_ts(),
        received_ts=_ts(),
    )
    with pytest.raises(StateTransitionError):
        machine.apply_order_event(cancelled)


def test_trade_before_ack_is_supported_when_local_orderid_is_present() -> None:
    machine = OrderStateMachine("OPENCTPSEC")
    machine.register_local_order(
        local_orderid="000002",
        symbol="513100",
        exchange="SSE",
        direction="BUY",
        price=Decimal("1"),
        volume=200,
        reference="ref",
        received_ts=_ts(),
    )
    order, trade = machine.apply_trade_event(
        AdapterTradeEvent(
            local_orderid="000002",
            broker_orderid=None,
            tradeid="TRADE-2",
            symbol="513100",
            exchange="SSE",
            direction="BUY",
            price=Decimal("1"),
            volume=100,
            exchange_ts=_ts(),
            received_ts=_ts(),
        )
    )
    assert order is not None and order.status == Status.PARTTRADED
    assert trade is not None
