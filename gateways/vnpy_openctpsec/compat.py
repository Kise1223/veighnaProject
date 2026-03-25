"""VeighNa compatibility layer with a local fallback for tests."""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any

VN_AVAILABLE = False

try:  # pragma: no cover - exercised only when vnpy is installed
    from vnpy.event import EventEngine  # type: ignore
    from vnpy.trader.constant import (  # type: ignore
        Direction,
        Exchange,
        Interval,
        OrderType,
        Product,
        Status,
    )
    from vnpy.trader.engine import MainEngine, OmsEngine  # type: ignore
    from vnpy.trader.gateway import BaseGateway  # type: ignore
    from vnpy.trader.object import (  # type: ignore
        AccountData,
        BarData,
        CancelRequest,
        ContractData,
        LogData,
        OrderData,
        OrderRequest,
        PositionData,
        SubscribeRequest,
        TickData,
        TradeData,
    )

    VN_AVAILABLE = True
except Exception:  # pragma: no cover - the fallback is the default local path

    class Exchange(StrEnum):
        SSE = "SSE"
        SZSE = "SZSE"

    class Direction(StrEnum):
        LONG = "LONG"
        SHORT = "SHORT"

    class Status(StrEnum):
        SUBMITTING = "SUBMITTING"
        NOTTRADED = "NOTTRADED"
        PARTTRADED = "PARTTRADED"
        ALLTRADED = "ALLTRADED"
        CANCELLED = "CANCELLED"
        REJECTED = "REJECTED"

    class Product(StrEnum):
        EQUITY = "EQUITY"
        ETF = "ETF"

    class OrderType(StrEnum):
        LIMIT = "LIMIT"

    @dataclass
    class TickData:
        symbol: str
        exchange: Exchange
        datetime: dt.datetime
        name: str
        last_price: Decimal
        gateway_name: str
        volume: Decimal = Decimal("0")
        turnover: Decimal = Decimal("0")
        open_interest: Decimal = Decimal("0")
        limit_up: Decimal | None = None
        limit_down: Decimal | None = None
        bid_price_1: Decimal | None = None
        bid_price_2: Decimal | None = None
        bid_price_3: Decimal | None = None
        bid_price_4: Decimal | None = None
        bid_price_5: Decimal | None = None
        ask_price_1: Decimal | None = None
        ask_price_2: Decimal | None = None
        ask_price_3: Decimal | None = None
        ask_price_4: Decimal | None = None
        ask_price_5: Decimal | None = None
        bid_volume_1: Decimal | None = None
        bid_volume_2: Decimal | None = None
        bid_volume_3: Decimal | None = None
        bid_volume_4: Decimal | None = None
        bid_volume_5: Decimal | None = None
        ask_volume_1: Decimal | None = None
        ask_volume_2: Decimal | None = None
        ask_volume_3: Decimal | None = None
        ask_volume_4: Decimal | None = None
        ask_volume_5: Decimal | None = None
        exchange_ts: dt.datetime | None = None
        received_ts: dt.datetime | None = None

        @property
        def vt_symbol(self) -> str:
            return f"{self.symbol}.{self.exchange}"

    class Interval(StrEnum):
        MINUTE = "1m"
        DAILY = "d"

    @dataclass
    class BarData:
        symbol: str
        exchange: Exchange
        datetime: dt.datetime
        gateway_name: str
        interval: Interval | None = None
        volume: Decimal = Decimal("0")
        turnover: Decimal = Decimal("0")
        open_interest: Decimal = Decimal("0")
        open_price: Decimal = Decimal("0")
        high_price: Decimal = Decimal("0")
        low_price: Decimal = Decimal("0")
        close_price: Decimal = Decimal("0")

        @property
        def vt_symbol(self) -> str:
            return f"{self.symbol}.{self.exchange}"

    @dataclass
    class SubscribeRequest:
        symbol: str
        exchange: Exchange

    @dataclass
    class OrderRequest:
        symbol: str
        exchange: Exchange
        direction: Direction
        type: OrderType
        volume: int
        price: Decimal
        reference: str = ""

    @dataclass
    class CancelRequest:
        orderid: str
        symbol: str
        exchange: Exchange

    @dataclass
    class OrderData:
        symbol: str
        exchange: Exchange
        orderid: str
        type: OrderType
        direction: Direction
        price: Decimal
        volume: int
        traded: int
        status: Status
        gateway_name: str
        datetime: dt.datetime
        reference: str = ""
        broker_orderid: str | None = None
        exchange_ts: dt.datetime | None = None
        received_ts: dt.datetime | None = None

        @property
        def vt_symbol(self) -> str:
            return f"{self.symbol}.{self.exchange}"

        @property
        def vt_orderid(self) -> str:
            return f"{self.gateway_name}.{self.orderid}"

    @dataclass
    class TradeData:
        symbol: str
        exchange: Exchange
        orderid: str
        tradeid: str
        direction: Direction
        price: Decimal
        volume: int
        datetime: dt.datetime
        gateway_name: str
        broker_orderid: str | None = None
        exchange_ts: dt.datetime | None = None
        received_ts: dt.datetime | None = None

        @property
        def vt_symbol(self) -> str:
            return f"{self.symbol}.{self.exchange}"

        @property
        def vt_tradeid(self) -> str:
            return f"{self.gateway_name}.{self.tradeid}"

    @dataclass
    class PositionData:
        symbol: str
        exchange: Exchange
        direction: Direction
        volume: int
        frozen: int
        gateway_name: str
        price: Decimal = Decimal("0")

        @property
        def vt_symbol(self) -> str:
            return f"{self.symbol}.{self.exchange}"

    @dataclass
    class AccountData:
        accountid: str
        balance: Decimal
        frozen: Decimal
        gateway_name: str

        @property
        def available(self) -> Decimal:
            return self.balance - self.frozen

    @dataclass
    class ContractData:
        symbol: str
        exchange: Exchange
        name: str
        product: Product
        size: int
        pricetick: Decimal
        gateway_name: str
        min_volume: int = 1

        @property
        def vt_symbol(self) -> str:
            return f"{self.symbol}.{self.exchange}"

    @dataclass
    class LogData:
        msg: str
        gateway_name: str

    class EventEngine:
        def __init__(self) -> None:
            self._handlers: dict[str, list[Callable[[Any], None]]] = defaultdict(list)

        def register(self, event_type: str, handler: Callable[[Any], None]) -> None:
            self._handlers[event_type].append(handler)

        def put(self, event_type: str, data: Any) -> None:
            for handler in self._handlers[event_type]:
                handler(data)

    class OmsEngine:
        def __init__(self, event_engine: EventEngine) -> None:
            self.contracts: dict[str, ContractData] = {}
            self.accounts: dict[str, AccountData] = {}
            self.positions: dict[str, PositionData] = {}
            self.orders: dict[str, OrderData] = {}
            self.trades: dict[str, TradeData] = {}
            event_engine.register("contract", self.on_contract)
            event_engine.register("account", self.on_account)
            event_engine.register("position", self.on_position)
            event_engine.register("order", self.on_order)
            event_engine.register("trade", self.on_trade)

        def on_contract(self, data: ContractData) -> None:
            self.contracts[data.vt_symbol] = data

        def on_account(self, data: AccountData) -> None:
            self.accounts[data.accountid] = data

        def on_position(self, data: PositionData) -> None:
            self.positions[f"{data.vt_symbol}.{data.direction}"] = data

        def on_order(self, data: OrderData) -> None:
            self.orders[data.vt_orderid] = data

        def on_trade(self, data: TradeData) -> None:
            self.trades[data.vt_tradeid] = data

    class MainEngine:
        def __init__(self, event_engine: EventEngine) -> None:
            self.event_engine = event_engine
            self._engines: dict[str, Any] = {"oms": OmsEngine(event_engine)}
            self._gateways: dict[str, BaseGateway] = {}

        def add_gateway(self, gateway_class: type[BaseGateway], gateway_name: str) -> BaseGateway:
            gateway = gateway_class(self.event_engine, gateway_name=gateway_name)
            self._gateways[gateway_name] = gateway
            return gateway

        def get_engine(self, name: str) -> Any:
            return self._engines[name]

    class BaseGateway:
        def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
            self.event_engine = event_engine
            self.gateway_name = gateway_name

        def on_tick(self, tick: TickData) -> None:
            self.event_engine.put("tick", tick)

        def on_trade(self, trade: TradeData) -> None:
            self.event_engine.put("trade", trade)

        def on_order(self, order: OrderData) -> None:
            self.event_engine.put("order", order)

        def on_position(self, position: PositionData) -> None:
            self.event_engine.put("position", position)

        def on_account(self, account: AccountData) -> None:
            self.event_engine.put("account", account)

        def on_contract(self, contract: ContractData) -> None:
            self.event_engine.put("contract", contract)

        def on_log(self, log: LogData) -> None:
            self.event_engine.put("log", log)


__all__ = [
    "AccountData",
    "BarData",
    "BaseGateway",
    "CancelRequest",
    "ContractData",
    "Direction",
    "EventEngine",
    "Exchange",
    "Interval",
    "LogData",
    "MainEngine",
    "OmsEngine",
    "OrderData",
    "OrderRequest",
    "OrderType",
    "PositionData",
    "Product",
    "Status",
    "SubscribeRequest",
    "TickData",
    "TradeData",
    "VN_AVAILABLE",
]
