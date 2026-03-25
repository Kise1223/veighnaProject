"""Symbol, identity, and object mapping helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from gateways.vnpy_openctpsec.compat import (
    AccountData,
    ContractData,
    Direction,
    Exchange,
    LogData,
    OrderData,
    OrderRequest,
    OrderType,
    PositionData,
    Product,
    Status,
    TickData,
    TradeData,
)
from libs.common.time import ensure_cn_aware
from libs.schemas.master_data import InstrumentType

STATUS_MAP = {
    "SUBMITTING": Status.SUBMITTING,
    "NOTTRADED": Status.NOTTRADED,
    "PARTTRADED": Status.PARTTRADED,
    "ALLTRADED": Status.ALLTRADED,
    "CANCELLED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
}

DIRECTION_MAP = {
    "BUY": Direction.LONG,
    "SELL": Direction.SHORT,
}

PRODUCT_MAP = {
    InstrumentType.EQUITY.value: Product.EQUITY,
    InstrumentType.ETF.value: Product.ETF,
}


@dataclass(frozen=True)
class AdapterOrderEvent:
    local_orderid: str | None
    broker_orderid: str | None
    symbol: str
    exchange: str
    direction: str
    price: Decimal
    volume: int
    traded: int
    status: str
    reference: str
    exchange_ts: datetime
    received_ts: datetime


@dataclass(frozen=True)
class AdapterTradeEvent:
    local_orderid: str | None
    broker_orderid: str | None
    tradeid: str
    symbol: str
    exchange: str
    direction: str
    price: Decimal
    volume: int
    exchange_ts: datetime
    received_ts: datetime


@dataclass(frozen=True)
class AdapterPositionEvent:
    symbol: str
    exchange: str
    volume: int
    frozen: int
    price: Decimal


@dataclass(frozen=True)
class AdapterAccountEvent:
    accountid: str
    balance: Decimal
    frozen: Decimal


@dataclass(frozen=True)
class AdapterContractEvent:
    symbol: str
    exchange: str
    name: str
    product: str
    size: int
    pricetick: Decimal
    min_volume: int


@dataclass(frozen=True)
class AdapterTickEvent:
    symbol: str
    exchange: str
    name: str
    last_price: Decimal
    exchange_ts: datetime
    received_ts: datetime
    volume: Decimal = Decimal("0")
    turnover: Decimal = Decimal("0")
    open_interest: Decimal | None = None
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
    limit_up: Decimal | None = None
    limit_down: Decimal | None = None
    source_seq: str | None = None


def make_vt_symbol(symbol: str, exchange: str) -> str:
    return f"{symbol}.{exchange}"


def make_vt_orderid(gateway_name: str, orderid: str) -> str:
    return f"{gateway_name}.{orderid}"


def make_vt_tradeid(gateway_name: str, tradeid: str) -> str:
    return f"{gateway_name}.{tradeid}"


def order_dedupe_key(gateway_name: str, event: AdapterOrderEvent) -> str:
    identifier = event.broker_orderid or event.local_orderid or "unknown"
    return f"{gateway_name}:order:{identifier}:{event.status}:{event.traded}"


def trade_dedupe_key(gateway_name: str, event: AdapterTradeEvent) -> str:
    return f"{gateway_name}:trade:{event.tradeid}"


def to_vnpy_exchange(exchange: str) -> Exchange:
    return Exchange(exchange)


def to_vnpy_direction(side: str) -> Direction:
    return DIRECTION_MAP[side]


def to_vnpy_status(status: str | Status) -> Status:
    if isinstance(status, Status):
        return status
    if status in STATUS_MAP:
        return STATUS_MAP[status]
    try:
        return Status(status)
    except ValueError:
        return Status[status]


def to_vnpy_order_request(
    *,
    symbol: str,
    exchange: str,
    side: str,
    price: Decimal,
    volume: int,
    reference: str,
) -> OrderRequest:
    return OrderRequest(
        symbol=symbol,
        exchange=to_vnpy_exchange(exchange),
        direction=to_vnpy_direction(side),
        type=OrderType.LIMIT,
        volume=float(volume),
        price=float(price),
        reference=reference,
    )


def to_order_data(gateway_name: str, orderid: str, event: AdapterOrderEvent) -> OrderData:
    order = OrderData(
        gateway_name=gateway_name,
        symbol=event.symbol,
        exchange=to_vnpy_exchange(event.exchange),
        orderid=orderid,
        type=OrderType.LIMIT,
        direction=to_vnpy_direction(event.direction),
        price=float(event.price),
        volume=float(event.volume),
        traded=float(event.traded),
        status=to_vnpy_status(event.status),
        datetime=ensure_cn_aware(event.received_ts),
        reference=event.reference,
    )
    return _attach_runtime_metadata(
        order,
        broker_orderid=event.broker_orderid,
        exchange_ts=ensure_cn_aware(event.exchange_ts),
        received_ts=ensure_cn_aware(event.received_ts),
    )


def to_trade_data(gateway_name: str, orderid: str, event: AdapterTradeEvent) -> TradeData:
    trade = TradeData(
        gateway_name=gateway_name,
        symbol=event.symbol,
        exchange=to_vnpy_exchange(event.exchange),
        orderid=orderid,
        tradeid=event.tradeid,
        direction=to_vnpy_direction(event.direction),
        price=float(event.price),
        volume=float(event.volume),
        datetime=ensure_cn_aware(event.received_ts),
    )
    return _attach_runtime_metadata(
        trade,
        broker_orderid=event.broker_orderid,
        exchange_ts=ensure_cn_aware(event.exchange_ts),
        received_ts=ensure_cn_aware(event.received_ts),
    )


def to_position_data(gateway_name: str, event: AdapterPositionEvent) -> PositionData:
    return PositionData(
        gateway_name=gateway_name,
        symbol=event.symbol,
        exchange=to_vnpy_exchange(event.exchange),
        direction=Direction.LONG,
        volume=float(event.volume),
        frozen=float(event.frozen),
        price=float(event.price),
    )


def to_account_data(gateway_name: str, event: AdapterAccountEvent) -> AccountData:
    return AccountData(
        gateway_name=gateway_name,
        accountid=event.accountid,
        balance=float(event.balance),
        frozen=float(event.frozen),
    )


def to_contract_data(gateway_name: str, event: AdapterContractEvent) -> ContractData:
    return ContractData(
        gateway_name=gateway_name,
        symbol=event.symbol,
        exchange=to_vnpy_exchange(event.exchange),
        name=event.name,
        product=PRODUCT_MAP[event.product],
        size=float(event.size),
        pricetick=float(event.pricetick),
        min_volume=float(event.min_volume),
    )


def to_tick_data(gateway_name: str, event: AdapterTickEvent) -> TickData:
    tick = TickData(
        gateway_name=gateway_name,
        symbol=event.symbol,
        exchange=to_vnpy_exchange(event.exchange),
        datetime=ensure_cn_aware(event.received_ts),
        name=event.name,
        volume=float(event.volume),
        turnover=float(event.turnover),
        open_interest=float(event.open_interest or Decimal("0")),
        last_price=float(event.last_price),
        bid_price_1=float(event.bid_price_1 or Decimal("0")),
        bid_price_2=float(event.bid_price_2 or Decimal("0")),
        bid_price_3=float(event.bid_price_3 or Decimal("0")),
        bid_price_4=float(event.bid_price_4 or Decimal("0")),
        bid_price_5=float(event.bid_price_5 or Decimal("0")),
        ask_price_1=float(event.ask_price_1 or Decimal("0")),
        ask_price_2=float(event.ask_price_2 or Decimal("0")),
        ask_price_3=float(event.ask_price_3 or Decimal("0")),
        ask_price_4=float(event.ask_price_4 or Decimal("0")),
        ask_price_5=float(event.ask_price_5 or Decimal("0")),
        bid_volume_1=float(event.bid_volume_1 or Decimal("0")),
        bid_volume_2=float(event.bid_volume_2 or Decimal("0")),
        bid_volume_3=float(event.bid_volume_3 or Decimal("0")),
        bid_volume_4=float(event.bid_volume_4 or Decimal("0")),
        bid_volume_5=float(event.bid_volume_5 or Decimal("0")),
        ask_volume_1=float(event.ask_volume_1 or Decimal("0")),
        ask_volume_2=float(event.ask_volume_2 or Decimal("0")),
        ask_volume_3=float(event.ask_volume_3 or Decimal("0")),
        ask_volume_4=float(event.ask_volume_4 or Decimal("0")),
        ask_volume_5=float(event.ask_volume_5 or Decimal("0")),
        limit_up=float(event.limit_up or Decimal("0")),
        limit_down=float(event.limit_down or Decimal("0")),
    )
    return _attach_runtime_metadata(
        tick,
        exchange_ts=ensure_cn_aware(event.exchange_ts),
        received_ts=ensure_cn_aware(event.received_ts),
        source_seq=event.source_seq,
    )


def to_log_data(gateway_name: str, message: str) -> LogData:
    return LogData(msg=message, gateway_name=gateway_name)


def _attach_runtime_metadata(target: Any, **metadata: object) -> Any:
    for key, value in metadata.items():
        setattr(target, key, value)
    return target
