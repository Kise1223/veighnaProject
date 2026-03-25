"""Idempotent order and trade state handling."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from gateways.vnpy_openctpsec.compat import OrderData, Status, TradeData
from gateways.vnpy_openctpsec.errors import StateTransitionError
from gateways.vnpy_openctpsec.mapper import (
    AdapterOrderEvent,
    AdapterTradeEvent,
    order_dedupe_key,
    to_order_data,
    to_trade_data,
)

MONOTONIC_TRANSITIONS: dict[Status, set[Status]] = {
    Status.SUBMITTING: {Status.NOTTRADED, Status.PARTTRADED, Status.REJECTED, Status.CANCELLED},
    Status.NOTTRADED: {Status.PARTTRADED, Status.ALLTRADED, Status.CANCELLED},
    Status.PARTTRADED: {Status.PARTTRADED, Status.ALLTRADED, Status.CANCELLED},
}

TERMINAL_STATUSES = {Status.ALLTRADED, Status.REJECTED, Status.CANCELLED}


@dataclass
class ManagedOrder:
    local_orderid: str
    order: OrderData
    broker_orderid: str | None = None


class OrderStateMachine:
    """Tracks order/trade lifecycles with monotonic transitions and dedupe keys."""

    def __init__(self, gateway_name: str) -> None:
        self.gateway_name = gateway_name
        self._orders_by_local: dict[str, ManagedOrder] = {}
        self._local_by_broker: dict[str, str] = {}
        self._seen_order_keys: set[str] = set()
        self._seen_trade_keys: set[str] = set()

    def register_local_order(
        self,
        *,
        local_orderid: str,
        symbol: str,
        exchange: str,
        direction: str,
        price: Decimal,
        volume: int,
        reference: str,
        received_ts: datetime,
    ) -> OrderData:
        order = to_order_data(
            self.gateway_name,
            local_orderid,
            AdapterOrderEvent(
                local_orderid=local_orderid,
                broker_orderid=None,
                symbol=symbol,
                exchange=exchange,
                direction=direction,
                price=price,
                volume=volume,
                traded=0,
                status=Status.SUBMITTING.value,
                reference=reference,
                exchange_ts=received_ts,
                received_ts=received_ts,
            ),
        )
        self._orders_by_local[local_orderid] = ManagedOrder(
            local_orderid=local_orderid, order=order
        )
        return order

    def apply_order_event(self, event: AdapterOrderEvent) -> OrderData | None:
        dedupe_key = order_dedupe_key(self.gateway_name, event)
        if dedupe_key in self._seen_order_keys:
            return None
        self._seen_order_keys.add(dedupe_key)

        local_orderid = self._resolve_local_orderid(event.local_orderid, event.broker_orderid)
        if local_orderid not in self._orders_by_local:
            provisional = self.register_local_order(
                local_orderid=local_orderid,
                symbol=event.symbol,
                exchange=event.exchange,
                direction=event.direction,
                price=event.price,
                volume=event.volume,
                reference=event.reference,
                received_ts=event.received_ts,
            )
            self._orders_by_local[local_orderid].order = provisional

        managed = self._orders_by_local[local_orderid]
        if event.broker_orderid:
            managed.broker_orderid = event.broker_orderid
            self._local_by_broker[event.broker_orderid] = local_orderid

        incoming = to_order_data(self.gateway_name, local_orderid, event)
        managed.order = self._merge_order(managed.order, incoming)
        return managed.order

    def apply_trade_event(
        self, event: AdapterTradeEvent
    ) -> tuple[OrderData | None, TradeData | None]:
        trade_key = f"{self.gateway_name}:trade:{event.tradeid}"
        if trade_key in self._seen_trade_keys:
            return None, None
        self._seen_trade_keys.add(trade_key)

        local_orderid = self._resolve_local_orderid(event.local_orderid, event.broker_orderid)
        if local_orderid not in self._orders_by_local:
            raise StateTransitionError(
                f"trade callback arrived without known local order: {event.tradeid}"
            )

        managed = self._orders_by_local[local_orderid]
        if event.broker_orderid:
            managed.broker_orderid = event.broker_orderid
            self._local_by_broker[event.broker_orderid] = local_orderid

        current_traded = int(managed.order.traded)
        total_volume = int(managed.order.volume)
        new_traded = current_traded + event.volume
        next_status = Status.ALLTRADED if new_traded >= total_volume else Status.PARTTRADED
        order_event = AdapterOrderEvent(
            local_orderid=local_orderid,
            broker_orderid=event.broker_orderid,
            symbol=event.symbol,
            exchange=event.exchange,
            direction=event.direction,
            price=Decimal(str(managed.order.price)),
            volume=total_volume,
            traded=new_traded,
            status=next_status.value,
            reference=managed.order.reference,
            exchange_ts=event.exchange_ts,
            received_ts=event.received_ts,
        )
        merged_order = self._merge_order(
            managed.order, to_order_data(self.gateway_name, local_orderid, order_event)
        )
        managed.order = merged_order
        trade = to_trade_data(self.gateway_name, local_orderid, event)
        return merged_order, trade

    def get_unfinished_orderids(self) -> set[str]:
        return {
            local_orderid
            for local_orderid, managed in self._orders_by_local.items()
            if managed.order.status not in TERMINAL_STATUSES
        }

    def _resolve_local_orderid(self, local_orderid: str | None, broker_orderid: str | None) -> str:
        if local_orderid:
            return local_orderid
        if broker_orderid and broker_orderid in self._local_by_broker:
            return self._local_by_broker[broker_orderid]
        if broker_orderid:
            synthetic = f"broker::{broker_orderid}"
            self._local_by_broker[broker_orderid] = synthetic
            return synthetic
        raise StateTransitionError("order callback missing both local_orderid and broker_orderid")

    def _merge_order(self, current: OrderData, incoming: OrderData) -> OrderData:
        if current.status in TERMINAL_STATUSES and incoming.status != current.status:
            raise StateTransitionError(
                f"terminal order state is immutable: {current.status} -> {incoming.status}"
            )
        if incoming.status == current.status:
            if incoming.traded < current.traded:
                raise StateTransitionError("traded volume cannot decrease")
            if incoming.traded == current.traded:
                return current
            return incoming
        allowed = MONOTONIC_TRANSITIONS.get(current.status, set())
        if incoming.status not in allowed:
            raise StateTransitionError(
                f"invalid order transition: {current.status} -> {incoming.status}"
            )
        if incoming.traded < current.traded:
            raise StateTransitionError("traded volume cannot decrease")
        return incoming
