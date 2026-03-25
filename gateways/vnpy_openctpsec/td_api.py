"""Trading adapter contract and wrapper."""

from __future__ import annotations

from typing import Protocol

from gateways.vnpy_openctpsec.compat import CancelRequest, OrderRequest
from gateways.vnpy_openctpsec.mapper import (
    AdapterAccountEvent,
    AdapterContractEvent,
    AdapterOrderEvent,
    AdapterPositionEvent,
    AdapterTradeEvent,
)


class TraderListener(Protocol):
    def on_td_order(self, event: AdapterOrderEvent) -> None: ...

    def on_td_trade(self, event: AdapterTradeEvent) -> None: ...

    def on_td_position(self, event: AdapterPositionEvent) -> None: ...

    def on_td_account(self, event: AdapterAccountEvent) -> None: ...

    def on_td_contract(self, event: AdapterContractEvent) -> None: ...

    def on_td_log(self, message: str) -> None: ...


class OpenCtpTdAdapter(Protocol):
    def set_listener(self, listener: TraderListener) -> None: ...

    def connect(self, settings: dict[str, str]) -> None: ...

    def send_order(self, local_orderid: str, request: OrderRequest) -> None: ...

    def cancel_order(self, request: CancelRequest) -> None: ...

    def query_account(self) -> list[AdapterAccountEvent]: ...

    def query_position(self) -> list[AdapterPositionEvent]: ...

    def query_orders(self) -> list[AdapterOrderEvent]: ...

    def query_trades(self) -> list[AdapterTradeEvent]: ...

    def query_contracts(self) -> list[AdapterContractEvent]: ...

    def close(self) -> None: ...


class TraderApi:
    def __init__(self, adapter: OpenCtpTdAdapter) -> None:
        self.adapter = adapter

    def set_listener(self, listener: TraderListener) -> None:
        self.adapter.set_listener(listener)

    def connect(self, settings: dict[str, str]) -> None:
        self.adapter.connect(settings)

    def send_order(self, local_orderid: str, request: OrderRequest) -> None:
        self.adapter.send_order(local_orderid, request)

    def cancel_order(self, request: CancelRequest) -> None:
        self.adapter.cancel_order(request)

    def query_account(self) -> list[AdapterAccountEvent]:
        return self.adapter.query_account()

    def query_position(self) -> list[AdapterPositionEvent]:
        return self.adapter.query_position()

    def query_orders(self) -> list[AdapterOrderEvent]:
        return self.adapter.query_orders()

    def query_trades(self) -> list[AdapterTradeEvent]:
        return self.adapter.query_trades()

    def query_contracts(self) -> list[AdapterContractEvent]:
        return self.adapter.query_contracts()

    def close(self) -> None:
        self.adapter.close()
