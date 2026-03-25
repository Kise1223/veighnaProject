"""VeighNa-compatible OpenCTP gateway skeleton."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from threading import Lock

from gateways.vnpy_openctpsec.compat import (
    BaseGateway,
    CancelRequest,
    Exchange,
    LogData,
    OrderRequest,
    Status,
    SubscribeRequest,
)
from gateways.vnpy_openctpsec.contract_loader import ContractLoader
from gateways.vnpy_openctpsec.errors import GatewayConfigurationError
from gateways.vnpy_openctpsec.mapper import (
    AdapterAccountEvent,
    AdapterContractEvent,
    AdapterOrderEvent,
    AdapterPositionEvent,
    AdapterTickEvent,
    AdapterTradeEvent,
    make_vt_orderid,
    to_account_data,
    to_contract_data,
    to_log_data,
    to_position_data,
    to_tick_data,
)
from gateways.vnpy_openctpsec.md_api import MarketDataApi, OpenCtpMdAdapter
from gateways.vnpy_openctpsec.reconnect import ReconnectState
from gateways.vnpy_openctpsec.state import OrderStateMachine
from gateways.vnpy_openctpsec.td_api import OpenCtpTdAdapter, TraderApi


class OpenCTPSecGateway(BaseGateway):
    """Non-blocking gateway shell with mock-first adapter injection."""

    default_name = "OPENCTPSEC"
    default_setting = {
        "account_id": "",
        "username": "",
        "password": "",
        "td_address": "",
        "md_address": "",
    }
    exchanges = [Exchange.SSE, Exchange.SZSE]

    def __init__(self, event_engine, gateway_name: str = "OPENCTPSEC") -> None:  # type: ignore[no-untyped-def]
        super().__init__(event_engine, gateway_name)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"{gateway_name}-io")
        self._lock = Lock()
        self._request_seq = 0
        self._settings: dict[str, str] = {}
        self._md_api: MarketDataApi | None = None
        self._td_api: TraderApi | None = None
        self._reconnect_state = ReconnectState()
        self._state = OrderStateMachine(gateway_name=gateway_name)
        self._connected = False
        self._adapters_bound = False
        self._last_error: str | None = None
        self._last_sync_at: datetime | None = None
        self._closed = False

    def bind_adapters(self, md_adapter: OpenCtpMdAdapter, td_adapter: OpenCtpTdAdapter) -> None:
        self._md_api = MarketDataApi(md_adapter)
        self._td_api = TraderApi(td_adapter)
        self._md_api.set_listener(self)
        self._td_api.set_listener(self)
        self._adapters_bound = True

    def connect(self, setting: dict[str, str]) -> None:
        self._settings = setting
        self._submit(self._connect_and_sync)

    def subscribe(self, req: SubscribeRequest) -> None:
        self._reconnect_state.remember_subscription(req)
        self._submit(self._require_md().subscribe, req)

    def send_order(self, req: OrderRequest) -> str:
        direction_value = getattr(req.direction, "value", str(req.direction))
        exchange_value = getattr(req.exchange, "value", str(req.exchange))
        local_orderid = self._next_local_orderid()
        submitting_order = self._state.register_local_order(
            local_orderid=local_orderid,
            symbol=req.symbol,
            exchange=exchange_value,
            direction="BUY" if str(direction_value).endswith("LONG") else "SELL",
            price=Decimal(str(req.price)),
            volume=int(req.volume),
            reference=req.reference,
            received_ts=datetime.now(),
        )
        self._reconnect_state.remember_unfinished_order(local_orderid)
        self.on_order(submitting_order)
        self._submit(self._require_td().send_order, local_orderid, req)
        return make_vt_orderid(self.gateway_name, local_orderid)

    def cancel_order(self, req: CancelRequest) -> None:
        self._submit(self._require_td().cancel_order, req)

    def query_account(self) -> None:
        self._submit(self._sync_accounts)

    def query_position(self) -> None:
        self._submit(self._sync_positions)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._connected = False
        if self._md_api:
            self._md_api.close()
        if self._td_api:
            self._td_api.close()
        self._executor.shutdown(wait=True)

    def get_status_snapshot(self) -> dict[str, object]:
        last_sync_at = self._last_sync_at.isoformat() if self._last_sync_at else None
        return {
            "name": self.gateway_name,
            "registered": True,
            "adapters_bound": self._adapters_bound,
            "connected": self._connected,
            "last_error": self._last_error,
            "last_sync_at": last_sync_at,
            "unfinished_orders": len(self._state.get_unfinished_orderids()),
            "subscriptions": len(self._reconnect_state.subscriptions),
        }

    def _drain_for_tests(self) -> None:
        self._submit(lambda: None).result(timeout=5)

    def _connect_and_sync(self) -> None:
        self._closed = False
        try:
            td_api = self._require_td()
            md_api = self._require_md()
            md_api.connect(self._settings)
            td_api.connect(self._settings)
            self._sync_contracts()
            self._sync_accounts()
            self._sync_positions()
            self._sync_orders()
            self._sync_trades()
        except Exception as exc:
            self._connected = False
            self._last_error = str(exc)
            self.on_log(to_log_data(self.gateway_name, f"connect failed: {exc}"))
            return

        self._connected = True
        self._last_error = None
        self._last_sync_at = datetime.now()
        self.on_log(to_log_data(self.gateway_name, "connect and initial sync completed"))

    def _sync_contracts(self) -> None:
        for event in self._require_td().query_contracts():
            self.on_td_contract(event)

    def _sync_accounts(self) -> None:
        for event in self._require_td().query_account():
            self.on_td_account(event)

    def _sync_positions(self) -> None:
        for event in self._require_td().query_position():
            self.on_td_position(event)

    def _sync_orders(self) -> None:
        for event in self._require_td().query_orders():
            self.on_td_order(event)

    def _sync_trades(self) -> None:
        for event in self._require_td().query_trades():
            self.on_td_trade(event)

    def on_md_tick(self, event: AdapterTickEvent) -> None:
        self.on_tick(to_tick_data(self.gateway_name, event))

    def on_md_log(self, message: str) -> None:
        self.on_log(to_log_data(self.gateway_name, message))

    def on_td_order(self, event: AdapterOrderEvent) -> None:
        order = self._state.apply_order_event(event)
        if order is not None:
            if order.status in {Status.ALLTRADED, Status.CANCELLED, Status.REJECTED}:
                self._reconnect_state.forget_unfinished_order(order.orderid)
            self.on_order(order)

    def on_td_trade(self, event: AdapterTradeEvent) -> None:
        order, trade = self._state.apply_trade_event(event)
        if order is not None:
            if order.status == Status.ALLTRADED:
                self._reconnect_state.forget_unfinished_order(order.orderid)
            self.on_order(order)
        if trade is not None:
            self.on_trade(trade)

    def on_td_position(self, event: AdapterPositionEvent) -> None:
        self.on_position(to_position_data(self.gateway_name, event))

    def on_td_account(self, event: AdapterAccountEvent) -> None:
        self.on_account(to_account_data(self.gateway_name, event))

    def on_td_contract(self, event: AdapterContractEvent) -> None:
        self.on_contract(to_contract_data(self.gateway_name, event))

    def on_td_log(self, message: str) -> None:
        self.on_log(to_log_data(self.gateway_name, message))

    def on_log(self, log: LogData) -> None:
        parent_on_log = getattr(super(), "on_log", None)
        if callable(parent_on_log):
            parent_on_log(log)
            return
        write_log = getattr(self, "write_log", None)
        if callable(write_log):
            write_log(getattr(log, "msg", str(log)))

    def load_bootstrap_contracts(self, bootstrap_dir: Path) -> list[AdapterContractEvent]:
        loader = ContractLoader(bootstrap_dir)
        return loader.load_contracts()

    def _next_local_orderid(self) -> str:
        with self._lock:
            self._request_seq += 1
            return f"{self._request_seq:08d}"

    def _submit(self, fn, *args):  # type: ignore[no-untyped-def]
        return self._executor.submit(fn, *args)

    def _require_md(self) -> MarketDataApi:
        if self._md_api is None:
            raise GatewayConfigurationError("market data adapter is not bound")
        return self._md_api

    def _require_td(self) -> TraderApi:
        if self._td_api is None:
            raise GatewayConfigurationError("trader adapter is not bound")
        return self._td_api
