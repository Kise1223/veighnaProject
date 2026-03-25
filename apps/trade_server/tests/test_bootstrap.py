from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from time import monotonic, sleep

from apps.trade_server.app.bootstrap import TradeServerBootstrap
from apps.trade_server.app.config import GatewayRuntimeConfig, TradeServerConfig
from gateways.vnpy_openctpsec.compat import Direction, Exchange, OrderRequest, OrderType
from gateways.vnpy_openctpsec.mapper import (
    AdapterAccountEvent,
    AdapterContractEvent,
    AdapterOrderEvent,
    AdapterPositionEvent,
    AdapterTradeEvent,
)
from libs.common.time import CN_TZ

ROOT = Path(__file__).resolve().parents[3]


class MockMdAdapter:
    def set_listener(self, listener) -> None:  # type: ignore[no-untyped-def]
        self.listener = listener

    def connect(self, settings: dict[str, str]) -> None:
        self.listener.on_md_log("md connected")

    def subscribe(self, request) -> None:  # type: ignore[no-untyped-def]
        return None

    def close(self) -> None:
        return None


class MockTdAdapter:
    def set_listener(self, listener) -> None:  # type: ignore[no-untyped-def]
        self.listener = listener

    def connect(self, settings: dict[str, str]) -> None:
        self.listener.on_td_log("td connected")

    def send_order(self, local_orderid: str, request: OrderRequest) -> None:
        exchange = (
            request.exchange.value if hasattr(request.exchange, "value") else str(request.exchange)
        )
        direction = "BUY" if str(request.direction).endswith("LONG") else "SELL"
        self.listener.on_td_order(
            AdapterOrderEvent(
                local_orderid=local_orderid,
                broker_orderid="BROKER-ORDER-1",
                symbol=request.symbol,
                exchange=exchange,
                direction=direction,
                price=Decimal(str(request.price)),
                volume=int(request.volume),
                traded=0,
                status="NOTTRADED",
                reference=request.reference,
                exchange_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
                received_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
            )
        )
        self.listener.on_td_trade(
            AdapterTradeEvent(
                local_orderid=local_orderid,
                broker_orderid="BROKER-ORDER-1",
                tradeid="TRADE-1",
                symbol=request.symbol,
                exchange=exchange,
                direction=direction,
                price=Decimal(str(request.price)),
                volume=int(request.volume),
                exchange_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
                received_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
            )
        )

    def cancel_order(self, request) -> None:  # type: ignore[no-untyped-def]
        return None

    def query_account(self) -> list[AdapterAccountEvent]:
        return [
            AdapterAccountEvent(accountid="ACC1", balance=Decimal("100000"), frozen=Decimal("0"))
        ]

    def query_position(self) -> list[AdapterPositionEvent]:
        return [
            AdapterPositionEvent(
                symbol="600000", exchange="SSE", volume=100, frozen=0, price=Decimal("10")
            )
        ]

    def query_orders(self) -> list[AdapterOrderEvent]:
        return []

    def query_trades(self) -> list[AdapterTradeEvent]:
        return []

    def query_contracts(self) -> list[AdapterContractEvent]:
        return [
            AdapterContractEvent(
                symbol="600000",
                exchange="SSE",
                name="600000",
                product="EQUITY",
                size=1,
                pricetick=Decimal("0.01"),
                min_volume=100,
            )
        ]

    def close(self) -> None:
        return None


def _wait_until(predicate, timeout: float = 2.0) -> None:  # type: ignore[no-untyped-def]
    deadline = monotonic() + timeout
    while monotonic() < deadline:
        if predicate():
            return
        sleep(0.05)
    raise AssertionError("condition not satisfied before timeout")


def test_trade_server_bootstrap_exposes_engine_and_module_state() -> None:
    config = TradeServerConfig(
        env="test",
        project_root=ROOT,
        gateway=GatewayRuntimeConfig(gateway_name="OPENCTPSEC"),
    )
    runtime = TradeServerBootstrap(config).bootstrap()
    try:
        snapshot = runtime.snapshot_health()
        assert {"email", "log", "oms"}.issubset(set(snapshot.engines))
        assert snapshot.gateway.name == "OPENCTPSEC"
        assert snapshot.gateway.registered
        assert all(
            module.status in {"loaded", "missing_optional", "disabled"}
            for module in snapshot.modules
        )
    finally:
        runtime.stop()


def test_trade_server_gateway_connects_with_mock_adapters() -> None:
    config = TradeServerConfig(
        env="test",
        project_root=ROOT,
        gateway=GatewayRuntimeConfig(gateway_name="OPENCTPSEC"),
    )
    runtime = TradeServerBootstrap(config).bootstrap()
    try:
        runtime.gateway.bind_adapters(MockMdAdapter(), MockTdAdapter())
        runtime.gateway.connect({"account_id": "ACC1"})
        runtime.gateway._drain_for_tests()
        runtime.main_engine.send_order(
            OrderRequest(
                symbol="600000",
                exchange=Exchange.SSE,
                direction=Direction.LONG,
                type=OrderType.LIMIT,
                volume=100,
                price=10.0,
                reference="trade-server-test",
            ),
            "OPENCTPSEC",
        )
        runtime.gateway._drain_for_tests()
        snapshot = runtime.snapshot_health()
        assert snapshot.gateway.connected
        assert snapshot.gateway.last_error is None
        assert runtime.main_engine.get_all_gateway_names() == ["OPENCTPSEC"]
        oms = runtime.main_engine.get_engine("oms")
        assert oms is not None
        _wait_until(lambda: bool(oms.orders) and bool(oms.trades))
        assert oms.orders
        assert oms.trades
    finally:
        runtime.stop()
