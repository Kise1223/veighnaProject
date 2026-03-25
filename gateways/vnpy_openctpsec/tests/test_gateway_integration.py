from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from time import monotonic, sleep

from gateways.vnpy_openctpsec.compat import (
    Direction,
    EventEngine,
    Exchange,
    MainEngine,
    OrderRequest,
    OrderType,
    SubscribeRequest,
)
from gateways.vnpy_openctpsec.gateway import OpenCTPSecGateway
from gateways.vnpy_openctpsec.mapper import (
    AdapterAccountEvent,
    AdapterContractEvent,
    AdapterOrderEvent,
    AdapterPositionEvent,
    AdapterTickEvent,
    AdapterTradeEvent,
)
from libs.common.time import CN_TZ


class MockMdAdapter:
    def set_listener(self, listener) -> None:  # type: ignore[no-untyped-def]
        self.listener = listener

    def connect(self, settings: dict[str, str]) -> None:
        self.listener.on_md_log("md connected")

    def subscribe(self, request: SubscribeRequest) -> None:
        self.listener.on_md_tick(
            AdapterTickEvent(
                symbol=request.symbol,
                exchange=request.exchange.value
                if hasattr(request.exchange, "value")
                else str(request.exchange),
                name=request.symbol,
                last_price=Decimal("10"),
                exchange_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
                received_ts=datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ),
            )
        )

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


def test_gateway_smoke_reaches_oms_engine() -> None:
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    try:
        gateway = main_engine.add_gateway(OpenCTPSecGateway, gateway_name="OPENCTPSEC")
        gateway.bind_adapters(MockMdAdapter(), MockTdAdapter())

        gateway.connect({"account_id": "ACC1"})
        gateway._drain_for_tests()
        gateway.subscribe(SubscribeRequest(symbol="600000", exchange=Exchange.SSE))
        gateway._drain_for_tests()
        gateway.send_order(
            OrderRequest(
                symbol="600000",
                exchange=Exchange.SSE,
                direction=Direction.LONG,
                type=OrderType.LIMIT,
                volume=100,
                price=Decimal("10"),
                reference="smoke",
            )
        )
        gateway._drain_for_tests()

        oms = main_engine.get_engine("oms")
        _wait_until(lambda: bool(oms.orders) and bool(oms.trades) and bool(oms.positions))
        assert any(contract.vt_symbol == "600000.SSE" for contract in oms.contracts.values())
        assert any(account.accountid == "ACC1" for account in oms.accounts.values())
        assert oms.positions
        assert oms.orders
        assert oms.trades
    finally:
        main_engine.close()
