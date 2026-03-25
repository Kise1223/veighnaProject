"""Small deterministic samples for M4 smoke tests and unit tests."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from gateways.vnpy_openctpsec.compat import Exchange, TickData
from libs.common.time import CN_TZ


def make_sample_ticks(
    *,
    symbol: str = "600000",
    exchange: Exchange = Exchange.SSE,
    gateway_name: str = "OPENCTPSEC",
    include_duplicate: bool = False,
    include_out_of_session: bool = False,
) -> list[TickData]:
    samples = [
        _tick(
            symbol=symbol,
            exchange=exchange,
            gateway_name=gateway_name,
            exchange_ts=datetime(2026, 3, 25, 9, 30, 1, tzinfo=CN_TZ),
            received_ts=datetime(2026, 3, 25, 9, 30, 1, 500000, tzinfo=CN_TZ),
            last_price=Decimal("10.00"),
            volume=Decimal("100"),
            turnover=Decimal("1000"),
        ),
        _tick(
            symbol=symbol,
            exchange=exchange,
            gateway_name=gateway_name,
            exchange_ts=datetime(2026, 3, 25, 9, 30, 20, tzinfo=CN_TZ),
            received_ts=datetime(2026, 3, 25, 9, 30, 20, 500000, tzinfo=CN_TZ),
            last_price=Decimal("10.20"),
            volume=Decimal("150"),
            turnover=Decimal("1510"),
        ),
        _tick(
            symbol=symbol,
            exchange=exchange,
            gateway_name=gateway_name,
            exchange_ts=datetime(2026, 3, 25, 9, 31, 5, tzinfo=CN_TZ),
            received_ts=datetime(2026, 3, 25, 9, 31, 5, 500000, tzinfo=CN_TZ),
            last_price=Decimal("10.10"),
            volume=Decimal("220"),
            turnover=Decimal("2217"),
        ),
        _tick(
            symbol=symbol,
            exchange=exchange,
            gateway_name=gateway_name,
            exchange_ts=datetime(2026, 3, 26, 9, 30, 1, tzinfo=CN_TZ),
            received_ts=datetime(2026, 3, 26, 9, 30, 1, 500000, tzinfo=CN_TZ),
            last_price=Decimal("5.10"),
            volume=Decimal("200"),
            turnover=Decimal("1020"),
        ),
        _tick(
            symbol=symbol,
            exchange=exchange,
            gateway_name=gateway_name,
            exchange_ts=datetime(2026, 3, 26, 9, 30, 40, tzinfo=CN_TZ),
            received_ts=datetime(2026, 3, 26, 9, 30, 40, 500000, tzinfo=CN_TZ),
            last_price=Decimal("5.20"),
            volume=Decimal("260"),
            turnover=Decimal("1332"),
        ),
        _tick(
            symbol=symbol,
            exchange=exchange,
            gateway_name=gateway_name,
            exchange_ts=datetime(2026, 3, 26, 9, 31, 20, tzinfo=CN_TZ),
            received_ts=datetime(2026, 3, 26, 9, 31, 20, 500000, tzinfo=CN_TZ),
            last_price=Decimal("5.25"),
            volume=Decimal("320"),
            turnover=Decimal("1647"),
        ),
    ]
    if include_duplicate:
        samples.append(samples[1])
    if include_out_of_session:
        samples.append(
            _tick(
                symbol=symbol,
                exchange=exchange,
                gateway_name=gateway_name,
                exchange_ts=datetime(2026, 3, 25, 12, 0, 0, tzinfo=CN_TZ),
                received_ts=datetime(2026, 3, 25, 12, 0, 0, 500000, tzinfo=CN_TZ),
                last_price=Decimal("10.05"),
                volume=Decimal("221"),
                turnover=Decimal("2227"),
            )
        )
    return samples


def _tick(
    *,
    symbol: str,
    exchange: Exchange,
    gateway_name: str,
    exchange_ts: datetime,
    received_ts: datetime,
    last_price: Decimal,
    volume: Decimal,
    turnover: Decimal,
) -> TickData:
    tick = TickData(
        gateway_name=gateway_name,
        symbol=symbol,
        exchange=exchange,
        datetime=received_ts,
        name=symbol,
        last_price=float(last_price),
        volume=float(volume),
        turnover=float(turnover),
        bid_price_1=float(last_price - Decimal("0.01")),
        ask_price_1=float(last_price + Decimal("0.01")),
        bid_volume_1=100.0,
        ask_volume_1=100.0,
        limit_up=float(last_price * Decimal("1.10")),
        limit_down=float(last_price * Decimal("0.90")),
    )
    setattr(tick, "exchange_ts", exchange_ts)
    setattr(tick, "received_ts", received_ts)
    return tick
