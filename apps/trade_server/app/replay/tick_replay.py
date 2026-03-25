"""Replay raw parquet files into TickData events."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from gateways.vnpy_openctpsec.compat import Exchange, TickData
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import require_parquet_support


def replay_ticks(path: Path, *, gateway_name: str = "REPLAY") -> list[TickData]:
    pd = require_parquet_support()
    frame = pd.read_parquet(path).sort_values(["exchange_ts", "received_ts", "raw_hash"])
    events: list[TickData] = []
    for row in frame.to_dict(orient="records"):
        exchange = Exchange(row["exchange"])
        tick = TickData(
            gateway_name=gateway_name,
            symbol=row["symbol"],
            exchange=exchange,
            datetime=ensure_cn_aware(row["received_ts"]),
            name=row["symbol"],
            last_price=float(row["last_price"]),
            volume=float(row.get("volume", 0.0) or 0.0),
            turnover=float(row.get("turnover", 0.0) or 0.0),
            open_interest=float(row.get("open_interest", 0.0) or 0.0),
            limit_up=_float_or_zero(row.get("limit_up")),
            limit_down=_float_or_zero(row.get("limit_down")),
            bid_price_1=_float_or_zero(row.get("bid_price_1")),
            bid_price_2=_float_or_zero(row.get("bid_price_2")),
            bid_price_3=_float_or_zero(row.get("bid_price_3")),
            bid_price_4=_float_or_zero(row.get("bid_price_4")),
            bid_price_5=_float_or_zero(row.get("bid_price_5")),
            ask_price_1=_float_or_zero(row.get("ask_price_1")),
            ask_price_2=_float_or_zero(row.get("ask_price_2")),
            ask_price_3=_float_or_zero(row.get("ask_price_3")),
            ask_price_4=_float_or_zero(row.get("ask_price_4")),
            ask_price_5=_float_or_zero(row.get("ask_price_5")),
            bid_volume_1=_float_or_zero(row.get("bid_volume_1")),
            bid_volume_2=_float_or_zero(row.get("bid_volume_2")),
            bid_volume_3=_float_or_zero(row.get("bid_volume_3")),
            bid_volume_4=_float_or_zero(row.get("bid_volume_4")),
            bid_volume_5=_float_or_zero(row.get("bid_volume_5")),
            ask_volume_1=_float_or_zero(row.get("ask_volume_1")),
            ask_volume_2=_float_or_zero(row.get("ask_volume_2")),
            ask_volume_3=_float_or_zero(row.get("ask_volume_3")),
            ask_volume_4=_float_or_zero(row.get("ask_volume_4")),
            ask_volume_5=_float_or_zero(row.get("ask_volume_5")),
        )
        setattr(tick, "exchange_ts", ensure_cn_aware(_as_datetime(row["exchange_ts"])))
        setattr(tick, "received_ts", ensure_cn_aware(_as_datetime(row["received_ts"])))
        setattr(tick, "instrument_key", row["instrument_key"])
        setattr(tick, "source_seq", row.get("source_seq"))
        events.append(tick)
    return events


def _float_or_zero(value: object) -> float:
    if value is None:
        return 0.0
    return float(str(value))


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
