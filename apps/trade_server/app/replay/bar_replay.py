"""Replay standardized bar parquet files into BarData events."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from gateways.vnpy_openctpsec.compat import BarData, Exchange, Interval
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import require_parquet_support


def replay_bars(path: Path, *, gateway_name: str = "REPLAY") -> list[BarData]:
    pd = require_parquet_support()
    frame = pd.read_parquet(path).sort_values(["bar_dt", "symbol"])
    interval = Interval.DAILY if "trade_date" in frame.columns else Interval.MINUTE
    events: list[BarData] = []
    for row in frame.to_dict(orient="records"):
        bar = BarData(
            gateway_name=gateway_name,
            symbol=row["symbol"],
            exchange=Exchange(row["exchange"]),
            datetime=ensure_cn_aware(_as_datetime(row["bar_dt"])),
            interval=interval,
            volume=float(row["volume"]),
            turnover=float(row["turnover"]),
            open_price=float(row["open"]),
            high_price=float(row["high"]),
            low_price=float(row["low"]),
            close_price=float(row["close"]),
        )
        bar_any: Any = bar
        bar_any.instrument_key = row["instrument_key"]
        events.append(bar)
    return events


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))
