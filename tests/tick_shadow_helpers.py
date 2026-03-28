from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from libs.common.time import CN_TZ


def make_tick_row(
    *,
    instrument_key: str,
    symbol: str,
    exchange: str,
    exchange_ts: datetime,
    last_price: str,
    bid_price_1: str | None = None,
    ask_price_1: str | None = None,
    bid_volume_1: str | None = None,
    ask_volume_1: str | None = None,
    raw_hash: str | None = None,
    received_ts: datetime | None = None,
    source_seq: str | None = None,
) -> dict[str, object]:
    tick_received_ts = received_ts or exchange_ts
    return {
        "instrument_key": instrument_key,
        "symbol": symbol,
        "exchange": exchange,
        "exchange_ts": exchange_ts.astimezone(CN_TZ).isoformat(),
        "received_ts": tick_received_ts.astimezone(CN_TZ).isoformat(),
        "last_price": last_price,
        "bid_price_1": bid_price_1,
        "ask_price_1": ask_price_1,
        "bid_volume_1": bid_volume_1,
        "ask_volume_1": ask_volume_1,
        "raw_hash": raw_hash or f"{instrument_key}_{exchange_ts.isoformat()}",
        "source_seq": source_seq,
    }


def write_tick_shadow_source(
    workspace: Path,
    *,
    trade_date: date,
    ticks: list[dict[str, object]],
    filename: str | None = None,
) -> Path:
    target_dir = workspace / "data" / "bootstrap" / "shadow_tick_sample"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / (filename or f"ticks_l1_{trade_date.isoformat()}.json")
    payload = {
        "trade_date": trade_date.isoformat(),
        "ticks": ticks,
    }
    target_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target_path
