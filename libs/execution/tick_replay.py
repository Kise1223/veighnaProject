"""Deterministic tick-replay helpers for M9 shadow sessions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from libs.common.time import ensure_cn_aware
from libs.execution.fill_model import FillDecision
from libs.execution.schemas import PaperOrderStatus
from libs.marketdata.raw_store import (
    file_sha256,
    list_partition_files,
    read_partitioned_frame,
    relative_path,
    require_parquet_support,
    stable_hash,
)

DEFAULT_TICK_SAMPLE_ROOT = Path("data/bootstrap/shadow_tick_sample")


@dataclass(frozen=True)
class TickReplayEvent:
    instrument_key: str
    event_dt: datetime
    received_dt: datetime
    raw_hash: str
    source_seq: str | None
    row: dict[str, object]


@dataclass(frozen=True)
class TickReplaySource:
    frames_by_instrument: dict[str, Any]
    tick_source_hash: str
    source_label: str


@dataclass(frozen=True)
class TickLiquidity:
    fill_dt: datetime
    fill_price: Decimal
    available_quantity: int
    used_fallback: bool


def resolve_tick_replay_source(
    *,
    project_root: Path,
    trade_date: date,
    wanted_instruments: set[tuple[str, str, str]],
    tick_input_path: Path | None,
) -> TickReplaySource:
    if tick_input_path is not None:
        return _load_tick_source(
            project_root=project_root,
            trade_date=trade_date,
            wanted_instruments=wanted_instruments,
            source_path=tick_input_path,
        )
    default_path = (
        project_root
        / DEFAULT_TICK_SAMPLE_ROOT
        / f"ticks_l1_{trade_date.isoformat()}.json"
    )
    if default_path.exists():
        return _load_tick_source(
            project_root=project_root,
            trade_date=trade_date,
            wanted_instruments=wanted_instruments,
            source_path=default_path,
        )
    return _load_tick_source_from_raw(
        project_root=project_root,
        trade_date=trade_date,
        wanted_instruments=wanted_instruments,
        base_dir=project_root / "data" / "raw" / "market_ticks",
    )


def collect_tick_replay_events(frames_by_instrument: dict[str, Any]) -> list[TickReplayEvent]:
    events: list[TickReplayEvent] = []
    for instrument_key, frame in frames_by_instrument.items():
        if frame is None or frame.empty:
            continue
        for row in frame.to_dict(orient="records"):
            event_dt = _as_datetime(row["exchange_ts"])
            received_dt = _as_datetime(row["received_ts"])
            events.append(
                TickReplayEvent(
                    instrument_key=instrument_key,
                    event_dt=event_dt,
                    received_dt=received_dt,
                    raw_hash=str(row["raw_hash"]),
                    source_seq=_as_optional_str(row.get("source_seq")),
                    row=row,
                )
            )
    return sorted(
        events,
        key=lambda item: (
            item.event_dt,
            item.received_dt,
            item.source_seq or "",
            item.raw_hash,
            item.instrument_key,
        ),
    )


def simulate_limit_fill_on_tick(
    *,
    side: str,
    limit_price: Decimal,
    tick_row: dict[str, object],
    tick_price_fallback: str,
) -> FillDecision | None:
    tick_dt = _as_datetime(tick_row["exchange_ts"])
    last_price = _as_optional_decimal(tick_row.get("last_price"))
    if side == "BUY":
        best_quote = _as_positive_decimal(tick_row.get("ask_price_1"))
        if best_quote is not None and best_quote <= limit_price:
            return FillDecision(
                status=PaperOrderStatus.FILLED,
                fill_bar_dt=tick_dt,
                fill_price=best_quote,
            )
        if (
            best_quote is None
            and tick_price_fallback == "last_price"
            and last_price is not None
            and last_price <= limit_price
        ):
            return FillDecision(
                status=PaperOrderStatus.FILLED,
                fill_bar_dt=tick_dt,
                fill_price=last_price,
            )
        return None

    best_quote = _as_positive_decimal(tick_row.get("bid_price_1"))
    if best_quote is not None and best_quote >= limit_price:
        return FillDecision(
            status=PaperOrderStatus.FILLED,
            fill_bar_dt=tick_dt,
            fill_price=best_quote,
        )
    if (
        best_quote is None
        and tick_price_fallback == "last_price"
        and last_price is not None
        and last_price >= limit_price
    ):
        return FillDecision(
            status=PaperOrderStatus.FILLED,
            fill_bar_dt=tick_dt,
            fill_price=last_price,
        )
    return None


def resolve_tick_liquidity(
    *,
    side: str,
    tick_row: dict[str, object],
    tick_price_fallback: str,
) -> TickLiquidity | None:
    tick_dt = _as_datetime(tick_row["exchange_ts"])
    last_price = _as_positive_decimal(tick_row.get("last_price"))
    if side == "BUY":
        best_quote = _as_positive_decimal(tick_row.get("ask_price_1"))
        quote_volume = _as_positive_quantity(tick_row.get("ask_volume_1"))
        if best_quote is not None and quote_volume > 0:
            return TickLiquidity(
                fill_dt=tick_dt,
                fill_price=best_quote,
                available_quantity=quote_volume,
                used_fallback=False,
            )
        if (
            best_quote is None
            and tick_price_fallback == "last_price"
            and last_price is not None
            and quote_volume > 0
        ):
            return TickLiquidity(
                fill_dt=tick_dt,
                fill_price=last_price,
                available_quantity=quote_volume,
                used_fallback=True,
            )
        return None

    best_quote = _as_positive_decimal(tick_row.get("bid_price_1"))
    quote_volume = _as_positive_quantity(tick_row.get("bid_volume_1"))
    if best_quote is not None and quote_volume > 0:
        return TickLiquidity(
            fill_dt=tick_dt,
            fill_price=best_quote,
            available_quantity=quote_volume,
            used_fallback=False,
        )
    if (
        best_quote is None
        and tick_price_fallback == "last_price"
        and last_price is not None
        and quote_volume > 0
    ):
        return TickLiquidity(
            fill_dt=tick_dt,
            fill_price=last_price,
            available_quantity=quote_volume,
            used_fallback=True,
        )
    return None

def last_tick_price(frame: Any) -> Decimal | None:
    if frame is None or frame.empty:
        return None
    last_row = frame.sort_values(["exchange_ts", "received_ts", "raw_hash"]).iloc[-1]
    return _as_optional_decimal(last_row.get("last_price"))


def _load_tick_source(
    *,
    project_root: Path,
    trade_date: date,
    wanted_instruments: set[tuple[str, str, str]],
    source_path: Path,
) -> TickReplaySource:
    if not source_path.exists():
        raise FileNotFoundError(f"tick input path not found: {source_path}")
    frame = _read_tick_frame(source_path)
    normalized = _normalize_tick_frame(frame, trade_date=trade_date)
    filtered = _filter_tick_frame(normalized, wanted_instruments=wanted_instruments)
    return TickReplaySource(
        frames_by_instrument=_group_frames_by_instrument(filtered, wanted_instruments=wanted_instruments),
        tick_source_hash=stable_hash(
            {
                "path": relative_path(project_root, source_path),
                "file_hash": file_sha256(source_path),
            }
        ),
        source_label=relative_path(project_root, source_path),
    )


def _load_tick_source_from_raw(
    *,
    project_root: Path,
    trade_date: date,
    wanted_instruments: set[tuple[str, str, str]],
    base_dir: Path,
) -> TickReplaySource:
    pd = require_parquet_support()
    files: list[Path] = []
    frames = []
    for _instrument_key, symbol, exchange in sorted(wanted_instruments):
        symbol_files = list_partition_files(
            base_dir,
            trade_date=trade_date,
            exchange=exchange,
            symbol=symbol,
        )
        files.extend(symbol_files)
        if symbol_files:
            frames.append(
                read_partitioned_frame(
                    base_dir,
                    trade_date=trade_date,
                    exchange=exchange,
                    symbol=symbol,
                )
            )
    if frames:
        frame = pd.concat(frames, ignore_index=True)
    else:
        frame = pd.DataFrame()
    normalized = _normalize_tick_frame(frame, trade_date=trade_date)
    filtered = _filter_tick_frame(normalized, wanted_instruments=wanted_instruments)
    return TickReplaySource(
        frames_by_instrument=_group_frames_by_instrument(filtered, wanted_instruments=wanted_instruments),
        tick_source_hash=stable_hash(
            {
                "files": [
                    {
                        "path": relative_path(project_root, path),
                        "file_hash": file_sha256(path),
                    }
                    for path in sorted(set(files))
                ]
            }
        ),
        source_label="data/raw/market_ticks",
    )


def _read_tick_frame(source_path: Path) -> Any:
    pd = require_parquet_support()
    if source_path.is_dir():
        return read_partitioned_frame(source_path)
    if source_path.suffix.lower() == ".json":
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        rows = payload["ticks"] if isinstance(payload, dict) and "ticks" in payload else payload
        return pd.DataFrame(rows)
    if source_path.suffix.lower() == ".parquet":
        return pd.read_parquet(source_path)
    raise ValueError(f"unsupported tick input path: {source_path}")


def _normalize_tick_frame(frame: Any, *, trade_date: date) -> Any:
    pd = require_parquet_support()
    if frame is None or frame.empty:
        return pd.DataFrame(
            columns=[
                "instrument_key",
                "symbol",
                "exchange",
                "exchange_ts",
                "received_ts",
                "last_price",
                "bid_price_1",
                "ask_price_1",
                "bid_volume_1",
                "ask_volume_1",
                "raw_hash",
                "source_seq",
            ]
        )
    normalized = frame.copy()
    for column in (
        "bid_price_1",
        "ask_price_1",
        "last_price",
        "bid_volume_1",
        "ask_volume_1",
    ):
        if column not in normalized.columns:
            normalized[column] = None
    if "received_ts" not in normalized.columns:
        normalized["received_ts"] = normalized["exchange_ts"]
    if "source_seq" not in normalized.columns:
        normalized["source_seq"] = None
    normalized["exchange_ts"] = normalized["exchange_ts"].map(_as_datetime)
    normalized["received_ts"] = normalized["received_ts"].map(_as_datetime)
    if "raw_hash" not in normalized.columns:
        normalized["raw_hash"] = normalized.apply(
            lambda row: stable_hash(
                {
                    "instrument_key": str(row["instrument_key"]),
                    "exchange_ts": row["exchange_ts"].isoformat(),
                    "received_ts": row["received_ts"].isoformat(),
                    "last_price": str(row["last_price"]),
                    "bid_price_1": str(row["bid_price_1"]),
                    "ask_price_1": str(row["ask_price_1"]),
                    "bid_volume_1": str(row["bid_volume_1"]),
                    "ask_volume_1": str(row["ask_volume_1"]),
                }
            ),
            axis=1,
        )
    normalized = normalized[
        normalized["exchange_ts"].map(lambda value: ensure_cn_aware(value).date()) == trade_date
    ].copy()
    return normalized.sort_values(
        ["exchange_ts", "received_ts", "source_seq", "raw_hash", "symbol"],
        na_position="last",
    ).reset_index(drop=True)


def _filter_tick_frame(frame: Any, *, wanted_instruments: set[tuple[str, str, str]]) -> Any:
    if frame.empty:
        return frame
    wanted_keys = {instrument_key for instrument_key, _symbol, _exchange in wanted_instruments}
    return frame[frame["instrument_key"].astype(str).isin(wanted_keys)].copy()


def _group_frames_by_instrument(
    frame: Any,
    *,
    wanted_instruments: set[tuple[str, str, str]],
) -> dict[str, Any]:
    pd = require_parquet_support()
    grouped: dict[str, Any] = {
        instrument_key: pd.DataFrame(columns=frame.columns)
        for instrument_key, _symbol, _exchange in wanted_instruments
    }
    if frame.empty:
        return grouped
    for instrument_key, group in frame.groupby("instrument_key", sort=True):
        grouped[str(instrument_key)] = group.copy().reset_index(drop=True)
    return grouped


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return ensure_cn_aware(value)
    return ensure_cn_aware(datetime.fromisoformat(str(value)))


def _as_optional_decimal(value: object | None) -> Decimal | None:
    if value is None or str(value) == "" or str(value).lower() == "nan":
        return None
    return Decimal(str(value))


def _as_positive_decimal(value: object | None) -> Decimal | None:
    decimal_value = _as_optional_decimal(value)
    if decimal_value is None or decimal_value <= 0:
        return None
    return decimal_value


def _as_optional_str(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _as_positive_quantity(value: object | None) -> int:
    decimal_value = _as_optional_decimal(value)
    if decimal_value is None or decimal_value <= 0:
        return 0
    return int(decimal_value)
