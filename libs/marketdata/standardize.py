"""Raw tick normalization and session-aware filtering."""

from __future__ import annotations

from datetime import date

from libs.marketdata.raw_store import (
    ensure_datetime_columns,
    read_partitioned_frame,
    require_parquet_support,
)
from libs.marketdata.schemas import SessionTag
from libs.rules_engine.market_rules import RulesRepository
from libs.schemas.master_data import Instrument
from libs.schemas.trading import TradingPhase


def load_raw_ticks(raw_root, *, trade_date: date, symbol: str | None = None, exchange: str | None = None):  # type: ignore[no-untyped-def]
    frame = read_partitioned_frame(raw_root, trade_date=trade_date, symbol=symbol, exchange=exchange)
    if frame.empty:
        return frame
    return ensure_datetime_columns(frame, ("exchange_ts", "received_ts", "recorded_at"))


def normalize_ticks(raw_frame, instrument: Instrument, rules_repo: RulesRepository, *, build_run_id: str):  # type: ignore[no-untyped-def]
    pd = require_parquet_support()
    if not hasattr(raw_frame, "empty"):
        raw_frame = pd.DataFrame(raw_frame)
    if raw_frame.empty:
        return pd.DataFrame()

    frame = raw_frame.copy()
    if frame["exchange_ts"].dtype == object:
        frame["exchange_ts"] = pd.to_datetime(frame["exchange_ts"])
    if frame["received_ts"].dtype == object:
        frame["received_ts"] = pd.to_datetime(frame["received_ts"])
    frame = ensure_datetime_columns(frame, ("exchange_ts", "received_ts"))
    frame = frame.sort_values(["exchange_ts", "received_ts", "raw_hash"]).drop_duplicates(
        subset=["raw_hash"], keep="first"
    )
    frame["phase"] = frame["exchange_ts"].map(
        lambda value: rules_repo.get_trading_phase(value, instrument).value
    )
    frame = filter_session_ticks(frame)
    if frame.empty:
        return frame

    frame["session_tag"] = frame["phase"].map(_phase_to_session_tag)
    volume_diff = frame["volume"].diff()
    turnover_diff = frame["turnover"].diff()
    frame["volume_delta"] = volume_diff.where(volume_diff >= 0, frame["volume"]).fillna(frame["volume"])
    frame["turnover_delta"] = turnover_diff.where(
        turnover_diff >= 0, frame["turnover"]
    ).fillna(frame["turnover"])
    frame["build_run_id"] = build_run_id
    columns = [
        "instrument_key",
        "symbol",
        "exchange",
        "vt_symbol",
        "exchange_ts",
        "received_ts",
        "last_price",
        "volume",
        "turnover",
        "volume_delta",
        "turnover_delta",
        "phase",
        "session_tag",
        "raw_hash",
        "build_run_id",
    ]
    return frame.loc[:, columns].reset_index(drop=True)


def filter_session_ticks(frame):  # type: ignore[no-untyped-def]
    pd = require_parquet_support()
    if not hasattr(frame, "empty"):
        frame = pd.DataFrame(frame)
    if frame.empty:
        return frame
    return frame.loc[frame["phase"] != TradingPhase.CLOSED.value].reset_index(drop=True)


def _phase_to_session_tag(phase: str) -> SessionTag:
    mapping = {
        TradingPhase.OPEN_CALL.value: SessionTag.AUCTION,
        TradingPhase.CLOSE_CALL.value: SessionTag.AUCTION,
        TradingPhase.CONTINUOUS_AM.value: SessionTag.AM,
        TradingPhase.CONTINUOUS_PM.value: SessionTag.PM,
        TradingPhase.AFTER_HOURS_FIXED.value: SessionTag.AFTER_HOURS,
    }
    return mapping[phase]
