"""Aggregations from standardized ticks to bars."""

from __future__ import annotations

from libs.common.time import CN_TZ
from libs.marketdata.raw_store import require_parquet_support


def build_1m_bars(standard_ticks, *, build_run_id: str):  # type: ignore[no-untyped-def]
    pd = require_parquet_support()
    if standard_ticks.empty:
        return pd.DataFrame()

    frame = standard_ticks.copy()
    frame["minute_bucket"] = frame["exchange_ts"].dt.floor("min")
    grouped = frame.groupby(
        ["instrument_key", "symbol", "exchange", "vt_symbol", "minute_bucket", "session_tag"],
        as_index=False,
    )
    bars = grouped.agg(
        open=("last_price", "first"),
        high=("last_price", "max"),
        low=("last_price", "min"),
        close=("last_price", "last"),
        volume=("volume_delta", "sum"),
        turnover=("turnover_delta", "sum"),
        trade_count=("last_price", "size"),
    )
    bars["vwap"] = bars.apply(
        lambda row: row["turnover"] / row["volume"] if row["volume"] > 0 else row["close"],
        axis=1,
    )
    bars["bar_dt"] = bars["minute_bucket"] + pd.Timedelta(minutes=1)
    bars["is_synthetic"] = False
    bars["build_run_id"] = build_run_id
    columns = [
        "instrument_key",
        "symbol",
        "exchange",
        "vt_symbol",
        "bar_dt",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "trade_count",
        "vwap",
        "session_tag",
        "is_synthetic",
        "build_run_id",
    ]
    return bars.loc[:, columns].sort_values(["bar_dt", "symbol"]).reset_index(drop=True)


def build_daily_bars_from_1m(bars_1m, *, build_run_id: str):  # type: ignore[no-untyped-def]
    pd = require_parquet_support()
    if bars_1m.empty:
        return pd.DataFrame()

    frame = bars_1m.copy()
    frame["trade_date"] = frame["bar_dt"].dt.date
    grouped = frame.groupby(
        ["instrument_key", "symbol", "exchange", "vt_symbol", "trade_date"],
        as_index=False,
    )
    daily = grouped.agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
        turnover=("turnover", "sum"),
        trade_count=("trade_count", "sum"),
    )
    daily["vwap"] = daily.apply(
        lambda row: row["turnover"] / row["volume"] if row["volume"] > 0 else row["close"],
        axis=1,
    )
    daily["bar_dt"] = daily["trade_date"].map(
        lambda trade_date: pd.Timestamp(trade_date).tz_localize(CN_TZ) + pd.Timedelta(hours=15)
    )
    daily["build_run_id"] = build_run_id
    columns = [
        "instrument_key",
        "symbol",
        "exchange",
        "vt_symbol",
        "trade_date",
        "bar_dt",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "trade_count",
        "vwap",
        "build_run_id",
    ]
    return daily.loc[:, columns].sort_values(["trade_date", "symbol"]).reset_index(drop=True)

