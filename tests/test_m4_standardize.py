from __future__ import annotations

from pathlib import Path

from libs.marketdata.bars import build_1m_bars, build_daily_bars_from_1m
from libs.marketdata.raw_store import write_partition_frame
from libs.marketdata.samples import make_sample_ticks
from libs.marketdata.standardize import load_raw_ticks, normalize_ticks
from libs.marketdata.symbol_mapping import InstrumentCatalog
from tests.bootstrap_helpers import bootstrap_rules

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"


def test_duplicate_ticks_removed_and_session_filter_applied(tmp_path: Path) -> None:
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    resolved = catalog.resolve(instrument_key="EQ_SH_600000")
    raw_rows = []
    recorder_ticks = make_sample_ticks()[:3]
    recorder_ticks.append(recorder_ticks[1])
    recorder_ticks.extend(make_sample_ticks(include_out_of_session=True)[-1:])
    for tick in recorder_ticks:
        raw_rows.append(
            {
                "instrument_key": resolved.mapping.instrument_key,
                "symbol": resolved.mapping.symbol,
                "exchange": resolved.mapping.exchange.value,
                "vt_symbol": resolved.mapping.vt_symbol,
                "gateway_name": tick.gateway_name,
                "exchange_ts": tick.exchange_ts,
                "received_ts": tick.received_ts,
                "last_price": tick.last_price,
                "volume": tick.volume,
                "turnover": tick.turnover,
                "open_interest": 0.0,
                "bid_price_1": tick.bid_price_1,
                "bid_price_2": None,
                "bid_price_3": None,
                "bid_price_4": None,
                "bid_price_5": None,
                "ask_price_1": tick.ask_price_1,
                "ask_price_2": None,
                "ask_price_3": None,
                "ask_price_4": None,
                "ask_price_5": None,
                "bid_volume_1": tick.bid_volume_1,
                "bid_volume_2": None,
                "bid_volume_3": None,
                "bid_volume_4": None,
                "bid_volume_5": None,
                "ask_volume_1": tick.ask_volume_1,
                "ask_volume_2": None,
                "ask_volume_3": None,
                "ask_volume_4": None,
                "ask_volume_5": None,
                "limit_up": tick.limit_up,
                "limit_down": tick.limit_down,
                "source_seq": None,
                "raw_hash": f"{tick.exchange_ts.isoformat()}_{tick.last_price}_{tick.volume}",
                "recorded_at": tick.received_ts,
            }
        )
    write_partition_frame(
        raw_rows,
        base_dir=tmp_path / "data" / "raw" / "market_ticks",
        trade_date=resolved.instrument.list_date.replace(year=2026, month=3, day=25),
        exchange=resolved.mapping.exchange.value,
        symbol=resolved.mapping.symbol,
        file_stem="raw_sample",
    )
    raw_frame = load_raw_ticks(
        tmp_path / "data" / "raw" / "market_ticks",
        trade_date=resolved.instrument.list_date.replace(year=2026, month=3, day=25),
        symbol=resolved.mapping.symbol,
        exchange=resolved.mapping.exchange.value,
    )
    normalized = normalize_ticks(
        raw_frame,
        resolved.instrument,
        bootstrap_rules(),
        build_run_id="build_test",
    )
    assert len(normalized) == 3
    assert normalized["session_tag"].tolist() == ["am", "am", "am"]


def test_1m_ohlcv_and_daily_bar_build_are_correct() -> None:
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    resolved = catalog.resolve(instrument_key="EQ_SH_600000")
    raw_rows = []
    for tick in make_sample_ticks()[:3]:
        raw_rows.append(
            {
                "instrument_key": resolved.mapping.instrument_key,
                "symbol": resolved.mapping.symbol,
                "exchange": resolved.mapping.exchange.value,
                "vt_symbol": resolved.mapping.vt_symbol,
                "gateway_name": tick.gateway_name,
                "exchange_ts": tick.exchange_ts,
                "received_ts": tick.received_ts,
                "last_price": tick.last_price,
                "volume": tick.volume,
                "turnover": tick.turnover,
                "raw_hash": f"{tick.exchange_ts.isoformat()}_{tick.last_price}_{tick.volume}",
                "recorded_at": tick.received_ts,
            }
        )
    normalized = normalize_ticks(raw_rows, resolved.instrument, bootstrap_rules(), build_run_id="build_test")
    bars_1m = build_1m_bars(normalized, build_run_id="build_test")
    assert len(bars_1m) == 2
    first_bar = bars_1m.iloc[0]
    assert first_bar["open"] == 10.0
    assert first_bar["high"] == 10.2
    assert first_bar["low"] == 10.0
    assert first_bar["close"] == 10.2
    assert first_bar["volume"] == 150.0
    assert first_bar["turnover"] == 1510.0
    assert first_bar["trade_count"] == 2

    bars_1d = build_daily_bars_from_1m(bars_1m, build_run_id="build_test")
    assert len(bars_1d) == 1
    assert bars_1d.iloc[0]["close"] == 10.1
