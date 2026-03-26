from __future__ import annotations

from pathlib import Path

from libs.marketdata.adjustments import build_adjustment_factors
from libs.marketdata.corporate_actions import load_corporate_actions
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.qlib_export import export_qlib_provider, qlib_smoke_read
from libs.marketdata.raw_store import write_partition_frame
from libs.marketdata.symbol_mapping import InstrumentCatalog

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"
CORPORATE_ACTIONS = ROOT / "data" / "marketdata" / "bootstrap" / "corporate_actions.json"


def test_sample_dividend_and_split_adjustments_are_correct() -> None:
    pd = __import__("pandas")
    actions = load_corporate_actions(CORPORATE_ACTIONS)

    dividend_bars = pd.DataFrame(
        [
            {"instrument_key": "EQ_SZ_000001", "trade_date": pd.Timestamp("2026-03-25").date(), "close": 10.0},
            {"instrument_key": "EQ_SZ_000001", "trade_date": pd.Timestamp("2026-03-26").date(), "close": 9.8},
        ]
    )
    dividend_factors = build_adjustment_factors(
        dividend_bars,
        [action for action in actions if action.instrument_key == "EQ_SZ_000001"],
        source_run_id="test",
    )
    assert dividend_factors.iloc[0]["adj_factor"] == 0.98

    split_bars = pd.DataFrame(
        [
            {"instrument_key": "EQ_SH_600000", "trade_date": pd.Timestamp("2026-03-25").date(), "close": 10.0},
            {"instrument_key": "EQ_SH_600000", "trade_date": pd.Timestamp("2026-03-26").date(), "close": 5.1},
        ]
    )
    split_factors = build_adjustment_factors(
        split_bars,
        [action for action in actions if action.instrument_key == "EQ_SH_600000"],
        source_run_id="test",
    )
    assert split_factors.iloc[0]["adj_factor"] == 0.5


def test_qlib_export_smoke_is_readable_for_1d_and_1min(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    manifest_store = ManifestStore(tmp_path / "data" / "manifests")

    bars_1m = [
        {
            "instrument_key": "EQ_SH_600000",
            "symbol": "600000",
            "exchange": "SSE",
            "vt_symbol": "600000.SSE",
            "bar_dt": "2026-03-25T09:31:00+08:00",
            "open": 10.0,
            "high": 10.2,
            "low": 10.0,
            "close": 10.2,
            "volume": 150.0,
            "turnover": 1510.0,
            "trade_count": 2,
            "vwap": 10.0667,
            "session_tag": "am",
            "is_synthetic": False,
            "build_run_id": "build_test",
        },
        {
            "instrument_key": "EQ_SH_600000",
            "symbol": "600000",
            "exchange": "SSE",
            "vt_symbol": "600000.SSE",
            "bar_dt": "2026-03-26T09:31:00+08:00",
            "open": 5.1,
            "high": 5.2,
            "low": 5.1,
            "close": 5.2,
            "volume": 260.0,
            "turnover": 1332.0,
            "trade_count": 2,
            "vwap": 5.1231,
            "session_tag": "am",
            "is_synthetic": False,
            "build_run_id": "build_test",
        },
    ]
    bars_1d = [
        {
            "instrument_key": "EQ_SH_600000",
            "symbol": "600000",
            "exchange": "SSE",
            "vt_symbol": "600000.SSE",
            "trade_date": "2026-03-25",
            "bar_dt": "2026-03-25T15:00:00+08:00",
            "open": 10.0,
            "high": 10.2,
            "low": 10.0,
            "close": 10.1,
            "volume": 220.0,
            "turnover": 2217.0,
            "trade_count": 3,
            "vwap": 10.0773,
            "build_run_id": "build_test",
        },
        {
            "instrument_key": "EQ_SH_600000",
            "symbol": "600000",
            "exchange": "SSE",
            "vt_symbol": "600000.SSE",
            "trade_date": "2026-03-26",
            "bar_dt": "2026-03-26T15:00:00+08:00",
            "open": 5.1,
            "high": 5.25,
            "low": 5.1,
            "close": 5.25,
            "volume": 320.0,
            "turnover": 1647.0,
            "trade_count": 3,
            "vwap": 5.1469,
            "build_run_id": "build_test",
        },
    ]
    factors = [
        {
            "instrument_key": "EQ_SH_600000",
            "trade_date": "2026-03-25",
            "adj_factor": 0.5,
            "adj_mode": "forward",
            "source_run_id": "build_test",
        },
        {
            "instrument_key": "EQ_SH_600000",
            "trade_date": "2026-03-26",
            "adj_factor": 1.0,
            "adj_mode": "forward",
            "source_run_id": "build_test",
        },
    ]
    write_partition_frame(
        bars_1m,
        base_dir=tmp_path / "data" / "standard" / "bars_1m",
        trade_date=__import__("datetime").date(2026, 3, 25),
        exchange="SSE",
        symbol="600000",
        file_stem="bars_1m",
    )
    write_partition_frame(
        bars_1d,
        base_dir=tmp_path / "data" / "standard" / "bars_1d",
        trade_date=__import__("datetime").date(2026, 3, 25),
        exchange="SSE",
        symbol="600000",
        file_stem="bars_1d",
    )
    write_partition_frame(
        factors,
        base_dir=tmp_path / "data" / "standard" / "adjustment_factors",
        trade_date=__import__("datetime").date(2026, 3, 26),
        exchange="SSE",
        symbol="600000",
        file_stem="adjustment",
    )

    export_qlib_provider(
        project_root=tmp_path,
        provider_root=tmp_path / "data" / "qlib_bin",
        catalog=catalog,
        manifest_store=manifest_store,
        freq="1d",
        build_run_id="build_test_day",
    )
    export_qlib_provider(
        project_root=tmp_path,
        provider_root=tmp_path / "data" / "qlib_bin",
        catalog=catalog,
        manifest_store=manifest_store,
        freq="1min",
        build_run_id="build_test_1min",
    )

    day_data = qlib_smoke_read(
        provider_root=tmp_path / "data" / "qlib_bin",
        qlib_symbol="SH600000",
        freq="1d",
    )
    min_data = qlib_smoke_read(
        provider_root=tmp_path / "data" / "qlib_bin",
        qlib_symbol="SH600000",
        freq="1min",
    )
    assert not day_data.empty
    assert not min_data.empty


def test_qlib_smoke_read_uses_provider_calendar_window(tmp_path: Path, monkeypatch) -> None:
    pytest = __import__("pytest")
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    manifest_store = ManifestStore(tmp_path / "data" / "manifests")
    bars_1d = [
        {
            "instrument_key": "EQ_SH_600000",
            "symbol": "600000",
            "exchange": "SSE",
            "vt_symbol": "600000.SSE",
            "trade_date": "2026-04-07",
            "bar_dt": "2026-04-07T15:00:00+08:00",
            "open": 10.0,
            "high": 10.2,
            "low": 9.9,
            "close": 10.1,
            "volume": 100.0,
            "turnover": 1005.0,
            "trade_count": 1,
            "vwap": 10.05,
            "build_run_id": "build_test_window",
        },
        {
            "instrument_key": "EQ_SH_600000",
            "symbol": "600000",
            "exchange": "SSE",
            "vt_symbol": "600000.SSE",
            "trade_date": "2026-04-08",
            "bar_dt": "2026-04-08T15:00:00+08:00",
            "open": 10.1,
            "high": 10.3,
            "low": 10.0,
            "close": 10.2,
            "volume": 120.0,
            "turnover": 1220.0,
            "trade_count": 1,
            "vwap": 10.1667,
            "build_run_id": "build_test_window",
        },
    ]
    write_partition_frame(
        bars_1d,
        base_dir=tmp_path / "data" / "standard" / "bars_1d",
        trade_date=__import__("datetime").date(2026, 4, 7),
        exchange="SSE",
        symbol="600000",
        file_stem="bars_1d_window",
    )
    export_qlib_provider(
        project_root=tmp_path,
        provider_root=tmp_path / "data" / "qlib_bin",
        catalog=catalog,
        manifest_store=manifest_store,
        freq="1d",
        build_run_id="build_test_window",
    )
    day_data = qlib_smoke_read(
        provider_root=tmp_path / "data" / "qlib_bin",
        qlib_symbol="SH600000",
        freq="1d",
    )
    assert list(day_data.index.get_level_values("datetime").strftime("%Y-%m-%d")) == ["2026-04-07", "2026-04-08"]
