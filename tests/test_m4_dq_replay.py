from __future__ import annotations

from pathlib import Path

from apps.trade_server.app.recording.recorder_service import RecorderService
from apps.trade_server.app.recording.sinks import ParquetTickSink
from apps.trade_server.app.replay.bar_replay import replay_bars
from apps.trade_server.app.replay.tick_replay import replay_ticks
from libs.marketdata.bars import build_1m_bars
from libs.marketdata.dq import evaluate_raw_tick_dq, write_dq_report
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.raw_store import require_parquet_support, write_partition_frame
from libs.marketdata.samples import make_sample_ticks
from libs.marketdata.standardize import normalize_ticks
from libs.marketdata.symbol_mapping import InstrumentCatalog
from tests.bootstrap_helpers import bootstrap_rules

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"


class DummyEventEngine:
    def register(self, event_type: str, handler) -> None:  # type: ignore[no-untyped-def]
        return None


def test_dq_detects_out_of_session_and_replay_preserves_order(tmp_path: Path) -> None:
    pd = require_parquet_support()
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    manifest_store = ManifestStore(tmp_path / "data" / "manifests")
    recorder = RecorderService(
        event_engine=DummyEventEngine(),
        project_root=tmp_path,
        instrument_catalog=catalog,
        sink=ParquetTickSink(
            project_root=tmp_path,
            raw_root=tmp_path / "data" / "raw" / "market_ticks",
            manifest_store=manifest_store,
        ),
        manifest_store=manifest_store,
        gateway_name="OPENCTPSEC",
        mode="test",
    )
    recorder.start("dq_test")
    for tick in make_sample_ticks(include_out_of_session=True):
        recorder.record_tick(tick)
    recorder.stop()

    raw_file = next((tmp_path / "data" / "raw" / "market_ticks").rglob("*.parquet"))
    raw_frame = pd.read_parquet(raw_file)
    issues = evaluate_raw_tick_dq(raw_frame, catalog=catalog, rules_repo=bootstrap_rules())
    assert any(issue.code == "out_of_session" for issue in issues)
    report = write_dq_report(
        project_root=tmp_path,
        report_root=tmp_path / "data" / "dq_reports",
        manifest_store=manifest_store,
        layer="raw_ticks",
        trade_date=__import__("datetime").date(2026, 3, 25),
        scope="trade_date=2026-03-25",
        issues=issues,
    )
    assert report.issue_count >= 1

    tick_events = replay_ticks(raw_file)
    assert [tick.exchange_ts for tick in tick_events] == sorted(tick.exchange_ts for tick in tick_events)

    resolved = catalog.resolve(instrument_key="EQ_SH_600000")
    normalized = normalize_ticks(raw_frame, resolved.instrument, bootstrap_rules(), build_run_id="build_test")
    bars_1m = build_1m_bars(normalized, build_run_id="build_test")
    write_partition_frame(
        bars_1m.to_dict(orient="records"),
        base_dir=tmp_path / "data" / "standard" / "bars_1m",
        trade_date=__import__("datetime").date(2026, 3, 25),
        exchange="SSE",
        symbol="600000",
        file_stem="bars_replay",
    )
    bar_file = next((tmp_path / "data" / "standard" / "bars_1m").rglob("*.parquet"))
    bar_events = replay_bars(bar_file)
    assert bar_events
    assert bar_events[0].open_price == 10.0
