from __future__ import annotations

import time
from pathlib import Path

from apps.trade_server.app.bootstrap import TradeServerBootstrap
from apps.trade_server.app.config import GatewayRuntimeConfig, TradeServerConfig
from apps.trade_server.app.recording.recorder_service import RecorderService
from apps.trade_server.app.recording.sinks import ParquetTickSink
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.raw_store import require_parquet_support
from libs.marketdata.samples import make_sample_ticks
from libs.marketdata.symbol_mapping import InstrumentCatalog

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"


def test_raw_recorder_writes_parquet_and_manifest(tmp_path: Path) -> None:
    pd = require_parquet_support()
    runtime = TradeServerBootstrap(
        TradeServerConfig(
            env="test",
            project_root=tmp_path,
            gateway=GatewayRuntimeConfig(gateway_name="OPENCTPSEC"),
        )
    ).bootstrap()
    try:
        manifest_store = ManifestStore(tmp_path / "data" / "manifests")
        recorder = RecorderService(
            event_engine=runtime.event_engine,
            project_root=tmp_path,
            instrument_catalog=InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR),
            sink=ParquetTickSink(
                project_root=tmp_path,
                raw_root=tmp_path / "data" / "raw" / "market_ticks",
                manifest_store=manifest_store,
            ),
            manifest_store=manifest_store,
            gateway_name="OPENCTPSEC",
            mode="test",
        )
        recorder.start("recording_test")
        for tick in make_sample_ticks()[:3]:
            runtime.gateway.on_tick(tick)
        time.sleep(0.5)
        recorder.stop()

        files = sorted((tmp_path / "data" / "raw" / "market_ticks").rglob("*.parquet"))
        assert files
        frame = pd.read_parquet(files[0])
        assert len(frame) == 3
        manifests = manifest_store.list_raw_file_manifests()
        assert len(manifests) == 1
        assert manifests[0].row_count == 3
    finally:
        runtime.stop()

