"""Smoke the M4 recorder path with deterministic sample ticks."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from apps.trade_server.app.bootstrap import TradeServerBootstrap
from apps.trade_server.app.config import GatewayRuntimeConfig, TradeServerConfig
from apps.trade_server.app.recording.recorder_service import RecorderService
from apps.trade_server.app.recording.sinks import ParquetTickSink
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.samples import make_sample_ticks
from libs.marketdata.symbol_mapping import InstrumentCatalog

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"


def main() -> int:
    runtime = TradeServerBootstrap(
        TradeServerConfig(
            env="smoke",
            project_root=ROOT,
            gateway=GatewayRuntimeConfig(gateway_name="OPENCTPSEC"),
        )
    ).bootstrap()
    try:
        manifest_store = ManifestStore(ROOT / "data" / "manifests")
        recorder = RecorderService(
            event_engine=runtime.event_engine,
            project_root=ROOT,
            instrument_catalog=InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR),
            sink=ParquetTickSink(
                project_root=ROOT,
                raw_root=ROOT / "data" / "raw" / "market_ticks",
                manifest_store=manifest_store,
            ),
            manifest_store=manifest_store,
            gateway_name="OPENCTPSEC",
            mode="smoke",
        )
        run = recorder.start()
        for tick in make_sample_ticks():
            runtime.gateway.on_tick(tick)
        time.sleep(0.5)
        recorder.stop()
        payload = {
            "run_id": run.run_id,
            "raw_files": len(manifest_store.list_raw_file_manifests()),
            "recording_runs": len(manifest_store.list_recording_runs()),
        }
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    finally:
        runtime.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
