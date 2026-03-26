from __future__ import annotations

import shutil
from pathlib import Path

from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.qlib_export import export_qlib_provider
from libs.marketdata.raw_store import (
    read_partitioned_frame,
    require_parquet_support,
    write_partition_frame,
)
from libs.marketdata.samples import make_sample_ticks
from libs.marketdata.symbol_mapping import InstrumentCatalog
from scripts import build_standard_data

ROOT = Path(__file__).resolve().parents[1]


def test_rebuild_replaces_target_partition_and_keeps_export_clean(
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    pd = require_parquet_support()
    workspace = tmp_path / "workspace"
    shutil.copytree(ROOT / "data" / "master" / "bootstrap", workspace / "data" / "master" / "bootstrap")
    shutil.copytree(ROOT / "data" / "marketdata" / "bootstrap", workspace / "data" / "marketdata" / "bootstrap")
    catalog = InstrumentCatalog.from_bootstrap_dir(workspace / "data" / "master" / "bootstrap")
    resolved = catalog.resolve(instrument_key="EQ_SH_600000")
    raw_rows = []
    for index, tick in enumerate(make_sample_ticks()[:3], start=1):
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
                "ingest_seq": index,
                "raw_hash": f"{index}_{tick.exchange_ts.isoformat()}",
                "recorded_at": tick.received_ts,
            }
        )
    write_partition_frame(
        raw_rows,
        base_dir=workspace / "data" / "raw" / "market_ticks",
        trade_date=__import__("datetime").date(2026, 3, 25),
        exchange=resolved.mapping.exchange.value,
        symbol=resolved.mapping.symbol,
        file_stem="raw_sample",
    )

    monkeypatch.setattr(build_standard_data, "ROOT", workspace)
    monkeypatch.setattr(build_standard_data, "BOOTSTRAP_DIR", workspace / "data" / "master" / "bootstrap")
    monkeypatch.setattr(
        build_standard_data,
        "CORPORATE_ACTIONS_PATH",
        workspace / "data" / "marketdata" / "bootstrap" / "corporate_actions.json",
    )

    monkeypatch.setattr(
        "sys.argv",
        ["build_standard_data", "--trade-date", "2026-03-25", "--symbol", "600000.SSE", "--rebuild"],
    )
    assert build_standard_data.main() == 0
    first_bars = read_partitioned_frame(
        workspace / "data" / "standard" / "bars_1d",
        trade_date=__import__("datetime").date(2026, 3, 25),
        symbol="600000",
        exchange="SSE",
    )
    assert len(first_bars) == 1

    monkeypatch.setattr(
        "sys.argv",
        ["build_standard_data", "--trade-date", "2026-03-25", "--symbol", "600000.SSE", "--rebuild"],
    )
    assert build_standard_data.main() == 0
    second_bars = read_partitioned_frame(
        workspace / "data" / "standard" / "bars_1d",
        trade_date=__import__("datetime").date(2026, 3, 25),
        symbol="600000",
        exchange="SSE",
    )
    assert len(second_bars) == 1
    assert second_bars["trade_date"].nunique() == 1

    manifest_store = ManifestStore(workspace / "data" / "manifests")
    bar_manifests = manifest_store.list_standard_file_manifests(layer="bars_1d")
    assert len(bar_manifests) == 1
    assert Path(workspace / bar_manifests[0].file_path).exists()

    payload = export_qlib_provider(
        project_root=workspace,
        provider_root=workspace / "data" / "qlib_bin",
        catalog=catalog,
        manifest_store=manifest_store,
        freq="1d",
        build_run_id="qlib_rebuild_test",
    )
    assert payload["rows"] == 1
    exported = pd.read_csv(workspace / "data" / "qlib_bin" / "instruments" / "all.txt", sep="\t", header=None)
    assert len(exported) == 1
