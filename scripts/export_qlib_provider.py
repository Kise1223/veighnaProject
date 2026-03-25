"""Export M4 standardized bars into a qlib file provider."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from apps.trade_server.app.recording.manifests import make_run_id
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.qlib_export import export_qlib_provider, qlib_smoke_read
from libs.marketdata.symbol_mapping import InstrumentCatalog

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freq", choices=["1d", "1min"], required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    build_run_id = make_run_id("qlib_export", args.freq)
    manifest_store = ManifestStore(ROOT / "data" / "manifests")
    payload = export_qlib_provider(
        project_root=ROOT,
        provider_root=ROOT / "data" / "qlib_bin",
        catalog=catalog,
        manifest_store=manifest_store,
        freq=args.freq,
        build_run_id=build_run_id,
    )
    sample = qlib_smoke_read(
        provider_root=ROOT / "data" / "qlib_bin",
        qlib_symbol=catalog.to_qlib_symbol("EQ_SH_600000"),
        freq=args.freq,
    )
    smoke_shape = list(getattr(sample, "shape", (0, 0)))
    output = {
        "build_run_id": build_run_id,
        "freq": args.freq,
        "rows": payload["rows"],
        "calendar_size": payload["calendar_size"],
        "smoke_shape": smoke_shape,
    }
    sys.stdout.write(json.dumps(output, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
