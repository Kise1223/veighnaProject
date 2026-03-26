"""List local M6 approved_target_weight artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from libs.planning.artifacts import PlanningArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    store = PlanningArtifactStore(ROOT)
    payload = [
        {
            "strategy_run_id": manifest.strategy_run_id,
            "prediction_run_id": manifest.prediction_run_id,
            "trade_date": manifest.trade_date.isoformat(),
            "account_id": manifest.account_id,
            "basket_id": manifest.basket_id,
            "row_count": manifest.row_count,
            "file_path": manifest.file_path,
            "source_qlib_export_run_id": manifest.source_qlib_export_run_id,
            "source_standard_build_run_id": manifest.source_standard_build_run_id,
        }
        for manifest in store.list_target_weight_manifests()
    ]
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
