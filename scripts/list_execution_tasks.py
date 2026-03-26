"""List local M6 execution_task artifacts."""

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
            "execution_task_id": manifest.execution_task_id,
            "strategy_run_id": manifest.strategy_run_id,
            "trade_date": manifest.trade_date.isoformat(),
            "account_id": manifest.account_id,
            "basket_id": manifest.basket_id,
            "status": manifest.status.value,
            "preview_row_count": manifest.preview_row_count,
            "file_path": manifest.file_path,
            "preview_file_path": manifest.preview_file_path,
            "source_target_weight_hash": manifest.source_target_weight_hash,
        }
        for manifest in store.list_execution_task_manifests()
    ]
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
