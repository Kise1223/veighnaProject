"""List local M5 research model runs."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from apps.research_qlib.bootstrap import load_runtime_config
from libs.research.artifacts import ResearchArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    runtime_config = load_runtime_config(ROOT / "configs" / "qlib" / "base.yaml")
    store = ResearchArtifactStore(ROOT, ROOT / runtime_config.artifacts_root)
    payload = [
        {
            "run_id": run.run_id,
            "status": run.status.value,
            "created_at": run.created_at.isoformat(),
            "model_name": run.model_name,
            "model_version": run.model_version,
            "source_qlib_export_run_id": run.source_qlib_export_run_id,
        }
        for run in store.list_runs()
    ]
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
