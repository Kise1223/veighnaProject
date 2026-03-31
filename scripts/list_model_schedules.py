"""List local M15 model schedule artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from libs.analytics.model_schedule_artifacts import ModelScheduleArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    store = ModelScheduleArtifactStore(ROOT)
    payload = {
        "model_schedule_runs": [item.model_dump(mode="json") for item in store.list_schedule_manifests()],
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
