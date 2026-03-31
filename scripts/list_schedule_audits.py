"""List local M16 schedule audit artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from libs.analytics.schedule_audit_artifacts import ScheduleAuditArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    store = ScheduleAuditArtifactStore(ROOT)
    payload = {
        "schedule_audit_runs": [item.model_dump(mode="json") for item in store.list_audit_manifests()],
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
