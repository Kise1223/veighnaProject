"""List local M8 replay-driven shadow session runs."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from libs.execution.shadow_artifacts import ShadowArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    store = ShadowArtifactStore(ROOT)
    payload = [item.model_dump(mode="json") for item in store.list_runs()]
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

