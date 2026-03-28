"""List local M12 portfolio analytics and compare artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    store = PortfolioAnalyticsArtifactStore(ROOT)
    payload = {
        "portfolio_runs": [item.model_dump(mode="json") for item in store.list_portfolio_manifests()],
        "compare_runs": [item.model_dump(mode="json") for item in store.list_compare_manifests()],
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
