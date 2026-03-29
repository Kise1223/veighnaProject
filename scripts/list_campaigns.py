"""List local M14 campaign and campaign compare artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from libs.analytics.campaign_artifacts import CampaignArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    store = CampaignArtifactStore(ROOT)
    payload = {
        "campaign_runs": [item.model_dump(mode="json") for item in store.list_campaign_manifests()],
        "campaign_compare_runs": [
            item.model_dump(mode="json") for item in store.list_campaign_compare_manifests()
        ],
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
