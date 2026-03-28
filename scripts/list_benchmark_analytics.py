"""List local M13 benchmark references, analytics, and compare artifacts."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore
from libs.analytics.benchmark_artifacts import BenchmarkReferenceArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    benchmark_store = BenchmarkReferenceArtifactStore(ROOT)
    analytics_store = BenchmarkAttributionArtifactStore(ROOT)
    payload = {
        "benchmark_runs": [item.model_dump(mode="json") for item in benchmark_store.list_benchmark_manifests()],
        "benchmark_analytics_runs": [
            item.model_dump(mode="json") for item in analytics_store.list_benchmark_analytics_manifests()
        ],
        "benchmark_compare_runs": [
            item.model_dump(mode="json") for item in analytics_store.list_benchmark_compare_manifests()
        ],
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
