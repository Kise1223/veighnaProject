"""Compare two M13 benchmark-relative analytics runs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from libs.analytics.benchmark_compare import compare_benchmark_analytics

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left-benchmark-analytics-run-id", required=True)
    parser.add_argument("--right-benchmark-analytics-run-id", required=True)
    parser.add_argument(
        "--compare-basis",
        required=True,
        choices=[
            "bars_vs_ticks",
            "full_vs_partial",
            "day_vs_ioc",
            "paper_vs_shadow",
            "target_vs_executed_relative",
        ],
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = compare_benchmark_analytics(
            project_root=ROOT,
            left_benchmark_analytics_run_id=args.left_benchmark_analytics_run_id,
            right_benchmark_analytics_run_id=args.right_benchmark_analytics_run_id,
            compare_basis=args.compare_basis,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
