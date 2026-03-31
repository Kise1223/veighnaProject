"""Compare two M15 rolling campaigns."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from libs.analytics.rolling_campaign_compare import compare_rolling_campaigns

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left-campaign-run-id", required=True)
    parser.add_argument("--right-campaign-run-id", required=True)
    parser.add_argument(
        "--compare-basis",
        required=True,
        choices=[
            "fixed_vs_rolling",
            "retrain_1d_vs_retrain_2d",
            "expanding_vs_rolling_lookback",
            "explicit_schedule_vs_fixed",
            "explicit_schedule_vs_retrain_1d",
            "bars_vs_ticks",
            "full_vs_partial",
            "day_vs_ioc",
            "paper_vs_shadow",
        ],
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = compare_rolling_campaigns(
            project_root=ROOT,
            left_campaign_run_id=args.left_campaign_run_id,
            right_campaign_run_id=args.right_campaign_run_id,
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
