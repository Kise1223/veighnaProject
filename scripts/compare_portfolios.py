"""Compare two paper/shadow execution runs under M12 portfolio analytics."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from libs.analytics.portfolio_compare import compare_portfolios

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left-paper-run-id")
    parser.add_argument("--left-shadow-run-id")
    parser.add_argument("--right-paper-run-id")
    parser.add_argument("--right-shadow-run-id")
    parser.add_argument(
        "--compare-basis",
        required=True,
        choices=[
            "planned_vs_executed",
            "paper_vs_shadow",
            "bars_vs_ticks",
            "full_vs_partial",
            "day_vs_ioc",
        ],
    )
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    if bool(args.left_paper_run_id) == bool(args.left_shadow_run_id):
        parser.error("left side requires exactly one of --left-paper-run-id or --left-shadow-run-id")
    if bool(args.right_paper_run_id) == bool(args.right_shadow_run_id):
        parser.error(
            "right side requires exactly one of --right-paper-run-id or --right-shadow-run-id"
        )
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = compare_portfolios(
            project_root=ROOT,
            left_paper_run_id=args.left_paper_run_id,
            left_shadow_run_id=args.left_shadow_run_id,
            right_paper_run_id=args.right_paper_run_id,
            right_shadow_run_id=args.right_shadow_run_id,
            compare_basis=args.compare_basis,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
