"""Build an M13 benchmark reference from an existing M12 portfolio source."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from libs.analytics.benchmark_attribution import build_benchmark_reference

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--portfolio-analytics-run-id")
    parser.add_argument("--paper-run-id")
    parser.add_argument("--shadow-run-id")
    parser.add_argument("--trade-date", type=date.fromisoformat)
    parser.add_argument("--account-id")
    parser.add_argument("--basket-id")
    parser.add_argument("--latest", action="store_true")
    parser.add_argument(
        "--source-type",
        required=True,
        choices=["custom_weights", "equal_weight_target_universe", "equal_weight_union"],
    )
    parser.add_argument("--benchmark-path", type=Path)
    parser.add_argument("--benchmark-name")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    if bool(args.paper_run_id) and bool(args.shadow_run_id):
        parser.error("--paper-run-id and --shadow-run-id are mutually exclusive")
    if not args.portfolio_analytics_run_id and not args.paper_run_id and not args.shadow_run_id:
        if not args.trade_date or not args.account_id or not args.basket_id:
            parser.error(
                "either --portfolio-analytics-run-id, --paper-run-id/--shadow-run-id, or --trade-date/--account-id/--basket-id is required"
            )
    if args.source_type == "custom_weights" and args.benchmark_path is None:
        parser.error("--benchmark-path is required when --source-type=custom_weights")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = build_benchmark_reference(
            project_root=ROOT,
            portfolio_analytics_run_id=args.portfolio_analytics_run_id,
            paper_run_id=args.paper_run_id,
            shadow_run_id=args.shadow_run_id,
            trade_date=args.trade_date,
            account_id=args.account_id,
            basket_id=args.basket_id,
            latest=args.latest,
            source_type=args.source_type,
            benchmark_path=args.benchmark_path,
            benchmark_name=args.benchmark_name,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
