"""Run M12 portfolio analytics for one paper or shadow execution source."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from libs.analytics.portfolio import run_portfolio_analytics

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-run-id")
    parser.add_argument("--shadow-run-id")
    parser.add_argument("--trade-date", type=date.fromisoformat)
    parser.add_argument("--account-id")
    parser.add_argument("--basket-id")
    parser.add_argument("--latest", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    if args.paper_run_id and args.shadow_run_id:
        parser.error("--paper-run-id and --shadow-run-id are mutually exclusive")
    if not args.paper_run_id and not args.shadow_run_id:
        if not args.trade_date or not args.account_id or not args.basket_id:
            parser.error(
                "either --paper-run-id/--shadow-run-id or --trade-date/--account-id/--basket-id is required"
            )
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_portfolio_analytics(
            project_root=ROOT,
            paper_run_id=args.paper_run_id,
            shadow_run_id=args.shadow_run_id,
            trade_date=args.trade_date,
            account_id=args.account_id,
            basket_id=args.basket_id,
            latest=args.latest,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
