"""Build dry-run execution_task and order_intent_preview artifacts from target weights."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from libs.planning.rebalance import plan_rebalance

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--basket-id", required=True)
    parser.add_argument("--strategy-run-id")
    parser.add_argument("--config", default="configs/planning/rebalance_planner.yaml")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = plan_rebalance(
        project_root=ROOT,
        trade_date=date.fromisoformat(args.trade_date),
        account_id=args.account_id,
        basket_id=args.basket_id,
        config_path=Path(args.config),
        strategy_run_id=args.strategy_run_id,
        force=args.force,
    )
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
