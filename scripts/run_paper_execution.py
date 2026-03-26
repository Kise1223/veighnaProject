"""Run the M7 paper execution sandbox for one trade date and basket."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from apps.trade_server.app.paper.runner import run_paper_execution

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--basket-id", required=True)
    parser.add_argument("--execution-task-id")
    parser.add_argument("--account-snapshot-path", type=Path)
    parser.add_argument("--positions-path", type=Path)
    parser.add_argument("--market-snapshot-path", type=Path)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_paper_execution(
        project_root=ROOT,
        trade_date=args.trade_date,
        account_id=args.account_id,
        basket_id=args.basket_id,
        execution_task_id=args.execution_task_id,
        account_snapshot_path=args.account_snapshot_path,
        positions_path=args.positions_path,
        market_snapshot_path=args.market_snapshot_path,
        force=args.force,
    )
    sys.stdout.write(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
