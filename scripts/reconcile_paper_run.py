"""Load the reconcile report for one M7 paper execution run."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from apps.trade_server.app.paper.runner import load_reconcile_report

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", type=date.fromisoformat, required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--basket-id", required=True)
    parser.add_argument("--paper-run-id")
    parser.add_argument("--execution-task-id")
    parser.add_argument("--latest", action="store_true")
    args = parser.parse_args(argv)
    if args.paper_run_id and args.execution_task_id:
        parser.error("--paper-run-id and --execution-task-id are mutually exclusive")
    if args.paper_run_id and args.latest:
        parser.error("--paper-run-id and --latest cannot be used together")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = load_reconcile_report(
            project_root=ROOT,
            trade_date=args.trade_date,
            account_id=args.account_id,
            basket_id=args.basket_id,
            paper_run_id=args.paper_run_id,
            execution_task_id=args.execution_task_id,
            latest=args.latest,
        )
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    sys.stdout.write(json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
