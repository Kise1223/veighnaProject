"""Dry-run ingest an execution_task into trade-server order-request previews."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from apps.trade_server.app.planning.ingest import ingest_execution_task_dry_run

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--basket-id", required=True)
    parser.add_argument("--execution-task-id")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.dry_run:
        raise SystemExit("M6 ingestion only supports --dry-run")
    result = ingest_execution_task_dry_run(
        project_root=ROOT,
        trade_date=date.fromisoformat(args.trade_date),
        account_id=args.account_id,
        basket_id=args.basket_id,
        execution_task_id=args.execution_task_id,
    )
    sys.stdout.write(json.dumps(result.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
