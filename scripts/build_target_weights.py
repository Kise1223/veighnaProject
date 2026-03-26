"""Build approved_target_weight artifacts from M5 prediction outputs."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from libs.planning.target_weights import build_target_weights

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--basket-id", required=True)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--prediction-run-id")
    parser.add_argument("--config", default="configs/planning/target_weight_baseline.yaml")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = build_target_weights(
        project_root=ROOT,
        trade_date=date.fromisoformat(args.trade_date),
        account_id=args.account_id,
        basket_id=args.basket_id,
        approved_by=args.approved_by,
        prediction_run_id=args.prediction_run_id,
        config_path=Path(args.config),
        force=args.force,
    )
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
