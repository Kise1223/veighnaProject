"""Run daily inference with the latest successful M5 baseline model."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from apps.research_qlib.workflow import run_daily_inference

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--base-config", default="configs/qlib/base.yaml")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_daily_inference(
        project_root=ROOT,
        trade_date=date.fromisoformat(args.trade_date),
        run_id=args.run_id,
        base_config_path=Path(args.base_config),
    )
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
