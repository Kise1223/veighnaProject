"""Build a deterministic explicit model schedule sample from an existing M15 schedule run."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from libs.analytics.model_schedule_artifacts import ModelScheduleArtifactStore

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-start", type=date.fromisoformat, required=True)
    parser.add_argument("--date-end", type=date.fromisoformat, required=True)
    parser.add_argument("--source-model-schedule-run-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    store = ModelScheduleArtifactStore(ROOT)
    manifest = next(
        (
            item
            for item in store.list_schedule_manifests()
            if item.model_schedule_run_id == args.source_model_schedule_run_id
        ),
        None,
    )
    if manifest is None:
        sys.stderr.write(f"no model schedule manifest found for {args.source_model_schedule_run_id}\n")
        return 2
    day_rows = store.load_day_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        model_schedule_run_id=manifest.model_schedule_run_id,
    ).sort_values("trade_date")
    payload = {
        "schedule": [
            {
                "trade_date": item["trade_date"].isoformat(),
                "model_run_id": str(item["resolved_model_run_id"]),
            }
            for item in day_rows.to_dict(orient="records")
            if args.date_start <= item["trade_date"] <= args.date_end
        ]
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    sys.stdout.write(json.dumps({"output": str(args.output), "row_count": len(payload["schedule"])}, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
