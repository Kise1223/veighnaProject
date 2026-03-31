"""Run the M15 rolling retrain campaign over a trade-date window."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from libs.analytics.rolling_campaign_runner import run_rolling_campaign

ROOT = Path(__file__).resolve().parents[1]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date-start", type=date.fromisoformat, required=True)
    parser.add_argument("--date-end", type=date.fromisoformat, required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--basket-id", required=True)
    parser.add_argument("--schedule-mode", choices=["fixed_model", "retrain_every_n_trade_days"], required=True)
    parser.add_argument("--model-run-id")
    parser.add_argument("--latest-model", action="store_true")
    parser.add_argument("--retrain-every-n-trade-days", type=int)
    parser.add_argument("--training-window-mode", choices=["expanding_to_prior_day"], default="expanding_to_prior_day")
    parser.add_argument("--execution-source-type", choices=["shadow", "paper"], default="shadow")
    parser.add_argument("--market-replay-mode", choices=["bars_1m", "ticks_l1"])
    parser.add_argument("--tick-fill-model", choices=["crossing_full_fill_v1", "l1_partial_fill_v1"])
    parser.add_argument("--time-in-force", choices=["DAY", "IOC"])
    parser.add_argument(
        "--benchmark-source-type",
        choices=["none", "equal_weight_target_universe", "equal_weight_union", "custom_weights"],
        default="none",
    )
    parser.add_argument("--benchmark-path", type=Path)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    if args.schedule_mode == "fixed_model" and args.model_run_id and args.latest_model:
        parser.error("--model-run-id and --latest-model are mutually exclusive")
    if args.schedule_mode == "retrain_every_n_trade_days":
        if args.retrain_every_n_trade_days is None or args.retrain_every_n_trade_days < 1:
            parser.error("--retrain-every-n-trade-days must be >= 1 for retrain_every_n_trade_days")
        if args.model_run_id or args.latest_model:
            parser.error("fixed-model selectors are not allowed for retrain_every_n_trade_days")
    if args.benchmark_source_type == "custom_weights" and args.benchmark_path is None:
        parser.error("--benchmark-path is required when --benchmark-source-type=custom_weights")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_rolling_campaign(
            project_root=ROOT,
            date_start=args.date_start,
            date_end=args.date_end,
            account_id=args.account_id,
            basket_id=args.basket_id,
            schedule_mode=args.schedule_mode,
            model_run_id=args.model_run_id,
            latest_model=args.latest_model,
            retrain_every_n_trade_days=args.retrain_every_n_trade_days,
            training_window_mode=args.training_window_mode,
            execution_source_type=args.execution_source_type,
            market_replay_mode=args.market_replay_mode,
            tick_fill_model=args.tick_fill_model,
            time_in_force=args.time_in_force,
            benchmark_source_type=args.benchmark_source_type,
            benchmark_path=args.benchmark_path,
            force=args.force,
        )
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
