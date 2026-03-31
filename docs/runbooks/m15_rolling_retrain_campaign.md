# M15 Rolling Retrain Campaign

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[research]"
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode fixed_model --latest-model --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode retrain_every_n_trade_days --retrain-every-n-trade-days 1 --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.list_model_schedules
.\.venv\Scripts\python.exe -m scripts.compare_rolling_campaigns --left-campaign-run-id <fixed_campaign_run_id> --right-campaign-run-id <rolling_campaign_run_id> --compare-basis fixed_vs_rolling
```

Optional cadence comparison:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode retrain_every_n_trade_days --retrain-every-n-trade-days 2 --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.compare_rolling_campaigns --left-campaign-run-id <rolling_1d_campaign_run_id> --right-campaign-run-id <rolling_2d_campaign_run_id> --compare-basis retrain_1d_vs_retrain_2d
```

## Schedule Semantics

- `fixed_model` uses one explicit successful `--model-run-id`, or resolves the latest successful model once at campaign start when `--latest-model` is used.
- `retrain_every_n_trade_days` retrains or reuses a deterministic model on refresh days and reuses that most recent successful model on non-refresh days.
- `--retrain-every-n-trade-days` must be at least `1`.
- Current required `--training-window-mode` is `expanding_to_prior_day`.

## No-Look-Ahead Guard

- On retrain days for trade date `T`, `train_end` is always the previous trade date.
- `train_start` uses the earliest eligible configured baseline dataset start.
- Model switches occur only between trading days, never intraday.

## Model-Age And Switch Metrics

- `model_switch_flag` is `true` only when the resolved model id changes from the previous trade date.
- `model_age_trade_days` is the trade-day distance from `train_end` to the current trade date, clamped at `0` when a fixed model was trained after the evaluated date.
- `days_since_last_retrain` is `0` on refresh days and increases on later reuse days.
- Campaign summary reports `unique_model_count`, `retrain_count`, `average_model_age_trade_days`, and `max_model_age_trade_days`.

## Compare Semantics

- Supported rolling compare bases are `fixed_vs_rolling` and `retrain_1d_vs_retrain_2d`.
- Compare only uses overlapping `trade_date` values.
- Left-only and right-only dates are recorded in `summary_json`; they are not silently aligned.

## Common Failures

- No successful fixed model exists: build or select a successful `model_run` first.
- Missing `--retrain-every-n-trade-days` for rolling mode: the CLI rejects the run.
- Retrain day has no previous trade date: the scheduler fails explicitly instead of training with future data.
- Benchmark-disabled campaigns show active metrics as `null`: this is expected and matches `M14`.
