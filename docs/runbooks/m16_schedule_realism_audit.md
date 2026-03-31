# M16 Schedule Realism Audit

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[research]"
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode fixed_model --latest-model --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode retrain_every_n_trade_days --retrain-every-n-trade-days 1 --training-window-mode expanding_to_prior_day --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode retrain_every_n_trade_days --retrain-every-n-trade-days 1 --training-window-mode rolling_lookback --lookback-trade-days 2 --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.build_explicit_model_schedule_sample --date-start 2026-03-24 --date-end 2026-03-26 --source-model-schedule-run-id <one_existing_schedule_run_id> --output data\bootstrap\model_schedule\explicit_schedule_2026-03-24_2026-03-26.json
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode explicit_model_schedule --schedule-path data\bootstrap\model_schedule\explicit_schedule_2026-03-24_2026-03-26.json --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.list_model_schedules
.\.venv\Scripts\python.exe -m scripts.list_schedule_audits
.\.venv\Scripts\python.exe -m scripts.compare_rolling_campaigns --left-campaign-run-id <fixed_campaign_run_id> --right-campaign-run-id <expanding_campaign_run_id> --compare-basis fixed_vs_rolling
.\.venv\Scripts\python.exe -m scripts.compare_rolling_campaigns --left-campaign-run-id <expanding_campaign_run_id> --right-campaign-run-id <rolling_lookback_campaign_run_id> --compare-basis expanding_vs_rolling_lookback
```

## Training Window Semantics

- `expanding_to_prior_day`:
  - `train_end = previous trade date`
  - `train_start = earliest eligible configured baseline dataset start`
- `rolling_lookback`:
  - `train_end = previous trade date`
  - `train_start = the trade date N trade days before train_end, inclusive if available, otherwise the earliest eligible training date`
  - `--lookback-trade-days` must be at least `1`
- The first implementation keeps the same deterministic baseline trainer and clamps `min_train_rows` to the smaller rolling-lookback capacity so short-window smoke runs remain feasible.

## Explicit Schedule Semantics

- `--schedule-mode explicit_model_schedule` requires `--schedule-path`.
- The file must resolve exactly one `model_run_id` for every `trade_date` in the requested window.
- Missing dates, duplicate dates, and unresolved `model_run_id` values are errors.
- There is no fallback to latest model.
- The helper `scripts.build_explicit_model_schedule_sample` can export a deterministic per-day sample from an existing M15 schedule run.

## No-Lookahead Audit Semantics

- Every rolling campaign emits a `schedule_audit_run`.
- `strict_no_lookahead_expected` is `true` for historical modes that are intended to be strict (`retrain_every_n_trade_days`, `explicit_model_schedule`) and `false` for convenience fixed modes.
- `strict_no_lookahead_passed` is `true` only when `train_end <= previous_trade_date`.
- `fixed_model + latest-model` may be non-strict for early dates because the latest model is resolved once at campaign start and then frozen.
- `explicit_model_schedule` only passes strict audit when model metadata can prove `train_end <= previous_trade_date`; otherwise it emits `explicit_schedule_no_train_metadata`.

## Compare Semantics

- Supported M16 compare bases:
  - `fixed_vs_rolling`
  - `retrain_1d_vs_retrain_2d`
  - `expanding_vs_rolling_lookback`
  - `explicit_schedule_vs_fixed`
  - `explicit_schedule_vs_retrain_1d`
- Compare only uses overlapping `trade_date` values.
- Left-only and right-only dates are recorded in `summary_json`; they are not silently aligned.

## Common Failures

- Missing `--lookback-trade-days` for `rolling_lookback`: the CLI rejects the run.
- Explicit schedule missing a campaign date: the loader fails instead of filling with the latest model.
- Duplicate explicit schedule dates: the loader rejects them explicitly.
- `fixed_model + latest-model` shows non-strict warnings on early dates: this is expected and documented.
- Benchmark-disabled campaigns keep active metrics as `null`, exactly as in `M14/M15`.
