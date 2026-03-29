# M14 Walk-Forward Campaign

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[research]"
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_walkforward_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest-model --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.run_walkforward_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest-model --execution-source-type shadow --market-replay-mode ticks_l1 --tick-fill-model l1_partial_fill_v1 --time-in-force DAY --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.list_campaigns
.\.venv\Scripts\python.exe -m scripts.compare_campaigns --left-campaign-run-id <bars_campaign_run_id> --right-campaign-run-id <ticks_partial_day_campaign_run_id> --compare-basis bars_vs_ticks
```

Optional benchmark-disabled path:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_walkforward_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest-model --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type none
```

## Model Selection Semantics

- `--model-run-id` pins one explicit successful model across the entire date range.
- `--latest-model` is an explicit selector for the default behavior: use the latest successful `model_run` when `--model-run-id` is not provided.
- `M14` is not a rolling retraining scheduler; it does not retrain a new model per day.

## Daily Reuse And Campaign Idempotency

- Each day resolves or reuses one prediction, one target-weight artifact, one execution task, one paper/shadow execution source, one `M11` analytics run, one `M12` portfolio analytics run, and optional `M13` benchmark artifacts.
- A campaign day is marked `reused` only when every upstream daily artifact for that date was already successful and reused.
- Campaign idempotency includes date window, account, basket, execution mode, benchmark mode, and resolved model identity.
- Successful campaigns are reused by default.
- Failed campaigns can rerun.
- `--force` clears and rebuilds the campaign artifacts at the same deterministic path instead of silently overwriting them.

## Metric Semantics

- `daily_fill_rate`, `daily_filled_notional`, and `daily_realized_cost` prefer `M11` summaries.
- `daily_weight_drift_l1`, `cash_weight_end`, concentration, PnL, and `net_liquidation_end` prefer `M12` summaries.
- `daily_active_share` and `daily_active_contribution_proxy` come from `M13` benchmark summaries when benchmark is enabled.
- When benchmark is disabled, those active metrics stay `null`.
- `max_drawdown` is the peak-to-trough drawdown of the ordered `net_liquidation_end` campaign series.

## Compare Semantics

- Supported campaign compare bases are `bars_vs_ticks`, `full_vs_partial`, `day_vs_ioc`, and `paper_vs_shadow`.
- Compare uses overlapping `trade_date` values only.
- Left-only and right-only dates are recorded in `summary_json`; they are not silently forward-filled or aligned.

## Common Failures

- No successful model run exists: train the baseline first or pass an explicit successful `--model-run-id`.
- Date window contains no trade dates: the campaign runner rejects the range instead of emitting an empty campaign.
- Multiple source artifacts exist upstream for a daily selector: use the normal upstream exact selectors first so the campaign can reuse a single deterministic source.
- Benchmark path missing: `--benchmark-path` is required when `--benchmark-source-type=custom_weights`.
- Benchmark-disabled campaigns show `active_share` and `active_contribution_proxy` as `null`: this is expected, not a data corruption issue.
