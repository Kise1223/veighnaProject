# ADR 0014: M14 Walk-Forward Campaign Orchestration

- Status: Accepted
- Date: 2026-03-29

## Scope

- `M14` adds file-first multi-date walk-forward campaign orchestration on top of existing `M5-M13` artifact producers.
- It remains orchestration-plus-analytics only, paper-only, and stops before any live routing, broker sync, or optimizer work.

## Decision 1: M14 Orchestrates Existing M5-M13 Helpers Instead Of Re-Implementing The Pipeline

- `M14` resolves one explicit successful `model_run_id`, or the latest successful `model_run` when no explicit model is provided, and reuses that same model identity for the full date window.
- For each `trade_date`, the runner calls the existing prediction, target-weight, rebalance, execution, `M11`, `M12`, and optional `M13` helpers.
- `M14` does not introduce a parallel execution or analytics stack and does not replay execution from raw market data on its own.

## Decision 2: Campaign Metrics Prefer Existing M11-M13 Summaries

- Daily execution quality metrics prefer `M11` summaries.
- Daily portfolio and cash / concentration metrics prefer `M12` summaries.
- Benchmark-relative daily metrics prefer `M13` summaries when benchmark is enabled.
- `max_drawdown` is a deterministic peak-to-trough drawdown computed from the campaign `net_liquidation_end` time series.
- Benchmark-disabled campaigns keep `active_share` and `active_contribution_proxy` nullable instead of inventing synthetic benchmark values.

## Decision 3: Campaign Compare Uses Overlapping Trade Dates Only

- `bars_vs_ticks`, `full_vs_partial`, `day_vs_ioc`, and `paper_vs_shadow` compare runs only across overlapping `trade_date` values.
- Left-only and right-only dates are not silently aligned; unmatched counts and dates are recorded in `summary_json`.
- Compare outputs remain file-first under `data/analytics/` and do not mutate source campaign artifacts.

## Idempotency

- `campaign_run_id` is derived from `date_start + date_end + account_id + basket_id + execution_source_type + market_replay_mode + tick_fill_model + time_in_force + benchmark_source_type + model_run_id + campaign_config_hash`.
- `campaign_compare_run_id` is derived from `left_campaign_run_id + right_campaign_run_id + compare_basis + compare_config_hash`.
- Existing successful campaign and compare runs are reused by default.
- Failed runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
