# M12 Portfolio / Risk Analytics

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
.\.venv\Scripts\python.exe -m scripts.build_target_weights --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --approved-by local_smoke
.\.venv\Scripts\python.exe -m scripts.plan_rebalance --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.ingest_execution_task --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --dry-run
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode bars_1m
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model crossing_full_fill_v1
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model l1_partial_fill_v1 --time-in-force DAY
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model l1_partial_fill_v1 --time-in-force IOC
.\.venv\Scripts\python.exe -m scripts.run_execution_tca --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.run_portfolio_analytics --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.compare_portfolios --left-shadow-run-id <bars_run_id> --right-shadow-run-id <ticks_crossing_run_id> --compare-basis bars_vs_ticks
.\.venv\Scripts\python.exe -m scripts.compare_portfolios --left-shadow-run-id <ticks_crossing_run_id> --right-shadow-run-id <ticks_partial_day_run_id> --compare-basis full_vs_partial
.\.venv\Scripts\python.exe -m scripts.compare_portfolios --left-shadow-run-id <ticks_partial_day_run_id> --right-shadow-run-id <ticks_partial_ioc_run_id> --compare-basis day_vs_ioc
.\.venv\Scripts\python.exe -m scripts.list_portfolio_analytics
```

## Supported Sources

- `M7` one-shot paper runs
- `M8` bar-driven shadow runs
- `M9` tick-driven crossing-full-fill shadow runs
- `M10` tick-driven L1 partial-fill shadow runs
- `M11` TCA rows and summaries when present

`M12` reads those artifacts as they are. It never replays execution and it never calls real `send_order`.

## Metric Semantics

- `target_weight`: approved target weight from `approved_target_weight`
- `executed_weight`: `market_value_end / net_liquidation_end`
- `weight_drift`: `abs(target_weight - executed_weight)`
- `target_cash_weight`: `max(0, 1 - sum(target_weight))`
- `executed_cash_weight`: `cash_end / net_liquidation_end`
- `tracking_error_proxy`: `0.5 * sum(abs(target_weight - executed_weight))`
- `realized_turnover`: `filled_notional_total / max(net_liquidation_start, epsilon)`
- `top1`, `top3`, `top5`: largest end-of-session executed weights
- `hhi_concentration`: sum of squared end-of-session executed weights

Planned and executed notionals stay distinct:

- `planned_notional_total` prefers planning / preview artifacts and falls back to `target_weight * net_liquidation_start` only when planning previews are absent
- `filled_notional_total`, `fill_rate`, and realized execution costs prefer `M11` TCA when available and otherwise derive from source fills / trades / reconcile artifacts
- `realized_pnl` and `unrealized_pnl` come from the final paper ledger snapshots

## Compare Semantics

- `planned_vs_executed`
- `bars_vs_ticks`
- `full_vs_partial`
- `day_vs_ioc`
- `paper_vs_shadow`

Comparison operates on the intersection of `instrument_key`. Unmatched left/right instruments are recorded in `summary_json` and excluded from row-by-row deltas.

## Selector And Idempotency Rules

- `scripts.run_portfolio_analytics` accepts `--paper-run-id`, `--shadow-run-id`, or `trade_date + account_id + basket_id`.
- If that selector matches multiple source runs, the CLI errors unless `--latest` is passed.
- Portfolio analytics and compare artifacts reuse successful runs by default.
- Failed runs never block reruns.
- `--force` clears and rebuilds the same deterministic artifact path.

## Common Failures

- No matching source run: the selected paper/shadow run has not been produced yet.
- Multiple matches without `--latest`: pass an explicit run id or add `--latest`.
- No matching target weights: the run cannot be mapped back to `approved_target_weight`; check `strategy_run_id`.
- Missing TCA artifacts: `M12` falls back to execution artifacts; metrics still run, but `summary_json["tca_source"]` will show the fallback path.
- Mismatched universes in compare: only the instrument intersection is comparable; inspect `summary_json` for dropped left/right instruments.
