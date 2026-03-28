# M11 Execution Analytics / TCA

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
.\.venv\Scripts\python.exe -m scripts.list_shadow_sessions
.\.venv\Scripts\python.exe -m scripts.run_execution_tca --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.compare_execution_runs --left-shadow-run-id <bars_run_id> --right-shadow-run-id <ticks_crossing_run_id> --compare-basis bars_vs_ticks
.\.venv\Scripts\python.exe -m scripts.compare_execution_runs --left-shadow-run-id <ticks_crossing_run_id> --right-shadow-run-id <ticks_partial_day_run_id> --compare-basis full_vs_partial
.\.venv\Scripts\python.exe -m scripts.compare_execution_runs --left-shadow-run-id <ticks_partial_day_run_id> --right-shadow-run-id <ticks_partial_ioc_run_id> --compare-basis day_vs_ioc
.\.venv\Scripts\python.exe -m scripts.list_execution_analytics
```

## Supported Sources

- `M7` paper runs
- `M8` bar-driven shadow runs
- `M9` tick-driven crossing-full-fill shadow runs
- `M10` tick-driven L1 partial-fill shadow runs

`M11` reads those artifacts as they are. It does not re-run execution and it never calls real `send_order`.

## TCA Semantics

- `requested_quantity`: original order quantity
- `filled_quantity`: total realized fill quantity
- `remaining_quantity`: unfilled remainder
- `fill_rate`: `filled_quantity / requested_quantity`
- `partial_fill_count`: number of realized fill slices that contributed to a partial outcome
- `estimated_cost_total`: planning / preview estimate
- `realized_cost_total`: realized cost from paper trades or shadow fills plus the existing cost model
- `implementation_shortfall`:
  - buy: `(avg_fill_price - reference_price) * filled_quantity + realized_cost_total`
  - sell: `(reference_price - avg_fill_price) * filled_quantity + realized_cost_total`
  - no fill: `0.00`

Session end states are normalized to:

- `filled`
- `partially_filled_then_expired`
- `expired_end_of_session`
- `expired_ioc_remaining`
- `rejected_validation`
- `unfilled`

## Compare Semantics

- `bars_vs_ticks`
- `full_vs_partial`
- `day_vs_ioc`
- `paper_vs_shadow`

Comparison uses only the intersection of `instrument_key`. Any left-only or right-only instruments are recorded in `summary_json` with counts and ids instead of being silently merged.

## Selector And Idempotency Rules

- `scripts.run_execution_tca` accepts `--paper-run-id`, `--shadow-run-id`, or `trade_date + account_id + basket_id`.
- If that date/account/basket selector matches multiple paper/shadow sources, the CLI errors unless `--latest` is passed.
- Analytics and compare artifacts reuse successful runs by default.
- Failed runs never block reruns.
- `--force` clears and rebuilds the same deterministic artifact path.

## Common Failures

- No matching source run: selector is wrong or the source run has not been produced yet.
- Multiple matching sources without `--latest`: pass an explicit run id or `--latest`.
- Unmatched instruments in compare: only the intersection is comparable; check `summary_json` for dropped left/right symbols.
- No fills / expired run: fill rate stays `0`, `implementation_shortfall` stays `0.00`, and `session_end_status` records the terminal outcome.
