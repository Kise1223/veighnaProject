# M13 Benchmark / Attribution Analytics

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
.\.venv\Scripts\python.exe -m scripts.build_benchmark_reference --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --source-type equal_weight_target_universe --latest
.\.venv\Scripts\python.exe -m scripts.run_benchmark_analytics --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest --benchmark-run-id <benchmark_run_id>
.\.venv\Scripts\python.exe -m scripts.compare_benchmark_analytics --left-benchmark-analytics-run-id <bars_benchmark_run_id> --right-benchmark-analytics-run-id <ticks_crossing_benchmark_run_id> --compare-basis bars_vs_ticks
.\.venv\Scripts\python.exe -m scripts.compare_benchmark_analytics --left-benchmark-analytics-run-id <ticks_crossing_benchmark_run_id> --right-benchmark-analytics-run-id <ticks_partial_day_benchmark_run_id> --compare-basis full_vs_partial
.\.venv\Scripts\python.exe -m scripts.compare_benchmark_analytics --left-benchmark-analytics-run-id <ticks_partial_day_benchmark_run_id> --right-benchmark-analytics-run-id <ticks_partial_ioc_benchmark_run_id> --compare-basis day_vs_ioc
.\.venv\Scripts\python.exe -m scripts.list_benchmark_analytics
```

## Supported Benchmark Sources

- `custom_weights`: read a local JSON file and normalize the supplied benchmark weights deterministically
- `equal_weight_target_universe`: equal-weight the positive target-weight universe from the selected `M12` portfolio analytics run
- `equal_weight_union`: equal-weight the union of target holdings and executed end-of-session holdings

`custom_weights` accepts either a JSON object keyed by `instrument_key` or a JSON list of rows with `instrument_key` and `benchmark_weight` / `weight`.

## Metric Semantics

- `active_weight_target = target_weight - benchmark_weight`
- `active_weight_executed = executed_weight - benchmark_weight`
- `target_active_share = 0.5 * sum(abs(target_weight - benchmark_weight))`
- `executed_active_share = 0.5 * sum(abs(executed_weight - benchmark_weight))`
- `active_cash_weight = executed_cash_weight - benchmark_cash_weight`
- `instrument_return_proxy = (mark_price_end - previous_close) / max(previous_close, epsilon)`
- `portfolio_contribution_proxy = executed_weight * instrument_return_proxy`
- `benchmark_contribution_proxy = benchmark_weight * instrument_return_proxy`
- `active_contribution_proxy = portfolio_contribution_proxy - benchmark_contribution_proxy`
- `allocation_proxy = (executed_group_weight - benchmark_group_weight) * benchmark_group_return_proxy`
- `selection_proxy = executed_group_weight * (group_return_proxy - benchmark_group_return_proxy)`

`mark_price_end` prefers `market_value_end / quantity_end`, then the executed price reference, then `previous_close`. If `previous_close` is unavailable, the first milestone version uses a `0` return proxy instead of inventing a new pricing path.

## Compare Semantics

- `bars_vs_ticks`
- `full_vs_partial`
- `day_vs_ioc`
- `paper_vs_shadow`
- `target_vs_executed_relative`

Comparison operates on the intersection of `instrument_key`. Left-only and right-only instruments are recorded in `summary_json`; they are not silently aligned or forward-filled.

## Selector And Idempotency Rules

- `scripts.build_benchmark_reference` accepts `--portfolio-analytics-run-id`, `--paper-run-id`, `--shadow-run-id`, or `trade_date + account_id + basket_id`.
- `scripts.run_benchmark_analytics` accepts the same source selectors plus `--benchmark-run-id` or `--benchmark-source-type`.
- If a selector matches multiple source runs, the CLI errors unless `--latest` is passed.
- Benchmark reference, benchmark analytics, and benchmark compare artifacts reuse successful runs by default.
- Failed runs never block reruns.
- `--force` clears and rebuilds the same deterministic artifact path.

## Common Failures

- No matching `M12` portfolio analytics run: build or select the upstream portfolio run first.
- Multiple matches without `--latest`: pass an explicit run id or add `--latest`.
- Missing custom benchmark file: `--benchmark-path` is required for `custom_weights`.
- Negative or zero-sum custom weights: the benchmark builder rejects them instead of normalizing silently.
- Benchmark mismatch in compare: left and right analytics runs must reference the same `benchmark_run_id`.
- Optional industry grouping is absent: `M13` skips it cleanly unless the repository has a stable industry source.
