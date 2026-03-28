# ADR 0013: M13 Benchmark / Attribution Analytics

- Status: Accepted
- Date: 2026-03-29

## Scope

- `M13` adds file-first benchmark / attribution analytics on top of existing `M12` portfolio analytics.
- It remains analytics-only, paper-only, and stops before any live routing, broker sync, or optimizer work.

## Decision 1: Build Benchmark Analytics On Top Of M12, Not Raw Execution

- `M13` treats `M12` portfolio analytics as the primary source of target weights, executed weights, end-of-session values, and lineage.
- It does not replay execution, rebuild paper ledgers, or introduce a separate execution or attribution engine.
- Benchmark reference, benchmark analytics, and compare outputs stay file-first under `data/analytics/`.

## Decision 2: Benchmark And Attribution Metrics Stay Deterministic And Proxy-Based

- Supported benchmark reference modes are `custom_weights`, `equal_weight_target_universe`, and `equal_weight_union`.
- `active_weight_target` is `target_weight - benchmark_weight`.
- `active_weight_executed` is `executed_weight - benchmark_weight`.
- `target_active_share` and `executed_active_share` are both `0.5 * sum(abs(portfolio_weight - benchmark_weight))`.
- `instrument_return_proxy` is `(mark_price_end - previous_close) / max(previous_close, epsilon)`.
- `portfolio_contribution_proxy` is `executed_weight * instrument_return_proxy`.
- `benchmark_contribution_proxy` is `benchmark_weight * instrument_return_proxy`.
- `active_contribution_proxy` is `portfolio_contribution_proxy - benchmark_contribution_proxy`.
- Group-level attribution uses deterministic proxy metrics only: `allocation_proxy` plus `selection_proxy`, with no interaction term in this milestone.

## Decision 3: Cross-Run Benchmark Comparison Uses Shared Benchmark And Instrument Intersections

- `bars_vs_ticks`, `full_vs_partial`, `day_vs_ioc`, `paper_vs_shadow`, and other benchmark-relative compares require a common `benchmark_run_id`.
- Row-level comparison operates on the intersection of `instrument_key`.
- Left-only and right-only instruments are not silently aligned; unmatched counts and identifiers are recorded in `summary_json`.

## Idempotency

- `benchmark_run_id` is derived from `source_portfolio_analytics_run_id + benchmark_config_hash`.
- `benchmark_analytics_run_id` is derived from `source_portfolio_analytics_run_id + source_run_id + benchmark_run_id + analytics_config_hash`.
- `benchmark_compare_run_id` is derived from `left_benchmark_analytics_run_id + right_benchmark_analytics_run_id + compare_basis + analytics_config_hash`.
- Existing successful runs are reused by default.
- Failed runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
