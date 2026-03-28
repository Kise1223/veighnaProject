# ADR 0012: M12 Portfolio / Risk Analytics

- Status: Accepted
- Date: 2026-03-28

## Scope

- `M12` adds file-first portfolio / risk analytics on top of existing `M6-M11` artifacts.
- It remains analytics-only, paper-only, and stops before any live routing, broker sync, or portfolio optimizer.

## Decision 1: Analyze Existing Artifacts Instead Of Building A New Portfolio Engine

- `M12` reads approved target weights, execution tasks, order-intent previews, final paper account / position snapshots, reconcile reports, and `M11` TCA artifacts when they exist.
- It does not replay execution again, does not introduce a new ledger, and does not route orders.
- Portfolio analytics and compare outputs stay file-first under `data/analytics/`.

## Decision 2: Use Deterministic, Explainable Portfolio Metrics

- `target_weight` comes from `approved_target_weight`.
- `executed_weight` comes from end-of-session `market_value_end / net_liquidation_end`.
- `target_cash_weight` is `max(0, 1 - sum(target_weight))`.
- `executed_cash_weight` is `cash_end / net_liquidation_end`.
- `tracking_error_proxy` is defined as `0.5 * sum(abs(target_weight - executed_weight))`.
- `realized_turnover` is defined as `filled_notional_total / max(net_liquidation_start, epsilon)`.
- `top1`, `top3`, `top5`, and `HHI` concentration all use end-of-session executed weights.

## Decision 3: Cross-Run Comparison Uses Instrument Intersections Only

- `bars_vs_ticks`, `full_vs_partial`, `day_vs_ioc`, `paper_vs_shadow`, and `planned_vs_executed` compare only the intersection of `instrument_key`.
- Left-only and right-only instruments are not silently ignored; counts and identifiers are recorded in `summary_json`.
- This keeps comparison deterministic and avoids inventing alignment heuristics for mismatched universes.

## Idempotency

- `portfolio_analytics_run_id` is derived from `source_run_ids + source_type + analytics_config_hash`.
- `portfolio_compare_run_id` is derived from `left_run_id + right_run_id + compare_basis + analytics_config_hash`.
- Existing successful analytics / compare runs are reused by default.
- Failed runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
