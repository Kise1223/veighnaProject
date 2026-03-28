# ADR 0011: M11 Execution Analytics / TCA

- Status: Accepted
- Date: 2026-03-28

## Scope

- `M11` adds file-first execution analytics / TCA on top of existing `M7-M10` paper and shadow artifacts.
- It remains analytics-only, does not alter execution behavior, and explicitly stops before any live routing or broker sync.

## Decision 1: Analyze Existing Artifacts Instead Of Replaying Execution

- `M11` only reads `paper_run`, `shadow_run`, paper trades, shadow fills, execution tasks, and order-intent previews that already exist.
- No new execution engine, fill model, or shadow path is introduced.
- The analytics layer stays file-first under `data/analytics/` and does not add PostgreSQL write requirements.

## Decision 2: Use One Deterministic TCA Contract Across `M7-M10`

- `M7` one-shot paper runs and `M8/M9/M10` shadow runs are normalized into one common analytics input view.
- `estimated_cost_total` comes from planning / preview artifacts.
- `realized_cost_total` comes from realized paper trades or shadow fills plus the existing cost model.
- `implementation_shortfall` is deterministic and explicitly defined:
  - buy: `(avg_fill_price - reference_price) * filled_quantity + realized_cost_total`
  - sell: `(reference_price - avg_fill_price) * filled_quantity + realized_cost_total`
  - no fill: `0.00`

## Decision 3: Cross-Run Comparison Uses Comparable Intersections Only

- `bars_vs_ticks`, `full_vs_partial`, `day_vs_ioc`, and `paper_vs_shadow` all compare only the intersection of `instrument_key`.
- Left-only and right-only instruments are not silently ignored; counts and instrument ids are recorded in `summary_json`.
- This keeps comparison deterministic and explainable without inventing alignment heuristics for unmatched symbols.

## Idempotency

- `analytics_run_id` is derived from `source_run_ids + source_type + analytics_config_hash`.
- `compare_run_id` is derived from `left_run_id + right_run_id + compare_basis + analytics_config_hash`.
- Existing successful analytics / compare runs are reused by default.
- Failed runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
