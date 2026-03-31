# ADR 0016: M16 Schedule Realism Audit

- Status: Accepted
- Date: 2026-04-01

## Scope

- `M16` extends `M15` rolling retrain campaigns with `rolling_lookback`, `explicit_model_schedule`, and first-class no-lookahead audit artifacts.
- It remains orchestration-plus-analytics only, paper-only, and stops before any live routing, broker sync, or daemon work.

## Decision 1: Extend M15 Instead Of Creating A New Campaign Stack

- `M16` keeps using the existing `M15` model-schedule layer and the existing `M14` campaign artifacts.
- The new work adds companion `schedule_audit` artifacts plus small M16-compatible field extensions on schedule and campaign rows.
- Daily execution, portfolio analytics, and benchmark analytics still come from the existing `M5-M13` helpers.

## Decision 2: Add Two Realism Extensions First

- `training_window_mode=rolling_lookback` is now supported for `retrain_every_n_trade_days`.
- `schedule_mode=explicit_model_schedule` is now supported through a simple file-first per-date schedule file.
- `fixed_model + latest-model` remains supported, but the semantics are now explicitly audited as “resolved once at campaign start” and non-strict for early dates when the model window extends past the previous trade date.

## Decision 3: No-Lookahead Is Audited, Not Only Assumed

- Every rolling campaign now produces a file-first schedule audit run.
- Per-day audit evaluates:
  - `previous_trade_date`
  - `strict_no_lookahead_expected`
  - `strict_no_lookahead_passed`
  - `schedule_warning_code`
- Warning codes include `fixed_latest_frozen_campaign_start_non_strict`, `explicit_schedule_no_train_metadata`, `train_end_after_previous_trade_date`, and `missing_train_window_metadata`.

## Decision 4: Schedule Compare Extends Rolling Compare With Audit Metrics

- `fixed_vs_rolling`, `retrain_1d_vs_retrain_2d`, `expanding_vs_rolling_lookback`, `explicit_schedule_vs_fixed`, and `explicit_schedule_vs_retrain_1d` all compare only overlapping `trade_date` values.
- Compare continues to reuse the existing campaign summaries and time-series metrics, then adds schedule deltas such as `strict_fail_day_count`, `warning_day_count`, and model-age metrics.
- Left-only and right-only dates are recorded in `summary_json`, never silently aligned.

## Idempotency

- `model_schedule_run_id` is derived from the date range, account, basket, schedule mode, fixed/latest model identity, retrain cadence, training window mode, lookback length, explicit schedule path/hash, benchmark mode, and campaign config hash.
- `schedule_audit_run_id` is derived from `model_schedule_run_id + audit_config_hash`.
- Rolling compare ids continue to include left/right campaign ids plus compare basis and config hash.
- Successful runs are reused by default.
- Failed runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
