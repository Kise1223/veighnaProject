# ADR 0015: M15 Rolling Retrain Campaign

- Status: Accepted
- Date: 2026-03-31

## Scope

- `M15` extends `M14` walk-forward campaigns with file-first model refresh scheduling at trade-date boundaries.
- It remains orchestration-plus-analytics only, paper-only, and stops before any live routing, broker sync, optimizer, or daemon work.

## Decision 1: M15 Extends M14 Campaigns Instead Of Replacing Them

- `M15` keeps using the existing `M14` campaign artifacts, day rows, time-series rows, and summary rows.
- The new layer adds companion model-schedule artifacts plus optional M15 fields on campaign rows and summaries.
- Daily execution, portfolio analytics, and benchmark analytics still come from the existing `M5-M13` helpers.

## Decision 2: Support Two Deterministic Schedule Modes First

- `fixed_model` preserves `M14` semantics by resolving one model once for the whole window.
- `retrain_every_n_trade_days` retrains or reuses a deterministic model every `N` trade days and reuses that model on non-refresh days.
- `latest-model` in fixed mode is resolved once at campaign start and then frozen for the full campaign.
- `training_window_mode=expanding_to_prior_day` is the required first mode.

## Decision 3: No Look-Ahead On Retrain Days

- When `schedule_mode=retrain_every_n_trade_days`, the refreshed model for trade date `T` uses `train_end = previous trade date`.
- `train_start` uses the earliest eligible configured dataset start.
- Model switches occur only at trade-date boundaries, never intraday.

## Decision 4: M15 Compare Extends M14 Compare With Schedule Metrics

- `fixed_vs_rolling` and `retrain_1d_vs_retrain_2d` compare runs only across overlapping `trade_date` values.
- Compare continues to reuse the existing M14 campaign summaries and time-series metrics, then adds schedule deltas such as `unique_model_count`, `retrain_count`, and `average_model_age_trade_days`.
- Left-only and right-only dates are recorded in `summary_json`, not silently aligned.

## Idempotency

- `model_schedule_run_id` is derived from the date range, account, basket, schedule mode, resolved fixed/latest model identity, retrain cadence, training window mode, benchmark mode, and campaign config hash.
- `campaign_run_id` for rolling campaigns includes both the campaign execution settings and the resolved `model_schedule_run_id`.
- Successful schedule and campaign runs are reused by default.
- Failed runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
