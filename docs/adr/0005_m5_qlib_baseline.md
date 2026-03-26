# ADR 0005: M5 Qlib Baseline Workflow

- Status: Accepted
- Date: 2026-03-26

## Scope

- `M5` delivers a standalone research runtime, a baseline dataset, a stable baseline model, `model_run` artifacts, `prediction` artifacts, daily inference CLI, and lineage back to the M4 qlib export and standard build.
- It explicitly does not deliver target weights, portfolio optimization, execution planning, signal service, or trade-server order routing.

## Decision 1: M5 Stops At Prediction

- `M5` produces `prediction` artifacts only.
- It does not create `approved_target_weight` or any rebalance plan.
- This keeps the research-to-trading hand-off frozen while avoiding premature M6 scope.

## Decision 2: Research Workflow Stays File-First

- `model_run` and `prediction` bodies are stored under `data/research/`.
- Qlib experiment tracking uses a local file URI under `data/research/mlruns`.
- `infra/sql/postgres/003_research.sql` freezes the relational schema for later deployment, but no database is required for the M5 closed loop.

## Decision 3: Stable Closed Loop Beats Fancy Baselines

- `M5` uses a deterministic linear regression baseline with minimal price-volume expressions.
- It does not depend on `Alpha158` or `LightGBM`.
- The model choice is driven by repeatable local training and low dependency friction, not by headline metrics.

## M5.1 Hardening

- Training run reuse is now status-aware: only `success` runs are auto-reused by deterministic `run_id`.
- Failed runs do not poison the config hash forever. The same `run_id` can be retrained, and `--force` explicitly retrains an already successful run.
- M4 rebuild semantics are frozen as partition replacement. `scripts.build_standard_data --rebuild` clears the target partition and replaces matching manifests before writing new parquet.
- Raw DQ ordering is frozen to ingest order. If raw data carries `ingest_seq`, that sequence is authoritative; otherwise the persisted parquet row order is used.
- Price-band validation is frozen to `previous_close`, not intraday `last_price`, and IPO free-limit windows are counted in trading days rather than natural days.
