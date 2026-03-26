# ADR 0007: M7 Paper Execution Sandbox

- Status: Accepted
- Date: 2026-03-27

## Scope

- `M7` delivers a file-first paper execution sandbox that starts from `execution_task` and `order_intent_preview`, then writes paper orders, paper trades, local ledger snapshots, and a reconcile report.
- It explicitly stops before any live `send_order`, broker routing, or resident signal service.

## Decision 1: M7 Stops At Paper Execution And Local Ledger

- `M7` may simulate order matching, update a local cash and position ledger, and write end-of-run reports.
- It must not call real `gateway.send_order`.
- `trade_server` keeps a separate paper-only entrypoint for this workflow.

## Decision 2: Paper Artifacts Stay File-First

- `paper_execution_run`, `paper_order`, `paper_trade`, `paper_account_snapshot`, `paper_position_snapshot`, and `paper_reconcile_report` are written to `data/trading/`.
- `infra/sql/postgres/005_paper_execution.sql` freezes the relational DDL only.
- No PostgreSQL write path is required for the `M7` closed loop.

## Decision 3: Deterministic Fill Model Beats Realism

- The default fill model is deterministic and bar-driven: sell orders cross when `bar.high >= limit_price`, buy orders cross when `bar.low <= limit_price`.
- Filled orders trade at `limit_price` on the first crossing bar.
- `M7` does not introduce queue models, order-book simulation, optimizers, or complex execution algorithms.

## Idempotency

- `paper_run` reuse key is `execution_task_id + fill_model_config_hash + market_data_hash + account_state_hash`.
- Existing `success` runs are reused by default.
- `failed` runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.

## M7.1 Hardening

- `scripts.run_paper_execution` now supports explicit `account_snapshot`, `positions`, and `market_snapshot` paths. The checked-in demo sample is only the default fallback, not a required hard binding.
- When `bars_1m` manifests are unavailable or do not match the requested instruments, `market_data_hash` falls back to file-content fingerprints plus the current market snapshot payload. Same-path-but-different-content inputs must not reuse an old successful paper run.
