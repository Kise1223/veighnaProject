# ADR 0008: M8 Replay-Driven Shadow Session

- Status: Accepted
- Date: 2026-03-27

## Scope

- `M8` extends the paper-only bridge from `M6/M7` into a replay-driven, session-aware shadow session.
- It consumes `execution_task` and `order_intent_preview`, emits shadow lifecycle events and fill events, and finalizes into `M7`-compatible paper ledger snapshots and reconcile outputs.
- It explicitly stops before live order routing, broker sync, or resident daemons.

## Decision 1: M8 Stays Paper-Only And Stops Before `send_order`

- `M8` may create working orders, incremental fill events, expiries, and final paper ledger snapshots.
- It must not call real `gateway.send_order`.
- `trade_server` exposes a separate paper-only `run_shadow_session` entrypoint for this workflow.

## Decision 2: Replay Drives Lifecycle, But The Fill Model Stays Deterministic

- `M7` is one-shot paper execution over the full bar set.
- `M8` replays bars in timestamp order and advances order state incrementally: `created -> working -> filled|expired_end_of_session|rejected_validation`.
- Lunch break and non-session timestamps are ignored through the existing trading-phase rules, not a new time model.
- The first version remains bar-driven and deterministic: buy fills on the first `bar.low <= limit_price`, sell fills on the first `bar.high >= limit_price`, and the fill price remains `limit_price`.

## Decision 3: M8 Reuses M7 Ledger And Stays File-First

- `shadow_session_run`, `shadow_order_state_event`, `shadow_fill_event`, and `shadow_session_report` are written to `data/trading/`.
- Final account snapshots, position snapshots, and reconcile reports still use the existing `M7` paper ledger and artifact shapes.
- `infra/sql/postgres/006_shadow_session.sql` freezes the relational DDL only; no PostgreSQL write path is required for the `M8` closed loop.

## Idempotency

- `shadow_run` reuse key is `execution_task_id + fill_model_config_hash + market_data_hash + account_state_hash + market_replay_mode`.
- Existing `success` runs are reused by default.
- `failed` runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
