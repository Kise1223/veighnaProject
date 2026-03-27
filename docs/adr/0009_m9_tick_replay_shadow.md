# ADR 0009: M9 Tick-Replay Shadow Session

- Status: Accepted
- Date: 2026-03-27

## Scope

- `M9` extends the existing `M8` shadow session from `bars_1m` replay to deterministic `ticks_l1` replay.
- It consumes the same `execution_task` and `order_intent_preview` artifacts, emits the same shadow order/fill artifacts, and finalizes into the same `M7` paper ledger and reconcile outputs.
- It remains paper-only and explicitly stops before live routing or broker sync.

## Decision 1: Extend M8 Instead Of Building A Parallel Tick Engine

- `ticks_l1` is a new `market_replay_mode` on the existing shadow session contract.
- `bars_1m` remains the default and stays backward compatible.
- Shadow runs keep the same artifact tree, with `market_replay_mode` and `tick_source_hash` distinguishing tick-driven runs.

## Decision 2: Tick Fills Stay Deterministic And Session-Aware

- Buy orders fill on the first valid-session tick where `ask_price_1 <= limit_price`, using `ask_price_1` as fill price.
- Sell orders fill on the first valid-session tick where `bid_price_1 >= limit_price`, using `bid_price_1` as fill price.
- If the top quote is missing, the first version falls back to `last_price` when it crosses the limit.
- When `limit_price_source=previous_close`, the engine uses the resolved `previous_close` from `market snapshot.previous_close -> preview.previous_close` for both the final order fields and `source_order_intent_hash`, so order lineage matches the effective parameters.
- Lunch break and other non-session ticks do not trigger fills because session checks keep reusing the existing trading-phase rules.
- No queue simulation, stochastic fills, or complex partial-fill model is introduced in `M9`.

## Decision 3: Tick Replay Reuses The Existing Ledger And Remains File-First

- Tick-driven shadow fills still end in the existing `M7` paper ledger and reconcile shapes.
- Costs keep reusing `calc_cost`, and sellability keeps reusing master-data `settlement_type` plus the current rules engine.
- `infra/sql/postgres/007_tick_replay_shadow.sql` freezes the minimum relational extension only; the working closed loop stays file-first.

## Idempotency

- Tick-driven reuse key is `execution_task_id + fill_model_config_hash + market_data_hash + account_state_hash + market_replay_mode + tick_source_hash`.
- Existing successful runs are reused by default.
- Failed runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
