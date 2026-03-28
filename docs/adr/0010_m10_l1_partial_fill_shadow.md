# ADR 0010: M10 L1 Partial-Fill Tick Shadow Session

- Status: Accepted
- Date: 2026-03-28

## Scope

- `M10` extends the existing `M9` `ticks_l1` shadow session with L1 top-of-book volume caps, deterministic partial fills, and simple `DAY/IOC` semantics.
- It remains paper-only, reuses the existing shadow artifacts plus the `M7` paper ledger and reconcile outputs, and explicitly stops before live routing or broker sync.

## Decision 1: Extend The Existing `ticks_l1` Path

- `bars_1m` remains unchanged.
- `ticks_l1` keeps `crossing_full_fill_v1` as the backward-compatible default.
- `l1_partial_fill_v1` is a new deterministic tick fill model on the same shadow-session contract; no parallel shadow system is introduced.

## Decision 2: Partial Fills Are L1-Constrained, Deterministic, And Queue-Free

- Buy orders become eligible on ticks where `ask_price_1 <= limit_price`.
- Sell orders become eligible on ticks where `bid_price_1 >= limit_price`.
- Per-tick fill size is capped by `ask_volume_1` or `bid_volume_1`.
- Competing working orders on the same symbol and side consume that top-of-book size in deterministic FIFO order by `created_at -> creation_seq -> order_id`.
- If the top quote is missing, the engine can still fall back to `last_price`, but only when the corresponding top-of-book volume field is present; this keeps the fallback conservative without inventing hidden liquidity.
- No queue-position model, stochastic behavior, or full order-book simulation is introduced.

## Decision 3: `DAY/IOC` Is The Minimal TIF Set

- `DAY` keeps working orders alive across valid-session ticks until they are fully filled or expire at session end.
- `IOC` consumes as much as possible on the first eligible tick and immediately expires any remaining quantity as `expired_ioc_remaining`.
- Lunch break and other non-session timestamps are still filtered by the existing trading-phase rules.

## Idempotency

- Shadow-session reuse key now explicitly includes `market_replay_mode + tick_fill_model + time_in_force + tick_source_hash` for `ticks_l1` runs.
- Existing successful runs are reused by default.
- Failed runs do not block reruns.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
