# ADR 0001: M0-M2 Frozen Contracts

- Status: Accepted
- Date: 2026-03-26

## Scope Freeze

- M0-M2 only covers SSE/SZSE cash equities and ETFs.
- Out of scope: BSE, convertible bonds, margin trading, stock options, HK Connect, real historical bar query, real OpenCTP SDK binary binding.

## Gateway Compatibility Contract

- `OpenCTPSecGateway` must subclass VeighNa `BaseGateway` directly.
- All public gateway methods must be thread-safe and non-blocking.
- Adapter callbacks may enter the gateway only through `on_tick`, `on_trade`, `on_order`, `on_position`, `on_account`, `on_contract`, and `on_log`.
- `connect()` performs the initial synchronization for contracts, account, positions, orders, and trades.
- `query_contract` is treated as an internal helper, not a required public gateway API.

## Identity And Time Contract

- The persistent primary key for instruments is `instrument_key`.
- `vt_symbol` is not a database primary key.
- Master data maintains `canonical_symbol`, `vendor_symbol`, `broker_symbol`, `vt_symbol`, and `qlib_symbol`.
- All persisted event timestamps are timezone-aware Asia/Shanghai values.
- Every gateway event persists both `exchange_ts` and `received_ts`.

## Order State Machine Contract

- Status transitions are monotonic:
  - `SUBMITTING -> NOTTRADED|PARTTRADED|REJECTED|CANCELLED`
  - `NOTTRADED -> PARTTRADED|ALLTRADED|CANCELLED`
  - `PARTTRADED -> PARTTRADED|ALLTRADED|CANCELLED`
- Terminal states are immutable.
- Order and trade dedupe keys are mandatory.
- Trade-before-ack, duplicate callbacks, and reconnect replay are first-class scenarios.

## Rules Engine Contract

- Public APIs include `get_trading_phase`, `is_order_accepting`, `is_match_phase`, and `next_actionable_time`.
- Rule snapshots must be queryable by effective date and reject overlapping effective windows.
- Master data and bootstrap files must retain provenance metadata.

## Cost Model Contract

- `calc_cost()` returns itemized breakdown:
  - `commission`
  - `stamp_duty`
  - `handling_fee`
  - `transfer_fee`
  - `reg_fee`
  - `total`
- Cost templates are product-specific and versioned by effective date.

## Compliance And Audit Skeleton

- Raw adapter events are persisted for future replay and audit.
- Per-account counters are reserved for:
  - `per_second_order_count`
  - `per_second_cancel_count`
  - `daily_order_count`
  - `daily_cancel_count`

