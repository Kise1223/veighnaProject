# M10 L1 Partial-Fill Tick Shadow Session

## Scope

- Extend the existing `ticks_l1` shadow session with deterministic L1 top-of-book constrained partial fills.
- Add simple `DAY/IOC` support.
- Keep `bars_1m` and `crossing_full_fill_v1` backward compatible.
- Reuse the existing `M7` paper ledger and reconcile path.

## Deliverables

- Shadow-session config fields for `tick_fill_model` and `time_in_force`
- Tick-replay L1 liquidity resolution with volume caps
- Partial-fill order lifecycle and FIFO allocation
- Reconcile/report counts for partially filled orders
- README, ADR, runbook, SQL DDL, and regression tests
