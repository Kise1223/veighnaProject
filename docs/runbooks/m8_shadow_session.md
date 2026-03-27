# M8 Replay-Driven Shadow Session

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
.\.venv\Scripts\python.exe -m scripts.build_target_weights --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --approved-by local_smoke
.\.venv\Scripts\python.exe -m scripts.plan_rebalance --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.ingest_execution_task --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --dry-run
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.reconcile_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.list_shadow_sessions
```

## M7 vs M8

- `M7` is one-shot paper execution. It scans the available bars once and decides filled or unfilled directly.
- `M8` is replay-driven shadow execution. Orders enter `working`, bars advance in time order, and fills or expiries happen over the session timeline.
- `M9` extends the same shadow-session path to `ticks_l1`; see `docs/runbooks/m9_tick_replay_shadow.md` for the tick-driven variant.

## Session Lifecycle

- Orders start as `created`.
- Orders move to `working` at the first actionable session timestamp.
- A buy fills on the first replayed bar where `bar.low <= limit_price`.
- A sell fills on the first replayed bar where `bar.high >= limit_price`.
- Orders that never cross remain `working` until session end, then become `expired_end_of_session`.
- Lunch break and other non-session bars do not trigger fills because matching reuses the existing trading-phase rules.
- `previous_close` is resolved uniformly as current market-snapshot `previous_close` first and preview `previous_close` second. That priority always drives validation and sellability checks, and it only changes `limit_price` when `limit_price_source=previous_close`.

## Paper Boundary

- `scripts.run_shadow_session` is paper-only.
- It never calls real `send_order`.
- Final cash, positions, fees, and reconcile outputs reuse the existing `M7` ledger helpers.

## Common Failures

- Missing bars: order stays `working` through replay and expires at session end.
- Cash insufficient: buy order never fills and stays `working`, then expires at session end.
- Sellable quantity insufficient: order is rejected up front with `sell_quantity_exceeds_sellable`.
- Missing `previous_close`: order is rejected up front with `previous_close_missing`.
- Missing symbol mapping: order is rejected up front with `symbol_mapping_missing`.

## Idempotency

- Re-running the same `execution_task_id + fill_model_config_hash + market_data_hash + account_state_hash + market_replay_mode` reuses an existing successful shadow run.
- Failed runs never block reruns.
- `--force` clears and rebuilds the same deterministic artifact path.
