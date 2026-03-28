# M10 L1 Partial-Fill Tick Shadow Session

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
.\.venv\Scripts\python.exe -m scripts.build_target_weights --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --approved-by local_smoke
.\.venv\Scripts\python.exe -m scripts.plan_rebalance --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.ingest_execution_task --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --dry-run
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model crossing_full_fill_v1
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model l1_partial_fill_v1 --time-in-force DAY --force
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model l1_partial_fill_v1 --time-in-force IOC --force
.\.venv\Scripts\python.exe -m scripts.reconcile_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.list_shadow_sessions
```

## M9 vs M10

- `M9` is tick-driven replay shadow execution with crossing-full-fill semantics.
- `M10` is tick-driven replay shadow execution with L1 top-of-book volume caps, deterministic partial fills, and `DAY/IOC`.
- Both remain paper-only and still finalize into the same `M7` paper ledger and reconcile outputs.

## Tick Fill Rules

- Buy: a tick is eligible when `ask_price_1 <= limit_price`.
- Sell: a tick is eligible when `bid_price_1 >= limit_price`.
- Under `l1_partial_fill_v1`, per-tick fill size is capped by `ask_volume_1` or `bid_volume_1`.
- When the top quote is missing, the model falls back to `last_price` only if the matching L1 volume field is still present; otherwise the tick provides no executable liquidity.
- `crossing_full_fill_v1` remains the backward-compatible full-fill model and ignores the new L1 volume cap.

## Allocation And TIF

- Same-tick competition on the same symbol and side uses deterministic FIFO allocation by `created_at -> creation_seq -> order_id`.
- `DAY`: orders can accumulate fills across multiple valid-session ticks. Remaining quantity expires as `expired_end_of_session`.
- `IOC`: the first eligible tick consumes as much as possible, and any remaining quantity immediately becomes `expired_ioc_remaining`.

## Session Behavior

- Lunch-break and non-session ticks never trigger fills.
- Missing ticks keep the order active until session end, then `expired_end_of_session`.
- The shadow engine remains deterministic and offline-replayable; it does not start a resident daemon and never calls real `send_order`.

## Common Failures

- Missing ticks or no crossing: order expires at session end.
- Cash insufficient: buy-side partial/full fill is skipped by the ledger; `DAY` orders continue until session end, while `IOC` remainder expires immediately once an eligible tick is reached.
- Sellable quantity insufficient: order is rejected up front with `sell_quantity_exceeds_sellable`.
- Missing `previous_close`: order is rejected up front with `previous_close_missing`.
- Missing symbol mapping: order is rejected up front with `symbol_mapping_missing`.

## Idempotency

- `ticks_l1` reuse key now includes `tick_fill_model`, `time_in_force`, and `tick_source_hash`.
- Same-path-but-different-content tick files do not reuse an old run.
- Successful runs are reused by default, failed runs never block reruns, and `--force` rebuilds the same deterministic artifact path.
