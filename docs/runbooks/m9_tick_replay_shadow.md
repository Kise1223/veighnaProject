# M9 Tick-Replay Shadow Session

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
.\.venv\Scripts\python.exe -m scripts.build_target_weights --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --approved-by local_smoke
.\.venv\Scripts\python.exe -m scripts.plan_rebalance --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.ingest_execution_task --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --dry-run
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1
.\.venv\Scripts\python.exe -m scripts.reconcile_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.list_shadow_sessions
```

- `scripts.reconcile_shadow_session` without selectors now returns the newest shadow run in the current `trade_date + account_id + basket_id` scope.
- Use `--shadow-run-id` when you want an exact run, or `--execution-task-id` to narrow the selection first.

## M8 vs M9

- `M8` is bar-driven replay shadow execution over `bars_1m`.
- `M9` is tick-driven replay shadow execution over `ticks_l1`.
- Both remain paper-only and finalize into the same `M7` paper ledger and reconcile outputs.

## Tick Input Resolution

- `--market-replay-mode ticks_l1` switches the shadow engine to tick replay.
- `--tick-input-path <path>` has highest priority.
- If `--tick-input-path` is omitted, the runner first uses the checked-in deterministic bootstrap sample under `data/bootstrap/shadow_tick_sample/`.
- If no bootstrap sample exists, the runner falls back to matching raw `data/raw/market_ticks` partitions.

## Tick Fill Rules

- Buy: first valid-session tick with `ask_price_1 <= limit_price` fills at `ask_price_1`.
- Sell: first valid-session tick with `bid_price_1 >= limit_price` fills at `bid_price_1`.
- If the top quote is missing, the first version falls back to `last_price` when it crosses the limit.
- No queue model, stochastic behavior, or complex partial fills are introduced.
- When `limit_price_source=previous_close`, the shadow order uses resolved `previous_close` with priority `market snapshot.previous_close -> preview.previous_close`, and `source_order_intent_hash` uses that same resolved value.
- Under the default `reference_price` config, the hash payload keeps the existing preview-based `previous_close` behavior.

## Session Behavior

- Orders still move `created -> working -> filled|expired_end_of_session|rejected_validation`.
- Lunch-break and non-session ticks do not trigger fills.
- Missing or insufficient ticks leave the order `working` until session end, then `expired_end_of_session`.

## Common Failures

- Missing ticks or no crossing: order expires at session end.
- Cash insufficient: buy order remains `working`, then expires at session end because the fill is skipped.
- Sellable quantity insufficient: order is rejected up front with `sell_quantity_exceeds_sellable`.
- Missing `previous_close`: order is rejected up front with `previous_close_missing`.
- Missing symbol mapping: order is rejected up front with `symbol_mapping_missing`.

## Idempotency

- Tick replay adds `tick_source_hash` to the existing shadow-session reuse key.
- Same-path-but-different-content tick files do not reuse an old run.
- Successful runs are reused by default, failed runs never block reruns, and `--force` rebuilds the same deterministic artifact path.
