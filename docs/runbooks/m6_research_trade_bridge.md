# M6 Research-to-Trade Dry-Run Bridge

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
.\.venv\Scripts\python.exe -m scripts.build_target_weights --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --approved-by local_smoke
.\.venv\Scripts\python.exe -m scripts.plan_rebalance --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.ingest_execution_task --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --dry-run
```

## Dry-Run Boundary

- `scripts.ingest_execution_task` only writes dry-run request previews.
- It never calls `send_order`.
- Successful ingestion moves task status from `planned` to `ingested_dry_run`.

## Reference Price And Rounding

- Default reference price is `previous_close`, configured in `configs/planning/rebalance_planner.yaml`.
- Buy target quantities are rounded down to the instrument buy lot.
- Odd-lot handling is sell-only. A `250`-share sell on an A-share stock is previewed as `200 + 50`.

## Common Failures

- Missing `previous_close`: preview row becomes `rejected` with `previous_close_missing`.
- Missing market snapshot: preview row becomes `rejected` with `market_snapshot_missing`.
- Cash insufficient after sequential sells and buys: buy preview becomes `rejected` with `insufficient_cash_for_planned_buy`.
- Sellable quantity insufficient: sell preview becomes `rejected` with `sell_quantity_exceeds_sellable`.

## Idempotency

- Re-running `build_target_weights` with the same `trade_date + prediction_run_id + account_id + basket_id + config_hash` reuses the existing artifact unless `--force` is used.
- Re-running `plan_rebalance` with the same target-weight hash, planner config, account snapshot, positions, and market snapshot reuses the existing task unless `--force` is used.
