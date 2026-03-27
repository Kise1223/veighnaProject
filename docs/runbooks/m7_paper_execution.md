# M7 Paper Execution Sandbox

## End-to-End Commands

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample --rebuild
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
.\.venv\Scripts\python.exe -m scripts.build_target_weights --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --approved-by local_smoke
.\.venv\Scripts\python.exe -m scripts.plan_rebalance --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.ingest_execution_task --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --dry-run
.\.venv\Scripts\python.exe -m scripts.run_paper_execution --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.reconcile_paper_run --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --paper-run-id paper_51f72560b628
```

## Input Modes

- Demo sample mode is the default. If no extra path flags are passed, `scripts.run_paper_execution` reads `data/bootstrap/execution_sample/account_demo.json`, `positions_demo.json`, and `market_snapshot_<trade_date>.json`.
- Custom input mode overrides those files explicitly:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_paper_execution --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --account-snapshot-path path\to\account.json --positions-path path\to\positions.json --market-snapshot-path path\to\market_snapshot.json --position-cost-basis-path path\to\position_cost_basis.json
```

- If custom paths are supplied, the runner uses those files first and only falls back to the demo sample for any path you did not provide.
- `--position-cost-basis-path` has highest priority. If it is omitted, the runner looks next to the chosen positions file for `position_cost_basis.json` or `position_cost_basis_demo.json`. If neither exists, average-price seeding falls back to `market_snapshot.previous_close`.

## Reconcile Selectors

- `--paper-run-id` is the most precise selector and should be used whenever you already know the run you want.
- `--execution-task-id` filters paper runs to one execution task. If that still matches multiple runs, add `--latest` or switch to `--paper-run-id`.
- `--latest` resolves the newest run in the current filter scope.
- If you pass no selector and exactly one paper run matches `trade_date + account_id + basket_id`, the CLI still works.
- If multiple runs match and you pass no selector, the CLI now fails fast and tells you to add `--paper-run-id` or `--latest`.

## Paper Boundary

- `scripts.run_paper_execution` only consumes M6 artifacts and local standard bars.
- It never calls real `send_order`.
- Output is limited to paper orders, paper trades, ledger snapshots, and a reconcile report.

## Fill And Ledger Rules

- Orders run in `sell_then_buy` order.
- Default limit price source is `reference_price` from the M6 preview.
- Buy fills when `bar.low <= limit_price`.
- Sell fills when `bar.high >= limit_price`.
- Filled orders trade at `limit_price` on the first crossing bar.
- Ordinary A-share buys are `T+1`, so same-day buy quantity does not increase `sellable_quantity`.
- `T+0` products follow `settlement_type` from master data and increase `sellable_quantity` on same-day buys.

## Common Failures

- Missing bars: order stays `unfilled` with `missing_bars`.
- Cash insufficient: buy order becomes `rejected` with `insufficient_cash_for_paper_buy`.
- Sellable quantity insufficient: sell order becomes `rejected` with `sell_quantity_exceeds_sellable`.
- Missing `previous_close`: order becomes `rejected` with `previous_close_missing`.
- Missing symbol mapping: order becomes `rejected` with `symbol_mapping_missing`.

## Idempotency And Manifest Fallback

- Re-running the same `execution_task_id + fill_model_config_hash + market_data_hash + account_state_hash` reuses an existing successful paper run.
- Failed runs never block reruns, and `--force` clears and rebuilds the same deterministic artifact path.
- If `bars_1m` manifests are missing or do not match the requested instruments, `market_data_hash` falls back to the actual bar file content hashes plus the current market snapshot payload. It is not path-only.
