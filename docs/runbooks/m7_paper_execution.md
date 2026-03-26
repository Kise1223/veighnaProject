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
.\.venv\Scripts\python.exe -m scripts.reconcile_paper_run --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
```

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
