# VeighNa Quant Platform

This repository implements the first sixteen milestones of an A-share quant platform:

- `M0`: repository bootstrap and local developer tooling
- `M1`: master data schemas, A-share rules engine, and bootstrap loader
- `M2`: VeighNa-compatible OpenCTP gateway skeleton with a mock-first adapter contract
- `M3`: trade server bootstrap around `MainEngine`, `OmsEngine`, and the OpenCTP gateway
- `M4`: parquet-first market data recording, standard ETL, adjustment factors, and qlib provider export
- `M5`: standalone qlib baseline dataset, baseline model training, experiment artifacts, and daily inference
- `M6`: file-first dry-run bridge from prediction to approved target weights, execution tasks, and order-request previews
- `M7`: paper-only execution sandbox, deterministic fills, local ledger, and end-of-run reconcile reports
- `M8`: replay-driven, session-aware shadow execution that reuses the paper ledger and reconcile path
- `M9`: deterministic `ticks_l1` replay-driven shadow execution that reuses the same paper ledger and reconcile path
- `M10`: deterministic `ticks_l1` replay-driven shadow execution with L1 top-of-book volume caps, partial fills, and simple `DAY/IOC` semantics
- `M11`: file-first execution analytics / TCA over `M7-M10` paper and shadow artifacts, including cross-run comparison
- `M12`: file-first portfolio / risk analytics over `M6-M11` artifacts, including planned-vs-executed drift and cross-run portfolio comparison
- `M13`: file-first benchmark / attribution analytics on top of `M12` portfolio analytics, including benchmark-relative active share, concentration deltas, and cross-run benchmark comparison
- `M14`: file-first multi-date walk-forward campaign orchestration over existing `M5-M13` steps, including campaign day indexing, time-series analytics, campaign summaries, and cross-campaign comparison
- `M15`: file-first rolling retrain walk-forward campaigns with per-day model refresh scheduling, model-age tracking, and fixed-vs-rolling campaign comparison on top of `M14`
- `M16`: file-first schedule-realism extensions on top of `M15`, adding `rolling_lookback`, `explicit_model_schedule`, no-lookahead audit artifacts, and schedule-mode comparison

## Scope Freeze

- Supported in `M0-M16`: SSE/SZSE cash equities and ETFs, including the `M6` dry-run bridge from prediction artifacts to approved target weights, rebalance planning, and order-request previews, the `M7` one-shot paper execution sandbox and local ledger, the `M8` bar-driven replay shadow session, the `M9` tick-driven replay shadow session, the `M10` L1 top-of-book constrained partial-fill shadow session, the `M11` execution analytics / TCA layer for `M7-M10` artifacts, the `M12` portfolio / risk analytics layer for planned-vs-executed outcome measurement, the `M13` benchmark / attribution analytics layer for benchmark-relative portfolio posture and comparison, the `M14` walk-forward campaign orchestration and time-series analytics layer over existing `M5-M13` artifacts, the `M15` rolling retrain model-schedule layer on top of those campaigns, and the `M16` schedule-realism / no-lookahead audit layer over `M15`
- Explicitly out of scope: BSE, convertible bonds, margin trading, stock options, HK Connect, ClickHouse, live order placement, real order routing via `send_order`, broker sync, long-running signal service processes, multi-account scheduling, optimizers, queue position simulation, full order-book simulation, stochastic fill models, and large-scale historical backfill

## Canonical Interpreter

Use the project virtual environment explicitly:

```powershell
.\.venv\Scripts\python.exe --version
```

Local tooling is exposed through PowerShell:

```powershell
.\scripts\dev.ps1 bootstrap
.\scripts\dev.ps1 lint
.\scripts\dev.ps1 test
.\scripts\dev.ps1 up
```

Trade server bootstrap is exposed separately:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_trade_server --config apps/trade_server/config.example.json --print-health
```

M4 market data tasks are exposed through standalone scripts:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_recorder_smoke
.\.venv\Scripts\python.exe -m scripts.build_standard_data --trade-date 2026-03-25
.\.venv\Scripts\python.exe -m scripts.run_data_dq --trade-date 2026-03-25
.\.venv\Scripts\python.exe -m scripts.export_qlib_provider --freq 1d
.\.venv\Scripts\python.exe -m scripts.export_qlib_provider --freq 1min
```

Install research extras before running M5:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[research]"
```

M5 research tasks are exposed through standalone scripts:

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
.\.venv\Scripts\python.exe -m scripts.list_research_runs
```

## Repository Layout

```text
apps/trade_server/      trade process bootstrap, health endpoint, and runtime config
gateways/             VeighNa-compatible gateway packages
libs/common/          shared logging and time helpers
libs/marketdata/      M4 recorder, ETL, DQ, adjustment, and qlib export helpers
libs/research/        M5 research artifact schemas, lineage, and file-first storage
libs/planning/        M6 target-weight, rebalance, and dry-run bridge helpers
libs/execution/       M7 paper execution, M8 shadow session, fill model, local ledger, and reconcile helpers
libs/analytics/       M11 execution analytics / TCA, M12 portfolio analytics, M13 benchmark / attribution helpers, M14 walk-forward campaigns, M15 rolling model scheduling, and M16 schedule realism audit helpers
libs/schemas/         pydantic schemas and canonical identifiers
libs/rules_engine/    A-share rule snapshots, phases, validation, and costs
infra/sql/postgres/   bootstrap SQL and schema definitions
data/master/bootstrap/ versioned seed master data and provenance metadata
data/marketdata/bootstrap/ sample corporate actions used by the M4 closed loop
data/bootstrap/research_sample/ deterministic M5 sample spec for smoke training
docs/adr/             architecture decisions and frozen contracts
docs/runbooks/        developer and bootstrap runbooks
scripts/              local developer entrypoints and ETL/loader CLIs
```

## Design Commitments

- Research and trading remain decoupled.
- `vt_symbol` is never used as a persistent database primary key.
- Gateway callbacks enter VeighNa only through `on_tick`, `on_trade`, `on_order`, `on_position`, `on_account`, `on_contract`, and `on_log`.
- All event timestamps are timezone-aware `UTC+8`, and both `exchange_ts` and `received_ts` are retained.
- Rule snapshots are versioned by effective date and enforce non-overlap.
- The trade server keeps `MainEngine` and `OmsEngine` as the canonical execution-side runtime and only exposes adapter state through health snapshots.
- Raw market data is append-only parquet with manifests, while standardized layers remain rebuildable from raw plus master data plus corporate actions.
- Qlib is an optional research consumer of exported provider files and is not imported by the trade runtime startup path.
- Research artifacts remain file-first in `data/research/`, and every prediction can be traced back to one `model_run`, one qlib export run, and one standard build run.
- `M7/M8/M9/M10` paper execution remains file-first in `data/trading/` and never calls real `send_order`.
- `M11` execution analytics remains file-first in `data/analytics/` and only analyzes existing `M7-M10` artifacts; it does not rerun execution or route orders.
- `M12` portfolio / risk analytics remains file-first in `data/analytics/` and only analyzes existing `M6-M11` artifacts; it does not replay execution, route orders, or introduce a separate portfolio engine.
- `M13` benchmark / attribution analytics remains file-first in `data/analytics/` and only analyzes existing `M12` portfolio analytics plus benchmark reference artifacts; it does not replay execution, route orders, or introduce a separate attribution engine.
- `M14` walk-forward campaign analytics remains file-first in `data/analytics/` and only orchestrates and aggregates existing `M5-M13` artifact producers across a date range; it does not retrain models on a rolling schedule, replay execution from scratch, or introduce a separate campaign execution stack.
- `M15` rolling retrain campaigns remain file-first in `data/analytics/` and only extend `M14` with trade-date-boundary model refresh scheduling; they do not introduce intraday model switching, a daemon scheduler, or any live routing.
- `M16` schedule realism artifacts remain file-first in `data/analytics/` and only audit or extend existing `M15` schedules; they do not introduce a new execution stack, a live scheduler, or any real routing.

## M5 Workflow

If the repository only contains the tiny M4 smoke sample, build the deterministic research sample first:

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample
```

Then verify provider consistency, train the baseline, and run daily inference:

```powershell
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
```

Training is idempotent by config and lineage hash. Re-running the same baseline uses the existing `model_run` only when that run is already `success`. Failed runs do not block retraining; the next train reuses the deterministic `run_id` but rebuilds the run directory, and `--force` can be used to retrain an already successful run on purpose. Daily inference is idempotent by `trade_date + run_id`; it reuses the existing prediction artifact instead of overwriting it.

`scripts.build_standard_data --rebuild` means partition rebuild, not append. The target `trade_date/exchange/symbol` partition is cleared before rewriting, matching manifests are replaced, and adjustment factors are rebuilt from the deduplicated standard layer. Raw DQ time-order checks follow original ingest order; if `ingest_seq` exists it is treated as the canonical write order, otherwise parquet row order is used.

See [ADR Template](docs/adr/ADR_TEMPLATE.md), [M0-M2 Contracts](docs/adr/0001_m0_m2_contracts.md), [Trade Server Runtime](docs/adr/0003_trade_server_runtime.md), [M4 Data Foundation](docs/adr/0004_m4_data_foundation.md), [M5 Qlib Baseline Workflow](docs/adr/0005_m5_qlib_baseline.md), [M6 Research-to-Trade Bridge](docs/adr/0006_m6_research_trade_bridge.md), [M7 Paper Execution Sandbox](docs/adr/0007_m7_paper_execution.md), [M8 Replay-Driven Shadow Session](docs/adr/0008_m8_shadow_session.md), [M9 Tick-Replay Shadow Session](docs/adr/0009_m9_tick_replay_shadow.md), [M10 L1 Partial-Fill Tick Shadow Session](docs/adr/0010_m10_l1_partial_fill_shadow.md), [M11 Execution Analytics / TCA](docs/adr/0011_m11_execution_analytics_tca.md), [M12 Portfolio / Risk Analytics](docs/adr/0012_m12_portfolio_risk_analytics.md), [M13 Benchmark / Attribution Analytics](docs/adr/0013_m13_benchmark_attribution_analytics.md), [M14 Walk-Forward Campaign](docs/adr/0014_m14_walkforward_campaign.md), [M15 Rolling Retrain Campaign](docs/adr/0015_m15_rolling_retrain_campaign.md), and [M16 Schedule Realism Audit](docs/adr/0016_m16_schedule_realism_audit.md) for the frozen implementation contracts.

## M6 Workflow

Build target weights from a prediction artifact:

```powershell
.\.venv\Scripts\python.exe -m scripts.build_target_weights --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --approved-by local_smoke
```

Then build the dry-run rebalance plan and ingest it into trade-server-side order-request previews:

```powershell
.\.venv\Scripts\python.exe -m scripts.plan_rebalance --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.ingest_execution_task --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --dry-run
.\.venv\Scripts\python.exe -m scripts.list_target_weights
.\.venv\Scripts\python.exe -m scripts.list_execution_tasks
```

M6 keeps `research` and `trade_server` decoupled. The bridge is file-first and dry-run only: it never calls `send_order`.

Target-weight idempotency key is `trade_date + prediction_run_id + account_id + basket_id + config_hash`. Rebalance idempotency key is `source_target_weight_hash + planner_config_hash + account snapshot + positions + market snapshot`. `--force` clears and rebuilds the same deterministic artifact path instead of silently overwriting.

The default reference price for target-quantity conversion is `previous_close`, configured in `configs/planning/rebalance_planner.yaml`. Buy quantities round down to the buy lot, and odd lots are only handled on the sell path.

## M7 Workflow

Run the paper-only execution sandbox after the M6 dry-run bridge:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_paper_execution --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.reconcile_paper_run --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --paper-run-id paper_51f72560b628
.\.venv\Scripts\python.exe -m scripts.list_paper_runs
```

`M7` is paper-only. It reads `execution_task` and `order_intent_preview`, simulates deterministic fills on standardized `1min` bars, writes paper orders and paper trades, updates a local cash and position ledger, and emits an end-of-run reconcile report. It never calls real `send_order`.

`scripts.run_paper_execution` has two input modes. Demo sample mode uses the checked-in execution sample automatically when no extra path flags are provided. Custom input mode overrides the paper-run inputs explicitly:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_paper_execution --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --account-snapshot-path data\bootstrap\execution_sample\account_demo.json --positions-path data\bootstrap\execution_sample\positions_demo.json --market-snapshot-path data\bootstrap\execution_sample\market_snapshot_2026-03-26.json --position-cost-basis-path data\bootstrap\execution_sample\position_cost_basis_demo.json
```

Explicit `--position-cost-basis-path` has highest priority. If it is omitted, `scripts.run_paper_execution` keeps the existing companion fallback: it first looks next to the chosen positions file for `position_cost_basis.json` or `position_cost_basis_demo.json`, and only then falls back to market-snapshot `previous_close` values for any remaining instruments.

The default fill model is `bar_limit_v1` from `configs/execution/paper_fill_model.yaml`. Orders run in `sell_then_buy` order. Buy orders fill when `bar.low <= limit_price`; sell orders fill when `bar.high >= limit_price`; filled orders trade at `limit_price` on the first crossing bar. Missing bars leave orders `unfilled`.

Paper cash and sellability stay deterministic. If cash is insufficient, buy orders are `rejected` with `insufficient_cash_for_paper_buy`. Ordinary A-share buys remain `T+1`, so same-day buys do not increase `sellable_quantity`; instruments that are `T+0` in master data do increase same-day `sellable_quantity`. Paper-run idempotency key is `execution_task_id + fill_model_config_hash + market_data_hash + account_state_hash`; successful runs are reused by default, failed runs can be rerun, and `--force` rebuilds the same artifact path without silent overwrite.

That idempotency rule is unchanged in `M7.1`. When `bars_1m` manifests are missing or do not match the requested instruments, `market_data_hash` falls back to file-content fingerprints plus the current market snapshot payload, so same-path-but-different-content inputs do not incorrectly reuse an old paper run.

`scripts.reconcile_paper_run` no longer silently picks the latest run when the selector is ambiguous. If exactly one paper run matches `trade_date + account_id + basket_id`, the short form still works. If multiple runs match, the CLI now returns a clear error and requires `--paper-run-id` or `--latest`. `--execution-task-id` can be used as a narrower filter, and `--execution-task-id ... --latest` resolves the newest run inside that task.

## M8 Workflow

Run the replay-driven shadow session after the M6 dry-run bridge:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.reconcile_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only
.\.venv\Scripts\python.exe -m scripts.list_shadow_sessions
```

`M7` is one-shot paper execution. `M8` is replay-driven shadow execution: orders enter `working`, replayed `1min` bars advance in time order, fills happen on the first crossing bar inside a valid matching phase, lunch break and other non-session timestamps are ignored, and remaining working orders become `expired_end_of_session`.

`M8` is still paper-only. It never calls real `send_order`, it does not introduce a resident daemon, and it finalizes into the same file-first paper ledger and reconcile shapes used by `M7`.

Shadow-session `previous_close` now uses one priority everywhere in the `M8` engine: current market-snapshot `previous_close` first, preview `previous_close` second. This affects validation and sellability checks in all cases, and it also affects order `limit_price` only when `configs/execution/shadow_session.yaml` sets `limit_price_source=previous_close`. Under the default `reference_price` config, limit-price behavior is unchanged.

Shadow-session idempotency key is `execution_task_id + fill_model_config_hash + market_data_hash + account_state_hash + market_replay_mode`; successful runs are reused by default, failed runs can be rerun, and `--force` rebuilds the same artifact path without silent overwrite.

## M9 Workflow

Run the tick-driven shadow session after the same M6 dry-run bridge:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode bars_1m
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1
.\.venv\Scripts\python.exe -m scripts.reconcile_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.list_shadow_sessions
```

`M8` is bar-driven replay shadow execution. `M9` extends the same shadow-session path to `ticks_l1`. It remains paper-only, deterministic, file-first, and still finalizes into the `M7` paper ledger and reconcile artifacts instead of calling real `send_order`.

`scripts.run_shadow_session` now supports `--market-replay-mode bars_1m|ticks_l1` and `--tick-input-path <path>`. When `ticks_l1` is selected, the tick source resolution order is explicit `--tick-input-path`, then the checked-in deterministic bootstrap sample under `data/bootstrap/shadow_tick_sample/`, then any matching raw `market_ticks` parquet partitions. Tick idempotency adds `tick_source_hash` on top of the existing `market_replay_mode` and market/account hashes, so same-path-but-different-content tick inputs do not reuse an old shadow run.

`scripts.reconcile_shadow_session` now defaults to the newest shadow run inside the current `trade_date + account_id + basket_id` filter, which makes the bare command work after both `bars_1m` and `ticks_l1` runs exist. Use `--shadow-run-id` for exact inspection or `--execution-task-id` to narrow the scope first.

Tick fills remain deterministic and session-aware. Buy orders fill on the first valid-session tick where `ask_price_1 <= limit_price`, using `ask_price_1` as fill price. Sell orders fill on the first valid-session tick where `bid_price_1 >= limit_price`, using `bid_price_1` as fill price. If `ask_price_1` or `bid_price_1` is missing, the engine falls back to `last_price` when the config keeps `tick_price_fallback=last_price`. Lunch-break and non-session ticks never fill orders, and orders that never cross still expire at session end.

`source_order_intent_hash` follows the same effective order parameters that the shadow engine actually uses. When `limit_price_source=previous_close`, both the final `previous_close` and the hash payload use the resolved value with priority `market snapshot.previous_close -> preview.previous_close`. Under the default `reference_price` config, existing hash behavior stays unchanged.

## M10 Workflow

Run the L1 top-of-book constrained partial-fill shadow session on the same `ticks_l1` replay path:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model crossing_full_fill_v1
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model l1_partial_fill_v1 --time-in-force DAY --force
.\.venv\Scripts\python.exe -m scripts.run_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --market-replay-mode ticks_l1 --tick-fill-model l1_partial_fill_v1 --time-in-force IOC --force
.\.venv\Scripts\python.exe -m scripts.reconcile_shadow_session --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.list_shadow_sessions
```

`M9` is tick-driven replay shadow execution with crossing-full-fill semantics. `M10` extends that same `ticks_l1` path with `tick_fill_model=l1_partial_fill_v1`, which enforces L1 top-of-book volume caps, deterministic partial fills, and simple `DAY/IOC` time-in-force without introducing queue simulation or live routing.

Under `l1_partial_fill_v1`, buy orders become eligible when `ask_price_1 <= limit_price`, sell orders become eligible when `bid_price_1 >= limit_price`, and per-tick fill size is capped by `ask_volume_1` or `bid_volume_1`. Competing working orders on the same symbol and side consume that top-of-book volume in deterministic FIFO order by `created_at -> creation_seq -> order_id`. If the top quote is missing, the engine keeps the existing `last_price` fallback only when the matching top-of-book volume field is still present; this keeps the fallback conservative and testable.

`DAY` orders stay active across multiple valid-session ticks, can move through `working -> partially_filled -> filled`, and any remaining quantity expires at session end. `IOC` orders attempt to consume as much top-of-book volume as possible on the first eligible tick and immediately move any remainder to `expired_ioc_remaining`. Lunch-break and non-session ticks never trigger fills in either mode.

M10 shadow-session idempotency extends the existing replay key with `tick_fill_model`, `time_in_force`, and `tick_source_hash` for `ticks_l1` runs. Successful runs are reused by default, failed runs can be rerun, and `--force` rebuilds the same deterministic artifact path without silent overwrite.

## M11 Workflow

Run single-run execution analytics / TCA on top of existing `M7-M10` artifacts:

```powershell
.\.venv\Scripts\python.exe -m scripts.list_shadow_sessions
.\.venv\Scripts\python.exe -m scripts.run_execution_tca --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.run_execution_tca --paper-run-id paper_25100177daec
.\.venv\Scripts\python.exe -m scripts.run_execution_tca --shadow-run-id shadow_2d26f3ae3995
.\.venv\Scripts\python.exe -m scripts.list_execution_analytics
```

Run cross-run comparisons across existing paper or shadow runs:

```powershell
.\.venv\Scripts\python.exe -m scripts.compare_execution_runs --left-shadow-run-id shadow_2ddcbde22e19 --right-shadow-run-id shadow_b1d4e224ed39 --compare-basis bars_vs_ticks
.\.venv\Scripts\python.exe -m scripts.compare_execution_runs --left-shadow-run-id shadow_b1d4e224ed39 --right-shadow-run-id shadow_2d26f3ae3995 --compare-basis full_vs_partial
.\.venv\Scripts\python.exe -m scripts.compare_execution_runs --left-shadow-run-id shadow_2d26f3ae3995 --right-shadow-run-id shadow_f108131b2ef9 --compare-basis day_vs_ioc
```

`M11` is analytics-only. It reads existing `M7` paper runs and `M8/M9/M10` shadow runs, normalizes them into a common TCA input view, and writes file-first analytics artifacts under `data/analytics/`. It does not rerun execution, does not call `send_order`, and does not create a parallel execution path.

Single-run TCA metrics include `requested_quantity`, `filled_quantity`, `remaining_quantity`, `fill_rate`, `partial_fill_count`, `avg_fill_price`, `planned_notional`, `filled_notional`, `estimated_cost_total`, `realized_cost_total`, `implementation_shortfall`, `first_fill_dt`, `last_fill_dt`, and `session_end_status`. The deterministic implementation shortfall definition is:

- buy: `(avg_fill_price - reference_price) * filled_quantity + realized_cost_total`
- sell: `(reference_price - avg_fill_price) * filled_quantity + realized_cost_total`
- no fill: `0.00`

`estimated_cost_total` comes from planning / preview artifacts, while `realized_cost_total` is recomputed from realized paper trades or shadow fills. Cross-run comparison works on the intersection of instruments only and records dropped/unmatched instruments in `summary_json`.

For selector semantics, `scripts.run_execution_tca` accepts `--paper-run-id`, `--shadow-run-id`, or `trade_date + account_id + basket_id`. If multiple paper/shadow sources match that date/account/basket selector, the command errors unless `--latest` is passed. Analytics and compare artifacts are idempotent by source run ids plus config hash; successful runs are reused by default, failed runs can be rerun, and `--force` rebuilds the same deterministic artifact path without silent overwrite.

## M12 Workflow

Run portfolio / risk analytics on top of existing `M6-M11` artifacts:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_portfolio_analytics --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest
.\.venv\Scripts\python.exe -m scripts.run_portfolio_analytics --shadow-run-id shadow_2d26f3ae3995
.\.venv\Scripts\python.exe -m scripts.compare_portfolios --left-shadow-run-id shadow_2ddcbde22e19 --right-shadow-run-id shadow_b1d4e224ed39 --compare-basis bars_vs_ticks
.\.venv\Scripts\python.exe -m scripts.compare_portfolios --left-shadow-run-id shadow_b1d4e224ed39 --right-shadow-run-id shadow_2d26f3ae3995 --compare-basis full_vs_partial
.\.venv\Scripts\python.exe -m scripts.compare_portfolios --left-shadow-run-id shadow_2d26f3ae3995 --right-shadow-run-id shadow_f108131b2ef9 --compare-basis day_vs_ioc
.\.venv\Scripts\python.exe -m scripts.list_portfolio_analytics
```

`M12` is analytics-only and still paper-only. It reads approved target weights, execution tasks, order-intent previews, final paper account / position snapshots, and `M11` TCA artifacts when they are available. It does not rerun execution, does not call `send_order`, and does not create a parallel portfolio engine.

Portfolio metrics stay deterministic and explainable:

- `executed_weight = market_value_end / net_liquidation_end`
- `target_cash_weight = max(0, 1 - sum(target_weight))`
- `executed_cash_weight = cash_end / net_liquidation_end`
- `tracking_error_proxy = 0.5 * sum(abs(target_weight - executed_weight))`
- `realized_turnover = filled_notional_total / max(net_liquidation_start, epsilon)`
- `top1/top3/top5` concentration uses the largest executed weights at session end
- `hhi_concentration` is the sum of squared executed weights

Planned versus executed inputs stay explicit:

- `planned_notional_total` prefers planning / preview artifacts when they exist and falls back to `target_weight * net_liquidation_start` only when those planning previews are absent
- `filled_notional_total`, `fill_rate_gross`, and realized execution costs prefer `M11` TCA artifacts when they exist and otherwise derive from the underlying paper/shadow execution artifacts
- `realized_pnl` and `unrealized_pnl` come from the final paper ledger snapshots

Portfolio comparison supports `planned_vs_executed`, `bars_vs_ticks`, `full_vs_partial`, `day_vs_ioc`, and `paper_vs_shadow`. Comparisons operate on the intersection of `instrument_key`; unmatched instruments are recorded in `summary_json` instead of being silently merged.

Selector semantics mirror `M11`: `--paper-run-id` and `--shadow-run-id` are exact selectors, while `trade_date + account_id + basket_id` requires `--latest` when multiple sources match. Portfolio analytics and compare artifacts are idempotent by source run ids plus config hash; successful runs are reused by default, failed runs can rerun, and `--force` rebuilds the same deterministic artifact path without silent overwrite.

## M13 Workflow

Run benchmark reference construction and benchmark-relative attribution analytics on top of existing `M12` portfolio analytics:

```powershell
.\.venv\Scripts\python.exe -m scripts.build_benchmark_reference --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --source-type equal_weight_target_universe --latest
.\.venv\Scripts\python.exe -m scripts.run_benchmark_analytics --trade-date 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest --benchmark-run-id <benchmark_run_id>
.\.venv\Scripts\python.exe -m scripts.run_benchmark_analytics --shadow-run-id shadow_2d26f3ae3995 --benchmark-run-id <benchmark_run_id>
.\.venv\Scripts\python.exe -m scripts.compare_benchmark_analytics --left-benchmark-analytics-run-id <bars_benchmark_analytics_run_id> --right-benchmark-analytics-run-id <ticks_crossing_benchmark_analytics_run_id> --compare-basis bars_vs_ticks
.\.venv\Scripts\python.exe -m scripts.compare_benchmark_analytics --left-benchmark-analytics-run-id <ticks_crossing_benchmark_analytics_run_id> --right-benchmark-analytics-run-id <ticks_partial_day_benchmark_analytics_run_id> --compare-basis full_vs_partial
.\.venv\Scripts\python.exe -m scripts.compare_benchmark_analytics --left-benchmark-analytics-run-id <ticks_partial_day_benchmark_analytics_run_id> --right-benchmark-analytics-run-id <ticks_partial_ioc_benchmark_analytics_run_id> --compare-basis day_vs_ioc
.\.venv\Scripts\python.exe -m scripts.list_benchmark_analytics
```

`M13` is analytics-only and still paper-only. It analyzes benchmark-relative portfolio posture on top of existing `M12` portfolio analytics and benchmark reference artifacts. It does not replay execution, does not call `send_order`, and does not introduce a new execution or attribution engine.

Supported benchmark reference modes are:

- `custom_weights`: load benchmark weights from a local JSON file and normalize them deterministically
- `equal_weight_target_universe`: build an equal-weight benchmark over the positive target-weight universe
- `equal_weight_union`: optional union benchmark over target plus executed holdings when that broader reference is useful

Benchmark-relative metrics stay deterministic and explainable:

- `active_weight_target = target_weight - benchmark_weight`
- `active_weight_executed = executed_weight - benchmark_weight`
- `target_active_share = 0.5 * sum(abs(target_weight - benchmark_weight))`
- `executed_active_share = 0.5 * sum(abs(executed_weight - benchmark_weight))`
- `active_cash_weight = executed_cash_weight - benchmark_cash_weight`
- `instrument_return_proxy = (mark_price_end - previous_close) / max(previous_close, epsilon)`
- `portfolio_contribution_proxy = executed_weight * instrument_return_proxy`
- `benchmark_contribution_proxy = benchmark_weight * instrument_return_proxy`
- `active_contribution_proxy = portfolio_contribution_proxy - benchmark_contribution_proxy`
- `allocation_proxy = (executed_group_weight - benchmark_group_weight) * benchmark_group_return_proxy`
- `selection_proxy = executed_group_weight * (group_return_proxy - benchmark_group_return_proxy)`

Benchmark comparison supports `bars_vs_ticks`, `full_vs_partial`, `day_vs_ioc`, `paper_vs_shadow`, and other benchmark-relative comparisons over the same benchmark. Comparisons operate on the intersection of `instrument_key`; unmatched instruments are recorded in `summary_json` instead of being silently aligned.

Selector semantics mirror `M11/M12`: benchmark analytics can be selected by `--portfolio-analytics-run-id`, `--paper-run-id`, `--shadow-run-id`, or `trade_date + account_id + basket_id + --latest`. If multiple sources match and `--latest` is omitted, the command errors explicitly. Benchmark reference, benchmark analytics, and benchmark compare artifacts are idempotent by source run ids plus config hash; successful runs are reused by default, failed runs can rerun, and `--force` rebuilds the same deterministic artifact path without silent overwrite.

## M14 Workflow

Run a small walk-forward campaign over an explicit trade-date range on top of the existing `M5-M13` chain:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_walkforward_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest-model --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.run_walkforward_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --latest-model --execution-source-type shadow --market-replay-mode ticks_l1 --tick-fill-model l1_partial_fill_v1 --time-in-force DAY --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.list_campaigns
.\.venv\Scripts\python.exe -m scripts.compare_campaigns --left-campaign-run-id <bars_campaign_run_id> --right-campaign-run-id <ticks_partial_day_campaign_run_id> --compare-basis bars_vs_ticks
```

`M14` is orchestration-plus-analytics only and still paper-only. It does not retrain on a rolling schedule, does not call real `send_order`, and does not introduce a new execution stack. Instead, it resolves one explicit `model_run_id` or the latest successful `model_run` once for the full date window, then reuses existing `M5-M13` artifact producers day by day: prediction, target weights, execution task, paper/shadow execution source, `M11` execution analytics, `M12` portfolio analytics, and optional `M13` benchmark analytics.

Campaign selectors and reuse rules stay explicit:

- `--model-run-id` pins one successful model across the whole range.
- `--latest-model` is an explicit alias for the default fallback: use the latest successful `model_run` when no `--model-run-id` is supplied.
- Campaign idempotency key includes `date_start`, `date_end`, `account_id`, `basket_id`, `execution_source_type`, `market_replay_mode`, `tick_fill_model`, `time_in_force`, `benchmark_source_type`, resolved model identity, and `campaign_config_hash`.
- Successful campaigns are reused by default, failed campaigns can rerun, and `--force` rebuilds the campaign artifacts at the same deterministic path without silent overwrite.
- Daily rows record whether a day was `success`, `reused`, or `failed`; a fully reused day is one where every upstream daily artifact was already successful and reused.

Campaign metrics prefer existing `M11/M12/M13` summaries rather than recomputing raw execution:

- `daily_fill_rate`, `daily_filled_notional`, and `daily_realized_cost` come from `M11` execution analytics summaries.
- `daily_weight_drift_l1`, concentration, cash weight, PnL, and `net_liquidation_end` come from `M12` portfolio summaries.
- `daily_active_share` and `daily_active_contribution_proxy` come from `M13` benchmark summaries when benchmark is enabled; they stay `null` when benchmark is disabled.
- `max_drawdown` is the deterministic peak-to-trough drawdown of the campaign `net_liquidation_end` time series.

Cross-campaign comparison supports `bars_vs_ticks`, `full_vs_partial`, `day_vs_ioc`, and `paper_vs_shadow`. Comparison only uses overlapping `trade_date` values; left-only and right-only dates are recorded in `summary_json` instead of being silently aligned.

## M15 Workflow

Run a small rolling retrain campaign over the same deterministic three-day window:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode fixed_model --latest-model --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode retrain_every_n_trade_days --retrain-every-n-trade-days 1 --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.list_model_schedules
.\.venv\Scripts\python.exe -m scripts.compare_rolling_campaigns --left-campaign-run-id <fixed_campaign_run_id> --right-campaign-run-id <rolling_campaign_run_id> --compare-basis fixed_vs_rolling
```

`M15` is still orchestration-plus-analytics only and still paper-only. It extends `M14` with a model refresh schedule at trade-date boundaries. It does not introduce a daemon, intraday model switching, live routing, or real `send_order`.

Supported schedule modes are:

- `fixed_model`: preserve `M14` semantics by using one explicit `--model-run-id`, or by resolving the latest successful model once at campaign start and freezing that identity for the entire window.
- `retrain_every_n_trade_days`: retrain or reuse a deterministic model every `N` trade days, then reuse that most recent successful model on non-refresh days.

Training-window semantics stay explicit and avoid look-ahead:

- Required default mode is `expanding_to_prior_day`.
- For trade date `T`, a retrained model uses `train_end = previous trade date`.
- `train_start` uses the earliest eligible configured sample date in the baseline dataset.
- Model changes happen only at trade-date boundaries, never intraday.

Model schedule metrics are deterministic:

- `model_switch_flag` is `true` only when the resolved model id changes from the previous trade date.
- `model_age_trade_days` is the trade-day distance from `train_end` to the current trade date, clamped at `0` when a fixed model was trained after the day being evaluated.
- `days_since_last_retrain` is `0` on refresh days and increments on later reuse days.
- Campaign summary now includes `unique_model_count`, `retrain_count`, `average_model_age_trade_days`, and `max_model_age_trade_days`.

Rolling compare uses overlapping trade dates only and supports `fixed_vs_rolling` plus `retrain_1d_vs_retrain_2d`. As in `M14`, unmatched dates are recorded in `summary_json` rather than silently aligned.

## M16 Workflow

Run the schedule-realism extensions on top of the same three-day rolling campaign window:

```powershell
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode fixed_model --latest-model --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode retrain_every_n_trade_days --retrain-every-n-trade-days 1 --training-window-mode expanding_to_prior_day --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode retrain_every_n_trade_days --retrain-every-n-trade-days 1 --training-window-mode rolling_lookback --lookback-trade-days 2 --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.build_explicit_model_schedule_sample --date-start 2026-03-24 --date-end 2026-03-26 --source-model-schedule-run-id <source_schedule_run_id> --output data\bootstrap\model_schedule\explicit_schedule_2026-03-24_2026-03-26.json
.\.venv\Scripts\python.exe -m scripts.run_rolling_campaign --date-start 2026-03-24 --date-end 2026-03-26 --account-id demo_equity --basket-id baseline_long_only --schedule-mode explicit_model_schedule --schedule-path data\bootstrap\model_schedule\explicit_schedule_2026-03-24_2026-03-26.json --execution-source-type shadow --market-replay-mode bars_1m --benchmark-source-type equal_weight_target_universe
.\.venv\Scripts\python.exe -m scripts.list_model_schedules
.\.venv\Scripts\python.exe -m scripts.list_schedule_audits
.\.venv\Scripts\python.exe -m scripts.compare_rolling_campaigns --left-campaign-run-id <fixed_campaign_run_id> --right-campaign-run-id <expanding_campaign_run_id> --compare-basis fixed_vs_rolling
.\.venv\Scripts\python.exe -m scripts.compare_rolling_campaigns --left-campaign-run-id <expanding_campaign_run_id> --right-campaign-run-id <rolling_lookback_campaign_run_id> --compare-basis expanding_vs_rolling_lookback
```

`M16` is still orchestration-plus-analytics only and still paper-only. It extends `M15` with more realistic and inspectable schedule semantics:

- `training_window_mode=rolling_lookback`
- `schedule_mode=explicit_model_schedule`
- file-first no-lookahead audit artifacts
- schedule-mode comparison on top of existing rolling campaigns

The three schedule families now behave as follows:

- `fixed_model`: same as `M15`; one fixed model is used across the whole window.
- `fixed_model + latest-model`: resolve the latest successful model once at campaign start and freeze it for the entire campaign. This is convenient, but it is not strict historical OOS for early dates when that model was trained after their previous trade date.
- `retrain_every_n_trade_days + expanding_to_prior_day`: strict historical if `train_end <= previous trade date`.
- `retrain_every_n_trade_days + rolling_lookback`: strict historical if `train_end <= previous trade date`; `train_start` becomes the last `N` trade days ending at `train_end`, bounded by the earliest available eligible training date.
- `explicit_model_schedule`: use exactly the configured `model_run_id` for each `trade_date`, with no fallback to latest model.

No-lookahead audit is now first-class and file-first:

- Each rolling campaign produces a `schedule_audit_run`.
- Per-day audit records store `previous_trade_date`, `strict_no_lookahead_expected`, `strict_no_lookahead_passed`, and `schedule_warning_code`.
- Typical warning codes are `fixed_latest_frozen_campaign_start_non_strict`, `explicit_schedule_no_train_metadata`, `train_end_after_previous_trade_date`, and `missing_train_window_metadata`.

`model_switch_flag` and `model_age_trade_days` are still deterministic:

- `model_switch_flag` is `true` only when the resolved model id changes from the previous trade date.
- `model_age_trade_days` is the trade-day distance from `train_end` to the current `trade_date`, clamped at `0` when the metadata window does not imply an older model.
- `days_since_last_retrain` is `0` on retrain days and increments on later reuse days.

Rolling compare now also supports `expanding_vs_rolling_lookback`, `explicit_schedule_vs_fixed`, and `explicit_schedule_vs_retrain_1d`. As in `M14/M15`, comparisons only use overlapping `trade_date` values and record unmatched dates in `summary_json`.
