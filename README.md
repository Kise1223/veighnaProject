# VeighNa Quant Platform

This repository implements the first seven milestones of an A-share quant platform:

- `M0`: repository bootstrap and local developer tooling
- `M1`: master data schemas, A-share rules engine, and bootstrap loader
- `M2`: VeighNa-compatible OpenCTP gateway skeleton with a mock-first adapter contract
- `M3`: trade server bootstrap around `MainEngine`, `OmsEngine`, and the OpenCTP gateway
- `M4`: parquet-first market data recording, standard ETL, adjustment factors, and qlib provider export
- `M5`: standalone qlib baseline dataset, baseline model training, experiment artifacts, and daily inference
- `M6`: file-first dry-run bridge from prediction to approved target weights, execution tasks, and order-request previews

## Scope Freeze

- Supported in `M0-M6`: SSE/SZSE cash equities and ETFs, including the `M6` dry-run bridge from prediction artifacts to approved target weights, rebalance planning, and order-request previews
- Explicitly out of scope: BSE, convertible bonds, margin trading, stock options, HK Connect, ClickHouse, live order placement, real order routing via `send_order`, long-running signal service processes, multi-account scheduling, complex execution algorithms, optimizers, and large-scale historical backfill

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

See [ADR Template](docs/adr/ADR_TEMPLATE.md), [M0-M2 Contracts](docs/adr/0001_m0_m2_contracts.md), [Trade Server Runtime](docs/adr/0003_trade_server_runtime.md), [M4 Data Foundation](docs/adr/0004_m4_data_foundation.md), and [M5 Qlib Baseline Workflow](docs/adr/0005_m5_qlib_baseline.md) for the frozen implementation contracts.

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
