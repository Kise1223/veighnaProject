# VeighNa Quant Platform

This repository implements the first five milestones of an A-share quant platform:

- `M0`: repository bootstrap and local developer tooling
- `M1`: master data schemas, A-share rules engine, and bootstrap loader
- `M2`: VeighNa-compatible OpenCTP gateway skeleton with a mock-first adapter contract
- `M3`: trade server bootstrap around `MainEngine`, `OmsEngine`, and the OpenCTP gateway
- `M4`: parquet-first market data recording, standard ETL, adjustment factors, and qlib provider export

## Scope Freeze

- Supported in `M0-M4`: SSE/SZSE cash equities and ETFs
- Explicitly out of scope: BSE, convertible bonds, margin trading, stock options, HK Connect, ClickHouse, model training, signal service, execution planner, large-scale historical backfill

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

## Repository Layout

```text
apps/trade_server/      trade process bootstrap, health endpoint, and runtime config
gateways/             VeighNa-compatible gateway packages
libs/common/          shared logging and time helpers
libs/marketdata/      M4 recorder, ETL, DQ, adjustment, and qlib export helpers
libs/schemas/         pydantic schemas and canonical identifiers
libs/rules_engine/    A-share rule snapshots, phases, validation, and costs
infra/sql/postgres/   bootstrap SQL and schema definitions
data/master/bootstrap/ versioned seed master data and provenance metadata
data/marketdata/bootstrap/ sample corporate actions used by the M4 closed loop
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

See [ADR Template](docs/adr/ADR_TEMPLATE.md), [M0-M2 Contracts](docs/adr/0001_m0_m2_contracts.md), [Trade Server Runtime](docs/adr/0003_trade_server_runtime.md), and [M4 Data Foundation](docs/adr/0004_m4_data_foundation.md) for the frozen implementation contracts.
