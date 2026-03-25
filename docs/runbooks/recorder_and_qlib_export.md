# Recorder And Qlib Export

## Preconditions

- Use `.\.venv\Scripts\python.exe`
- Keep the working directory at the repository root
- Install base dependencies for parquet tasks and install the optional `research` extra before running qlib smoke reads

## Recorder Smoke

```powershell
.\.venv\Scripts\python.exe -m scripts.run_recorder_smoke
```

- Writes append-only raw tick parquet under `data/raw/market_ticks/`
- Writes manifest metadata under `data/manifests/`

## Standard ETL

```powershell
.\.venv\Scripts\python.exe -m scripts.build_standard_data --trade-date 2026-03-25
```

- Builds standardized ticks under `data/standard/ticks/`
- Builds `bars_1m`, `bars_1d`, and adjustment factors under `data/standard/`

## Data Quality

```powershell
.\.venv\Scripts\python.exe -m scripts.run_data_dq --trade-date 2026-03-25
```

- Reads raw parquet for the trade date
- Emits readable JSON reports under `data/dq_reports/`

## Qlib Export

```powershell
.\.venv\Scripts\python.exe -m scripts.export_qlib_provider --freq 1d
.\.venv\Scripts\python.exe -m scripts.export_qlib_provider --freq 1min
```

- Exports provider files under `data/qlib_bin/`
- Runs a smoke read through `qlib.init(provider_uri=..., region="cn")`

## Replay

```powershell
.\.venv\Scripts\python.exe -m scripts.replay_sample --input <parquet-path>
```

- Raw tick parquet replays into `TickData`
- Standardized bar parquet replays into `BarData`
