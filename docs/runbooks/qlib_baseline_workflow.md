# Qlib Baseline Workflow

## Install Research Extras

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[research]"
```

## Build Deterministic Research Sample

Run this when the existing M4 sample is too small for baseline training:

```powershell
.\.venv\Scripts\python.exe -m scripts.build_research_sample
```

The builder writes deterministic `bars_1d`, `bars_1m`, adjustment factors, and a refreshed qlib provider under `data/qlib_bin/`.

## Check Provider Consistency

```powershell
.\.venv\Scripts\python.exe -m scripts.check_qlib_consistency
```

## Train Baseline

```powershell
.\.venv\Scripts\python.exe -m scripts.train_qlib_baseline
```

Artifacts are stored under `data/research/model_runs/<run_id>/`.

## Run Daily Inference

```powershell
.\.venv\Scripts\python.exe -m scripts.run_daily_inference --trade-date 2026-03-26
```

Predictions are written under `data/research/predictions/trade_date=YYYY-MM-DD/run_id=<run_id>/`.

## Idempotency Rules

- Training derives a deterministic `run_id` from the provider lineage and config snapshots.
- Re-running training with the same provider lineage and config reuses the existing successful `model_run` instead of overwriting it.
- Re-running inference for the same `trade_date` and `run_id` reuses the existing prediction artifact instead of overwriting it.
