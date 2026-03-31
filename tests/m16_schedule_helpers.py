from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from apps.research_qlib.workflow import train_baseline_workflow
from libs.analytics.model_schedule_artifacts import ModelScheduleArtifactStore
from libs.analytics.rolling_campaign_runner import run_rolling_campaign
from scripts.build_research_sample import build_research_sample
from tests.research_helpers import prepare_research_workspace


def prepare_m16_workspace(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    workspace = prepare_research_workspace(tmp_path)
    build_research_sample(project_root=workspace, rebuild=True)
    fixed_model_run_id = str(train_baseline_workflow(project_root=workspace)["run_id"])
    return workspace, {"fixed_model_run_id": fixed_model_run_id}


def run_fixed_campaign(
    workspace: Path,
    *,
    model_run_id: str | None = None,
    latest_model: bool = False,
    benchmark_source_type: str = "equal_weight_target_universe",
    date_start: date = date(2026, 3, 24),
    date_end: date = date(2026, 3, 26),
    force: bool = False,
) -> dict[str, object]:
    return run_rolling_campaign(
        project_root=workspace,
        date_start=date_start,
        date_end=date_end,
        account_id="demo_equity",
        basket_id="baseline_long_only",
        schedule_mode="fixed_model",
        model_run_id=model_run_id,
        latest_model=latest_model,
        execution_source_type="shadow",
        market_replay_mode="bars_1m",
        benchmark_source_type=benchmark_source_type,
        force=force,
    )


def run_retrain_campaign(
    workspace: Path,
    *,
    retrain_every_n_trade_days: int,
    training_window_mode: str = "expanding_to_prior_day",
    lookback_trade_days: int | None = None,
    benchmark_source_type: str = "equal_weight_target_universe",
    date_start: date = date(2026, 3, 24),
    date_end: date = date(2026, 3, 26),
    force: bool = False,
) -> dict[str, object]:
    return run_rolling_campaign(
        project_root=workspace,
        date_start=date_start,
        date_end=date_end,
        account_id="demo_equity",
        basket_id="baseline_long_only",
        schedule_mode="retrain_every_n_trade_days",
        retrain_every_n_trade_days=retrain_every_n_trade_days,
        training_window_mode=training_window_mode,
        lookback_trade_days=lookback_trade_days,
        execution_source_type="shadow",
        market_replay_mode="bars_1m",
        benchmark_source_type=benchmark_source_type,
        force=force,
    )


def build_explicit_schedule_file(
    workspace: Path,
    *,
    rows: list[dict[str, str]],
    filename: str = "explicit_schedule.json",
) -> Path:
    path = workspace / "data" / "bootstrap" / "model_schedule" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"schedule": rows}, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run_explicit_campaign(
    workspace: Path,
    *,
    schedule_path: Path,
    benchmark_source_type: str = "equal_weight_target_universe",
    date_start: date = date(2026, 3, 24),
    date_end: date = date(2026, 3, 26),
    force: bool = False,
) -> dict[str, object]:
    return run_rolling_campaign(
        project_root=workspace,
        date_start=date_start,
        date_end=date_end,
        account_id="demo_equity",
        basket_id="baseline_long_only",
        schedule_mode="explicit_model_schedule",
        schedule_path=schedule_path,
        execution_source_type="shadow",
        market_replay_mode="bars_1m",
        benchmark_source_type=benchmark_source_type,
        force=force,
    )


def load_schedule_day_rows(workspace: Path, model_schedule_run_id: str):
    store = ModelScheduleArtifactStore(workspace)
    manifest = next(
        item for item in store.list_schedule_manifests() if item.model_schedule_run_id == model_schedule_run_id
    )
    return store.load_day_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        model_schedule_run_id=manifest.model_schedule_run_id,
    ).sort_values("trade_date")
