from __future__ import annotations

from datetime import date
from pathlib import Path

from apps.research_qlib.workflow import train_baseline_workflow
from libs.analytics.rolling_campaign_runner import run_rolling_campaign
from scripts.build_research_sample import build_research_sample
from tests.research_helpers import prepare_research_workspace


def prepare_m15_workspace(tmp_path: Path) -> tuple[Path, dict[str, str]]:
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
    force: bool = False,
) -> dict[str, object]:
    return run_rolling_campaign(
        project_root=workspace,
        date_start=date(2026, 3, 24),
        date_end=date(2026, 3, 26),
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
    benchmark_source_type: str = "equal_weight_target_universe",
    force: bool = False,
) -> dict[str, object]:
    return run_rolling_campaign(
        project_root=workspace,
        date_start=date(2026, 3, 24),
        date_end=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        schedule_mode="retrain_every_n_trade_days",
        retrain_every_n_trade_days=retrain_every_n_trade_days,
        training_window_mode="expanding_to_prior_day",
        execution_source_type="shadow",
        market_replay_mode="bars_1m",
        benchmark_source_type=benchmark_source_type,
        force=force,
    )
