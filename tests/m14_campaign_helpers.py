from __future__ import annotations

from datetime import date
from pathlib import Path

from apps.research_qlib.workflow import train_baseline_workflow
from libs.analytics.campaign_runner import run_walkforward_campaign
from scripts.build_research_sample import build_research_sample
from tests.research_helpers import prepare_research_workspace


def prepare_m14_workspace(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    workspace = prepare_research_workspace(tmp_path)
    build_research_sample(project_root=workspace, rebuild=True)
    model_run_id = str(train_baseline_workflow(project_root=workspace)["run_id"])
    return workspace, {"model_run_id": model_run_id}


def run_campaign(
    workspace: Path,
    *,
    model_run_id: str,
    execution_source_type: str = "shadow",
    market_replay_mode: str | None = "bars_1m",
    tick_fill_model: str | None = None,
    time_in_force: str | None = None,
    benchmark_source_type: str = "equal_weight_target_universe",
    force: bool = False,
) -> dict[str, object]:
    return run_walkforward_campaign(
        project_root=workspace,
        date_start=date(2026, 3, 24),
        date_end=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        model_run_id=model_run_id,
        execution_source_type=execution_source_type,
        market_replay_mode=market_replay_mode,
        tick_fill_model=tick_fill_model,
        time_in_force=time_in_force,
        benchmark_source_type=benchmark_source_type,
        force=force,
    )
