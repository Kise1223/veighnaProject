from __future__ import annotations

from datetime import date
from pathlib import Path

from apps.research_qlib.workflow import run_daily_inference, train_baseline_workflow
from scripts.build_research_sample import build_research_sample
from tests.research_helpers import prepare_research_workspace


def prepare_m6_workspace(tmp_path: Path) -> Path:
    workspace = prepare_research_workspace(tmp_path)
    build_research_sample(project_root=workspace, rebuild=True)
    train_baseline_workflow(project_root=workspace, force=True)
    run_daily_inference(project_root=workspace, trade_date=date(2026, 3, 26))
    return workspace
