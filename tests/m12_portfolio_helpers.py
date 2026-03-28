from __future__ import annotations

from pathlib import Path

from libs.analytics.tca import run_execution_tca
from tests.m11_analytics_helpers import prepare_m11_workspace


def prepare_m12_workspace(
    tmp_path: Path,
    *,
    with_execution_analytics: bool = True,
) -> tuple[Path, dict[str, str]]:
    workspace, ids = prepare_m11_workspace(tmp_path)
    if with_execution_analytics:
        run_execution_tca(project_root=workspace, paper_run_id=ids["paper_run_id"])
        run_execution_tca(project_root=workspace, shadow_run_id=ids["bars_shadow_run_id"])
        run_execution_tca(project_root=workspace, shadow_run_id=ids["ticks_crossing_run_id"])
        run_execution_tca(project_root=workspace, shadow_run_id=ids["ticks_partial_day_run_id"])
        run_execution_tca(project_root=workspace, shadow_run_id=ids["ticks_partial_ioc_run_id"])
    return workspace, ids
