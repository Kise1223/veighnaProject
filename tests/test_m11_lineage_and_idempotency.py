from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.artifacts import ExecutionAnalyticsArtifactStore
from libs.analytics.compare import compare_execution_runs
from libs.analytics.lineage import (
    resolve_execution_analytics_lineage,
    resolve_execution_compare_lineage,
)
from libs.analytics.schemas import ExecutionAnalyticsStatus
from libs.analytics.tca import run_execution_tca
from tests.m11_analytics_helpers import prepare_m11_workspace


def test_execution_analytics_and_compare_support_idempotency_and_lineage(tmp_path: Path) -> None:
    workspace, ids = prepare_m11_workspace(tmp_path)

    first = run_execution_tca(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )
    reused = run_execution_tca(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )
    store = ExecutionAnalyticsArtifactStore(workspace)
    failed = store.load_analytics_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        analytics_run_id=str(first["analytics_run_id"]),
    ).model_copy(update={"status": ExecutionAnalyticsStatus.FAILED})
    store.save_analytics_run(failed)
    rerun = run_execution_tca(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )
    forced = run_execution_tca(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
        force=True,
    )
    lineage = resolve_execution_analytics_lineage(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        analytics_run_id=str(first["analytics_run_id"]),
    )
    compare = compare_execution_runs(
        project_root=workspace,
        left_shadow_run_id=ids["ticks_crossing_run_id"],
        right_shadow_run_id=ids["ticks_partial_day_run_id"],
        compare_basis="full_vs_partial",
    )
    compare_reused = compare_execution_runs(
        project_root=workspace,
        left_shadow_run_id=ids["ticks_crossing_run_id"],
        right_shadow_run_id=ids["ticks_partial_day_run_id"],
        compare_basis="full_vs_partial",
    )
    compare_lineage = resolve_execution_compare_lineage(
        project_root=workspace,
        compare_run_id=str(compare["compare_run_id"]),
    )

    assert reused["reused"] is True
    assert rerun["reused"] is False
    assert forced["reused"] is False
    assert lineage.source_execution_task_id == ids["execution_task_id"]
    assert ids["ticks_partial_day_run_id"] in lineage.source_run_ids
    assert compare_reused["reused"] is True
    assert compare_lineage.left_run_id == ids["ticks_crossing_run_id"]
    assert compare_lineage.right_run_id == ids["ticks_partial_day_run_id"]
