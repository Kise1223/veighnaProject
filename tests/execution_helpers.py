from __future__ import annotations

from datetime import date
from pathlib import Path

from apps.trade_server.app.planning.ingest import ingest_execution_task_dry_run
from libs.planning.rebalance import plan_rebalance
from libs.planning.target_weights import build_target_weights
from tests.planning_helpers import prepare_m6_workspace


def prepare_m7_workspace(tmp_path: Path) -> tuple[Path, str]:
    workspace = prepare_m6_workspace(tmp_path)
    target = build_target_weights(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        approved_by="local_smoke",
    )
    del target
    plan = plan_rebalance(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
    )
    execution_task_id = str(plan["execution_task_id"])
    ingest_execution_task_dry_run(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    return workspace, execution_task_id
