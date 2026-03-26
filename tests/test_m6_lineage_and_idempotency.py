from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from apps.trade_server.app.planning.ingest import ingest_execution_task_dry_run
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.lineage import (
    resolve_execution_task_lineage,
    resolve_order_intent_lineage,
    resolve_target_weight_lineage,
)
from libs.planning.rebalance import plan_rebalance
from libs.planning.target_weights import build_target_weights
from tests.planning_helpers import prepare_m6_workspace


def test_lineage_and_idempotency_across_m6_bridge(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace = prepare_m6_workspace(tmp_path)

    first_target = build_target_weights(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        approved_by="local_smoke",
    )
    second_target = build_target_weights(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        approved_by="local_smoke",
    )
    assert second_target["reused"] is True

    first_plan = plan_rebalance(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
    )
    second_plan = plan_rebalance(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
    )
    assert second_plan["reused"] is True

    ingest_execution_task_dry_run(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=str(first_plan["execution_task_id"]),
    )
    store = PlanningArtifactStore(workspace)
    target_lineage = resolve_target_weight_lineage(
        store,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        strategy_run_id=str(first_target["strategy_run_id"]),
    )
    task_lineage = resolve_execution_task_lineage(
        store,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=str(first_plan["execution_task_id"]),
    )
    preview_lineage = resolve_order_intent_lineage(
        store,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=str(first_plan["execution_task_id"]),
    )
    assert target_lineage.prediction_run_id.startswith("model_")
    assert target_lineage.source_standard_build_run_id == "research_sample_standard_v1"
    assert task_lineage.source_target_weight_hash
    assert preview_lineage.execution_task_id == str(first_plan["execution_task_id"])
