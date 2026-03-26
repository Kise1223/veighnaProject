from __future__ import annotations

from datetime import date
from pathlib import Path

from apps.trade_server.app.bootstrap import TradeServerBootstrap
from apps.trade_server.app.config import GatewayRuntimeConfig, TradeServerConfig
from apps.trade_server.app.planning.ingest import (
    ingest_execution_task_dry_run,
    load_dry_run_order_request_preview,
)
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.rebalance import plan_rebalance
from tests.research_helpers import prepare_research_workspace
from tests.test_m6_rebalance_planner import _seed_target_weights


def test_trade_server_ingest_reads_execution_task_and_never_calls_send_order(
    tmp_path: Path,
) -> None:
    workspace = prepare_research_workspace(tmp_path)
    _seed_target_weights(workspace)
    plan_result = plan_rebalance(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
    )
    runtime = TradeServerBootstrap(
        TradeServerConfig(
            env="test",
            project_root=workspace,
            gateway=GatewayRuntimeConfig(gateway_name="OPENCTPSEC"),
        )
    ).bootstrap()
    try:
        runtime.main_engine.send_order = _fail_send_order  # type: ignore[method-assign]
        ingest_result = ingest_execution_task_dry_run(
            project_root=workspace,
            trade_date=date(2026, 3, 26),
            account_id="demo_equity",
            basket_id="baseline_long_only",
            execution_task_id=str(plan_result["execution_task_id"]),
        )
        assert ingest_result.send_order_called is False
        preview = load_dry_run_order_request_preview(
            project_root=workspace,
            trade_date=date(2026, 3, 26),
            account_id="demo_equity",
            basket_id="baseline_long_only",
            execution_task_id=str(plan_result["execution_task_id"]),
        )
        assert preview
        store = PlanningArtifactStore(workspace)
        task = store.load_execution_task(
            trade_date=date(2026, 3, 26),
            account_id="demo_equity",
            basket_id="baseline_long_only",
            execution_task_id=str(plan_result["execution_task_id"]),
        )
        assert task.status.value == "ingested_dry_run"
    finally:
        runtime.stop()


def _fail_send_order(*args, **kwargs):  # type: ignore[no-untyped-def]
    raise AssertionError("send_order must not be called in M6 dry-run ingestion")
