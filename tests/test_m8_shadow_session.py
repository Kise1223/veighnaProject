from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from apps.trade_server.app.shadow.session import load_shadow_session_reconcile, run_shadow_session
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.execution.shadow_schemas import ShadowRunStatus
from tests.execution_helpers import prepare_m7_workspace


def test_shadow_session_reads_m6_artifacts_and_never_calls_send_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    from gateways.vnpy_openctpsec.gateway import OpenCTPSecGateway

    monkeypatch.setattr(OpenCTPSecGateway, "send_order", _fail_send_order, raising=False)
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )

    assert result.send_order_called is False
    assert result.status == ShadowRunStatus.SUCCESS
    shadow_store = ShadowArtifactStore(workspace)
    execution_store = ExecutionArtifactStore(workspace)
    run = shadow_store.load_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    shadow_report = shadow_store.load_report(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    paper_report = execution_store.load_reconcile_report(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    reconcile_payload = load_shadow_session_reconcile(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )

    assert run.execution_task_id == execution_task_id
    assert shadow_report.shadow_run_id == result.shadow_run_id
    assert paper_report.paper_run_id == result.paper_run_id
    assert reconcile_payload["paper_report"] is not None


def _fail_send_order(*args, **kwargs):  # type: ignore[no-untyped-def]
    raise AssertionError("send_order must not be called in M8 shadow execution")
