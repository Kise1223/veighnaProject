from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from apps.trade_server.app.paper.runner import run_paper_execution
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.schemas import PaperRunStatus
from tests.execution_helpers import prepare_m7_workspace


def test_paper_execution_reads_m6_artifacts_and_never_calls_send_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    from gateways.vnpy_openctpsec.gateway import OpenCTPSecGateway

    monkeypatch.setattr(
        OpenCTPSecGateway,
        "send_order",
        _fail_send_order,
        raising=False,
    )
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    result = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )

    assert result.send_order_called is False
    assert result.status == PaperRunStatus.SUCCESS
    store = ExecutionArtifactStore(workspace)
    run = store.load_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    orders = store.load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    trades = store.load_paper_trades(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=result.paper_run_id,
    )
    report = store.load_reconcile_report(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )

    assert run.execution_task_id == execution_task_id
    assert len(orders) == result.order_count
    assert len(trades) == result.trade_count
    assert account.paper_run_id == result.paper_run_id
    assert report.paper_run_id == result.paper_run_id


def _fail_send_order(*args, **kwargs):  # type: ignore[no-untyped-def]
    raise AssertionError("send_order must not be called in M7 paper execution")
