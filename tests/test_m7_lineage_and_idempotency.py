from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from apps.trade_server.app.paper.runner import run_paper_execution
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.lineage import resolve_paper_run_lineage
from libs.execution.schemas import PaperRunStatus
from tests.execution_helpers import prepare_m7_workspace


def test_lineage_and_idempotency_for_paper_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)

    first = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    second = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    assert second.reused is True

    lineage = resolve_paper_run_lineage(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=first.paper_run_id,
    )
    assert lineage.execution_task_id == execution_task_id
    assert lineage.strategy_run_id.startswith("strategy_")
    assert lineage.source_prediction_run_id.startswith("model_")

    store = ExecutionArtifactStore(workspace)
    failed = store.load_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=first.paper_run_id,
    ).model_copy(update={"status": PaperRunStatus.FAILED})
    store.save_run(failed)
    rerun = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    forced = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        force=True,
    )

    assert rerun.reused is False
    assert forced.reused is False
    final_run = store.load_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=first.paper_run_id,
    )
    assert final_run.status == PaperRunStatus.SUCCESS
