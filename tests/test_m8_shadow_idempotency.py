from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.execution.shadow_lineage import resolve_shadow_run_lineage
from libs.execution.shadow_schemas import ShadowRunStatus
from tests.shadow_helpers import make_preview, prepare_shadow_workspace


def test_shadow_session_idempotency_and_lineage(tmp_path: Path) -> None:
    previews = [
        make_preview(
            instrument_key="EQ_SZ_000001",
            symbol="000001",
            exchange="SZSE",
            side="BUY",
            quantity=100,
            reference_price="10.00",
            previous_close="10.00",
            created_at=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
        )
    ]
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=previews,
        bars_by_instrument={
            "EQ_SZ_000001": [
                {
                    "bar_dt": datetime(2026, 3, 26, 10, 0, tzinfo=CN_TZ),
                    "open": 10.01,
                    "high": 10.08,
                    "low": 9.98,
                    "close": 10.05,
                }
            ]
        },
    )

    first = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    second = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )

    assert second.reused is True
    lineage = resolve_shadow_run_lineage(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=first.shadow_run_id,
    )
    assert lineage.execution_task_id == execution_task_id
    assert lineage.strategy_run_id == "strategy_shadow"
    assert lineage.source_prediction_run_id == "model_shadow"

    shadow_store = ShadowArtifactStore(workspace)
    failed = shadow_store.load_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=first.shadow_run_id,
    ).model_copy(update={"status": ShadowRunStatus.FAILED})
    shadow_store.save_run(failed)

    rerun = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    forced = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        force=True,
    )

    assert rerun.reused is False
    assert forced.reused is False
    execution_store = ExecutionArtifactStore(workspace)
    assert execution_store.has_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=first.paper_run_id,
    )

