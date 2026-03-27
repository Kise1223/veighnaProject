from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.shadow.session import (
    load_shadow_session_reconcile,
    run_shadow_session,
)
from libs.common.time import CN_TZ
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.execution.shadow_lineage import resolve_shadow_run_lineage
from libs.execution.shadow_schemas import ShadowRunStatus
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_tick_shadow_idempotency_uses_tick_source_hash_and_force_semantics(tmp_path: Path) -> None:
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
        bars_by_instrument={},
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="idempotency_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 10, 0, tzinfo=CN_TZ),
                last_price="10.00",
                bid_price_1="9.99",
                ask_price_1="10.00",
            )
        ],
    )

    first = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_input_path=tick_path,
    )
    second = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_input_path=tick_path,
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
    assert lineage.tick_source_hash is not None

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
        market_replay_mode="ticks_l1",
        tick_input_path=tick_path,
    )
    forced = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_input_path=tick_path,
        force=True,
    )

    assert rerun.reused is False
    assert forced.reused is False

    write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="idempotency_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 10, 1, tzinfo=CN_TZ),
                last_price="9.99",
                bid_price_1="9.98",
                ask_price_1="9.99",
            )
        ],
    )
    changed = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_input_path=tick_path,
    )

    assert changed.reused is False
    assert changed.shadow_run_id != first.shadow_run_id
    latest_payload = load_shadow_session_reconcile(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
    )
    assert latest_payload["shadow_run"]["shadow_run_id"] == changed.shadow_run_id
