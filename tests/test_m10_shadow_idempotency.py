from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.execution.shadow_lineage import resolve_shadow_run_lineage
from libs.execution.shadow_schemas import ShadowRunStatus
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_m10_shadow_idempotency_includes_tick_fill_model_time_in_force_and_tick_source_hash(
    tmp_path: Path,
) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.00",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={},
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="m10_idempotency_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 10, 0, tzinfo=CN_TZ),
                last_price="10.00",
                bid_price_1="9.99",
                ask_price_1="10.00",
                bid_volume_1="40",
                ask_volume_1="40",
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
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="DAY",
        tick_input_path=tick_path,
    )
    reused = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="DAY",
        tick_input_path=tick_path,
    )
    ioc = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="IOC",
        tick_input_path=tick_path,
    )
    full_fill = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=tick_path,
    )

    assert reused.reused is True
    assert ioc.shadow_run_id != first.shadow_run_id
    assert full_fill.shadow_run_id != first.shadow_run_id

    lineage = resolve_shadow_run_lineage(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=first.shadow_run_id,
    )
    assert lineage.tick_fill_model == "l1_partial_fill_v1"
    assert lineage.time_in_force == "DAY"
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
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="DAY",
        tick_input_path=tick_path,
    )
    forced = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="DAY",
        tick_input_path=tick_path,
        force=True,
    )

    assert rerun.reused is False
    assert forced.reused is False

    write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="m10_idempotency_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 10, 1, tzinfo=CN_TZ),
                last_price="9.99",
                bid_price_1="9.98",
                ask_price_1="9.99",
                bid_volume_1="20",
                ask_volume_1="20",
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
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="DAY",
        tick_input_path=tick_path,
    )

    assert changed.reused is False
    assert changed.shadow_run_id != first.shadow_run_id
