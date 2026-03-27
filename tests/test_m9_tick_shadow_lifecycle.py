from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.shadow_artifacts import ShadowArtifactStore
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_tick_shadow_ignores_lunch_ticks_and_fills_in_afternoon_session(tmp_path: Path) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.00",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={},
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="lunch_ignored.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 11, 45, tzinfo=CN_TZ),
                last_price="9.90",
                bid_price_1="9.89",
                ask_price_1="9.90",
            ),
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 13, 1, tzinfo=CN_TZ),
                last_price="9.95",
                bid_price_1="9.94",
                ask_price_1="9.95",
            ),
        ],
    )

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_input_path=tick_path,
    )

    store = ShadowArtifactStore(workspace)
    fills = store.load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    assert fills.iloc[0]["fill_dt"] == datetime(2026, 3, 26, 13, 1, tzinfo=CN_TZ)


def test_tick_shadow_expires_when_no_valid_session_cross_occurs(tmp_path: Path) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.00",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={},
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="expire_end_session.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 8, 59, tzinfo=CN_TZ),
                last_price="9.90",
                bid_price_1="9.89",
                ask_price_1="9.90",
            ),
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 13, 5, tzinfo=CN_TZ),
                last_price="10.20",
                bid_price_1="10.19",
                ask_price_1="10.20",
            ),
        ],
    )

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_input_path=tick_path,
    )

    store = ShadowArtifactStore(workspace)
    events = store.load_order_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    assert "expired_end_of_session" in set(events["event_type"])
    assert "filled" not in set(events["event_type"])
