from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.shadow_artifacts import ShadowArtifactStore
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_l1_partial_fill_allocates_top_of_book_volume_in_fifo_order(tmp_path: Path) -> None:
    previews = [
        make_preview(
            instrument_key="EQ_SZ_000001",
            symbol="000001",
            exchange="SZSE",
            side="BUY",
            quantity=200,
            reference_price="10.00",
            previous_close="10.00",
            created_at=datetime(2026, 3, 26, 9, 28, tzinfo=CN_TZ),
        ),
        make_preview(
            instrument_key="EQ_SZ_000001",
            symbol="000001",
            exchange="SZSE",
            side="BUY",
            quantity=300,
            reference_price="10.00",
            previous_close="10.00",
            created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
        ),
    ]
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=previews,
        bars_by_instrument={},
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="fifo_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="10.00",
                bid_price_1="9.99",
                ask_price_1="10.00",
                bid_volume_1="250",
                ask_volume_1="250",
            ),
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 32, tzinfo=CN_TZ),
                last_price="10.00",
                bid_price_1="9.99",
                ask_price_1="10.00",
                bid_volume_1="50",
                ask_volume_1="50",
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
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="DAY",
        tick_input_path=tick_path,
    )

    fills = ShadowArtifactStore(workspace).load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    orders = ExecutionArtifactStore(workspace).load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )

    first_order_id = orders.sort_values("created_at").iloc[0]["order_id"]
    second_order_id = orders.sort_values("created_at").iloc[1]["order_id"]
    first_order_fills = list(fills[fills["order_id"] == first_order_id]["quantity"])
    second_order_fills = list(fills[fills["order_id"] == second_order_id]["quantity"])
    second_order = orders[orders["order_id"] == second_order_id].iloc[0]

    assert first_order_fills == [200]
    assert second_order_fills == [50, 50]
    assert second_order["status"] == "partially_filled"
    assert second_order["status_reason"] == "expired_end_of_session"
