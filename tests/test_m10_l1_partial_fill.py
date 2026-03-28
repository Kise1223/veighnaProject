from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.shadow_artifacts import ShadowArtifactStore
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_l1_partial_fill_respects_top_of_book_volume_for_buy_and_sell(tmp_path: Path) -> None:
    previews = [
        make_preview(
            instrument_key="ETF_SH_510300",
            symbol="510300",
            exchange="SSE",
            side="SELL",
            quantity=300,
            reference_price="2.00",
            previous_close="2.00",
            created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
        ),
        make_preview(
            instrument_key="EQ_SZ_000001",
            symbol="000001",
            exchange="SZSE",
            side="BUY",
            quantity=700,
            reference_price="12.00",
            previous_close="12.00",
            created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
        ),
    ]
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=previews,
        bars_by_instrument={},
        positions_payload=[
            {"instrument_key": "ETF_SH_510300", "total_quantity": 300, "sellable_quantity": 300}
        ],
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="partial_fill_day.json",
        ticks=[
            make_tick_row(
                instrument_key="ETF_SH_510300",
                symbol="510300",
                exchange="SSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="2.00",
                bid_price_1="2.00",
                ask_price_1="2.01",
                bid_volume_1="100",
                ask_volume_1="100",
            ),
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="12.00",
                bid_price_1="11.99",
                ask_price_1="12.00",
                bid_volume_1="300",
                ask_volume_1="300",
            ),
            make_tick_row(
                instrument_key="ETF_SH_510300",
                symbol="510300",
                exchange="SSE",
                exchange_ts=datetime(2026, 3, 26, 14, 55, tzinfo=CN_TZ),
                last_price="2.01",
                bid_price_1="2.01",
                ask_price_1="2.02",
                bid_volume_1="200",
                ask_volume_1="200",
            ),
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 14, 55, tzinfo=CN_TZ),
                last_price="12.00",
                bid_price_1="11.98",
                ask_price_1="12.00",
                bid_volume_1="400",
                ask_volume_1="200",
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

    shadow_store = ShadowArtifactStore(workspace)
    execution_store = ExecutionArtifactStore(workspace)
    fills = shadow_store.load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    orders = execution_store.load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    report = execution_store.load_reconcile_report(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )

    etf_fills = list(fills[fills["instrument_key"] == "ETF_SH_510300"]["quantity"])
    stock_fills = list(fills[fills["instrument_key"] == "EQ_SZ_000001"]["quantity"])
    etf_order = orders[orders["instrument_key"] == "ETF_SH_510300"].iloc[0]
    stock_order = orders[orders["instrument_key"] == "EQ_SZ_000001"].iloc[0]

    assert etf_fills == [100, 200]
    assert stock_fills == [300, 200]
    assert etf_order["status"] == "filled"
    assert stock_order["status"] == "partially_filled"
    assert stock_order["status_reason"] == "expired_end_of_session"
    assert report.filled_order_count == 1
    assert report.partially_filled_order_count == 1


def test_l1_partial_fill_uses_last_price_fallback_with_volume_cap(tmp_path: Path) -> None:
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
        filename="partial_fill_fallback.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="9.99",
                bid_price_1="9.98",
                ask_price_1=None,
                bid_volume_1="40",
                ask_volume_1="40",
            )
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

    shadow_store = ShadowArtifactStore(workspace)
    execution_store = ExecutionArtifactStore(workspace)
    fills = shadow_store.load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    order = execution_store.load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    ).iloc[0]

    assert list(fills["quantity"]) == [40]
    assert list(fills["price"]) == [Decimal("9.99")]
    assert order["status"] == "partially_filled"
    assert order["status_reason"] == "expired_end_of_session"


def test_crossing_full_fill_model_remains_unchanged(tmp_path: Path) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=700,
        reference_price="12.00",
        previous_close="12.00",
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
        filename="full_fill_compat.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="12.00",
                bid_price_1="11.99",
                ask_price_1="12.00",
                bid_volume_1="300",
                ask_volume_1="300",
            )
        ],
    )

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=tick_path,
    )

    fills = ShadowArtifactStore(workspace).load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    order = ExecutionArtifactStore(workspace).load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    ).iloc[0]

    assert list(fills["quantity"]) == [700]
    assert order["status"] == "filled"
