from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.artifacts import ExecutionArtifactStore
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_l1_partial_fills_flow_into_m7_ledger_with_t1_and_t0_rules(tmp_path: Path) -> None:
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
        ),
        make_preview(
            instrument_key="ETF_SH_588000_T0",
            symbol="588000",
            exchange="SSE",
            side="BUY",
            quantity=200,
            reference_price="2.00",
            previous_close="2.00",
            created_at=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
        ),
    ]
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=previews,
        bars_by_instrument={},
        account_payload={
            "account_id": "demo_equity",
            "available_cash": "10000.00",
            "frozen_cash": "0.00",
            "nav": "10000.00",
        },
        extra_instrument_rows=[
            "ETF_SH_588000_T0,SSE,588000,ETF,ETF,2020-01-01,,T0,0.001,100,false,0.10,0,false,test_fixture,2026-03-27,2025-01-01,"
        ],
        extra_mapping_rows=[
            "ETF_SH_588000_T0,588000,588000,588000,588000.SSE,SH588000,588000,SSE,test_fixture,2026-03-27,2025-01-01,"
        ],
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="partial_ledger_ticks.json",
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
            ),
            make_tick_row(
                instrument_key="ETF_SH_588000_T0",
                symbol="588000",
                exchange="SSE",
                exchange_ts=datetime(2026, 3, 26, 10, 1, tzinfo=CN_TZ),
                last_price="2.00",
                bid_price_1="2.00",
                ask_price_1="2.00",
                bid_volume_1="200",
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

    store = ExecutionArtifactStore(workspace)
    account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=result.paper_run_id,
    )
    positions = store.load_position_snapshots(
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

    stock = positions[positions["instrument_key"] == "EQ_SZ_000001"].iloc[0]
    etf = positions[positions["instrument_key"] == "ETF_SH_588000_T0"].iloc[0]
    assert stock["quantity"] == 40
    assert stock["sellable_quantity"] == 0
    assert etf["quantity"] == 200
    assert etf["sellable_quantity"] == 200
    assert account.cash_end < account.cash_start
    assert account.fees_total > Decimal("0")
    assert report.filled_order_count == 1
    assert report.partially_filled_order_count == 1
