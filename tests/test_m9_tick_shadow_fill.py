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


def test_tick_shadow_buy_and_sell_cross_quotes(tmp_path: Path) -> None:
    previews = [
        make_preview(
            instrument_key="EQ_SH_600000",
            symbol="600000",
            exchange="SSE",
            side="SELL",
            quantity=100,
            reference_price="10.00",
            previous_close="10.00",
            created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
        ),
        make_preview(
            instrument_key="EQ_SZ_000001",
            symbol="000001",
            exchange="SZSE",
            side="BUY",
            quantity=100,
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
            {"instrument_key": "EQ_SH_600000", "total_quantity": 100, "sellable_quantity": 100}
        ],
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="quotes_cross.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SH_600000",
                symbol="600000",
                exchange="SSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="10.00",
                bid_price_1="10.01",
                ask_price_1="10.02",
            ),
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 32, tzinfo=CN_TZ),
                last_price="11.99",
                bid_price_1="11.98",
                ask_price_1="11.99",
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

    shadow_store = ShadowArtifactStore(workspace)
    execution_store = ExecutionArtifactStore(workspace)
    fills = shadow_store.load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    paper_trades = execution_store.load_paper_trades(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )

    sell_fill = fills[fills["instrument_key"] == "EQ_SH_600000"].iloc[0]
    buy_fill = fills[fills["instrument_key"] == "EQ_SZ_000001"].iloc[0]
    assert result.send_order_called is False
    assert sell_fill["price"] == Decimal("10.01")
    assert buy_fill["price"] == Decimal("11.99")
    assert set(paper_trades["price"]) == {Decimal("10.01"), Decimal("11.99")}


def test_tick_shadow_falls_back_to_last_price_when_quotes_missing(tmp_path: Path) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
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
        filename="fallback_last_price.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="11.98",
                bid_price_1="11.97",
                ask_price_1=None,
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
        tick_input_path=tick_path,
    )

    shadow_store = ShadowArtifactStore(workspace)
    fills = shadow_store.load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    assert fills.iloc[0]["price"] == Decimal("11.98")


def test_shadow_session_bars_mode_still_uses_bar_crossing(tmp_path: Path) -> None:
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
        bars_by_instrument={
            "EQ_SZ_000001": [
                {
                    "bar_dt": datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                    "open": 10.05,
                    "high": 10.06,
                    "low": 9.95,
                    "close": 10.01,
                }
            ]
        },
    )

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="bars_1m",
    )

    shadow_store = ShadowArtifactStore(workspace)
    fills = shadow_store.load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    assert fills.iloc[0]["price"] == Decimal("10.00")
