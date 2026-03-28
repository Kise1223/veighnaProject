from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.analytics.portfolio_compare import compare_portfolios
from libs.common.time import CN_TZ
from tests.m12_portfolio_helpers import prepare_m12_workspace
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_portfolio_compare_supports_shadow_and_paper_modes(tmp_path: Path) -> None:
    workspace, ids = prepare_m12_workspace(tmp_path)

    bars_vs_ticks = compare_portfolios(
        project_root=workspace,
        left_shadow_run_id=ids["bars_shadow_run_id"],
        right_shadow_run_id=ids["ticks_crossing_run_id"],
        compare_basis="bars_vs_ticks",
    )
    full_vs_partial = compare_portfolios(
        project_root=workspace,
        left_shadow_run_id=ids["ticks_crossing_run_id"],
        right_shadow_run_id=ids["ticks_partial_day_run_id"],
        compare_basis="full_vs_partial",
    )
    day_vs_ioc = compare_portfolios(
        project_root=workspace,
        left_shadow_run_id=ids["ticks_partial_day_run_id"],
        right_shadow_run_id=ids["ticks_partial_ioc_run_id"],
        compare_basis="day_vs_ioc",
    )
    paper_vs_shadow = compare_portfolios(
        project_root=workspace,
        left_paper_run_id=ids["paper_run_id"],
        right_shadow_run_id=ids["bars_shadow_run_id"],
        compare_basis="paper_vs_shadow",
    )
    summary = PortfolioAnalyticsArtifactStore(workspace).load_compare_summary(
        portfolio_compare_run_id=str(day_vs_ioc["portfolio_compare_run_id"])
    )

    assert bars_vs_ticks["row_count"] >= 6
    assert full_vs_partial["row_count"] >= 6
    assert day_vs_ioc["row_count"] >= 6
    assert paper_vs_shadow["row_count"] >= 6
    assert summary.comparable_count == 1
    assert summary.summary_json["left_only_count"] == 0
    assert summary.summary_json["right_only_count"] == 0


def test_portfolio_compare_records_unmatched_instruments(tmp_path: Path) -> None:
    left_preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.00",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
        basket_id="left_basket",
        execution_task_id="left_task",
        strategy_run_id="left_strategy",
    )
    workspace, left_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[left_preview],
        bars_by_instrument={},
        execution_task_id="left_task",
        strategy_run_id="left_strategy",
    )
    left_ticks = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="left_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="10.00",
                bid_price_1="9.99",
                ask_price_1="10.00",
                bid_volume_1="100",
                ask_volume_1="100",
            )
        ],
    )
    left_run = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="left_basket",
        execution_task_id=left_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=left_ticks,
    )
    right_preview = make_preview(
        instrument_key="ETF_SH_510300",
        symbol="510300",
        exchange="SSE",
        side="SELL",
        quantity=100,
        reference_price="2.00",
        previous_close="2.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
        basket_id="right_basket",
        execution_task_id="right_task",
        strategy_run_id="right_strategy",
    )
    _, right_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[right_preview],
        bars_by_instrument={},
        positions_payload=[
            {"instrument_key": "ETF_SH_510300", "total_quantity": 100, "sellable_quantity": 100}
        ],
        execution_task_id="right_task",
        strategy_run_id="right_strategy",
    )
    right_ticks = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="right_ticks.json",
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
            )
        ],
    )
    right_run = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="right_basket",
        execution_task_id=right_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=right_ticks,
    )
    compare = compare_portfolios(
        project_root=workspace,
        left_shadow_run_id=left_run.shadow_run_id,
        right_shadow_run_id=right_run.shadow_run_id,
        compare_basis="bars_vs_ticks",
    )
    summary = PortfolioAnalyticsArtifactStore(workspace).load_compare_summary(
        portfolio_compare_run_id=str(compare["portfolio_compare_run_id"])
    )

    assert summary.comparable_count == 0
    assert summary.summary_json["left_only_count"] == 1
    assert summary.summary_json["right_only_count"] == 1
