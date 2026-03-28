from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.analytics.portfolio import run_portfolio_analytics
from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.common.time import CN_TZ
from tests.m12_portfolio_helpers import prepare_m12_workspace
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_portfolio_analytics_supports_paper_and_shadow_runs(tmp_path: Path) -> None:
    workspace, ids = prepare_m12_workspace(tmp_path, with_execution_analytics=False)

    paper = run_portfolio_analytics(
        project_root=workspace,
        paper_run_id=ids["paper_run_id"],
    )
    shadow = run_portfolio_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )

    store = PortfolioAnalyticsArtifactStore(workspace)
    paper_summary = store.load_portfolio_summary(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        portfolio_analytics_run_id=str(paper["portfolio_analytics_run_id"]),
    )
    shadow_rows = store.load_position_rows(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        portfolio_analytics_run_id=str(shadow["portfolio_analytics_run_id"]),
    )
    shadow_row = shadow_rows.iloc[0]

    assert paper_summary.holdings_count_target == 1
    assert paper_summary.holdings_count_end == 1
    assert paper_summary.target_cash_weight == Decimal("0.900000")
    assert paper_summary.summary_json["tca_source"] == "derived_from_execution_source"
    assert shadow_row["replay_mode"] == "ticks_l1"
    assert shadow_row["fill_model_name"] == "l1_partial_fill_v1"
    assert shadow_row["fill_rate"] == 1.0


def test_portfolio_analytics_handles_expired_no_fill_runs(tmp_path: Path) -> None:
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
        filename="m12_no_fill_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="10.10",
                bid_price_1="10.09",
                ask_price_1="10.11",
                bid_volume_1="100",
                ask_volume_1="100",
            )
        ],
    )
    shadow = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=tick_path,
    )
    analytics = run_portfolio_analytics(project_root=workspace, shadow_run_id=shadow.shadow_run_id)
    store = PortfolioAnalyticsArtifactStore(workspace)
    rows = store.load_position_rows(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        portfolio_analytics_run_id=str(analytics["portfolio_analytics_run_id"]),
    )
    summary = store.load_portfolio_summary(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        portfolio_analytics_run_id=str(analytics["portfolio_analytics_run_id"]),
    )

    assert rows.iloc[0]["session_end_status"] == "expired_end_of_session"
    assert rows.iloc[0]["executed_weight"] == Decimal("0.000000")
    assert summary.filled_notional_total == Decimal("0.00")
    assert summary.fill_rate_gross == 0.0
    assert summary.total_weight_drift_l1 == Decimal("0.100000")
