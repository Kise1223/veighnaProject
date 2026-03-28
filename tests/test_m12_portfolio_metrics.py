from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from libs.analytics.portfolio import run_portfolio_analytics
from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.analytics.portfolio_normalize import (
    compute_realized_turnover,
    compute_tracking_error_proxy,
)
from tests.m12_portfolio_helpers import prepare_m12_workspace


def test_portfolio_metrics_match_documented_formulas(tmp_path: Path) -> None:
    workspace, ids = prepare_m12_workspace(tmp_path)

    result = run_portfolio_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )
    store = PortfolioAnalyticsArtifactStore(workspace)
    summary = store.load_portfolio_summary(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        portfolio_analytics_run_id=str(result["portfolio_analytics_run_id"]),
    )
    rows = store.load_position_rows(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        portfolio_analytics_run_id=str(result["portfolio_analytics_run_id"]),
    )
    row = rows.iloc[0]

    expected_target_cash_weight = (Decimal("1") - row["target_weight"]).quantize(
        Decimal("0.000001")
    )
    expected_executed_cash_weight = (Decimal("1") - row["executed_weight"]).quantize(
        Decimal("0.000001")
    )
    expected_hhi = (row["executed_weight"] * row["executed_weight"]).quantize(
        Decimal("0.000001")
    )
    expected_realized_turnover = compute_realized_turnover(
        filled_notional_total=summary.filled_notional_total,
        net_liquidation_start=summary.net_liquidation_start,
    )

    assert summary.target_cash_weight == expected_target_cash_weight
    assert summary.executed_cash_weight == expected_executed_cash_weight
    assert summary.top1_concentration == row["executed_weight"]
    assert summary.top3_concentration == row["executed_weight"]
    assert summary.top5_concentration == row["executed_weight"]
    assert summary.hhi_concentration == expected_hhi
    assert summary.tracking_error_proxy == compute_tracking_error_proxy(
        summary.total_weight_drift_l1
    )
    assert summary.realized_turnover == expected_realized_turnover
    assert summary.total_realized_pnl == row["realized_pnl"]
    assert summary.total_unrealized_pnl == row["unrealized_pnl"]
