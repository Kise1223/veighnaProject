from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from libs.analytics.artifacts import ExecutionAnalyticsArtifactStore
from libs.analytics.tca import run_execution_tca
from tests.m11_analytics_helpers import prepare_m11_workspace


def test_execution_tca_metrics_match_documented_shortfall_formula(tmp_path: Path) -> None:
    workspace, ids = prepare_m11_workspace(tmp_path)

    result = run_execution_tca(
        project_root=workspace,
        shadow_run_id=ids["ticks_crossing_run_id"],
    )
    rows = ExecutionAnalyticsArtifactStore(workspace).load_analytics_rows(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        analytics_run_id=str(result["analytics_run_id"]),
    )
    row = rows.iloc[0]

    assert row["filled_notional"] == Decimal("8400.00")
    assert row["avg_fill_price"] == Decimal("12.0000")
    assert row["estimated_cost_total"] == Decimal("5.00")
    assert row["implementation_shortfall"] == row["realized_cost_total"]
