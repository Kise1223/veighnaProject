from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore
from tests.m13_benchmark_helpers import prepare_m13_workspace


def test_shadow_run_produces_benchmark_analytics_and_active_metrics(tmp_path: Path) -> None:
    workspace, ids = prepare_m13_workspace(tmp_path)
    store = BenchmarkAttributionArtifactStore(workspace)
    summary = store.load_benchmark_summary(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        benchmark_analytics_run_id=ids["ticks_partial_day_benchmark_analytics_run_id"],
    )
    rows = store.load_benchmark_position_rows(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        benchmark_analytics_run_id=ids["ticks_partial_day_benchmark_analytics_run_id"],
    )
    expected_target_active_share = (
        sum((abs(row["target_weight"] - row["benchmark_weight"]) for _, row in rows.iterrows()), Decimal("0"))
        / Decimal("2")
    ).quantize(Decimal("0.000001"))
    expected_executed_active_share = (
        sum((abs(row["executed_weight"] - row["benchmark_weight"]) for _, row in rows.iterrows()), Decimal("0"))
        / Decimal("2")
    ).quantize(Decimal("0.000001"))
    overlap_count = int(
        sum(
            1
            for _, row in rows.iterrows()
            if row["benchmark_weight"] > 0 and row["executed_weight"] > 0
        )
    )

    assert summary.target_active_share == expected_target_active_share
    assert summary.executed_active_share == expected_executed_active_share
    assert summary.holdings_overlap_count == overlap_count
    assert rows.iloc[0]["active_contribution_proxy"] == (
        rows.iloc[0]["portfolio_contribution_proxy"] - rows.iloc[0]["benchmark_contribution_proxy"]
    ).quantize(Decimal("0.000001"))
