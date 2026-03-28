from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore
from tests.m13_benchmark_helpers import prepare_m13_workspace


def test_group_attribution_uses_documented_formulas_and_skips_industry(tmp_path: Path) -> None:
    workspace, ids = prepare_m13_workspace(tmp_path)
    rows = BenchmarkAttributionArtifactStore(workspace).load_benchmark_group_rows(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        benchmark_analytics_run_id=ids["ticks_partial_day_benchmark_analytics_run_id"],
    )

    group_types = set(rows["group_type"].tolist())
    assert {"exchange", "board", "instrument_type"} <= group_types
    assert "industry" not in group_types

    row = rows.iloc[0]
    expected_allocation = (
        (row["executed_weight_sum"] - row["benchmark_weight_sum"]) * row["benchmark_return_proxy"]
    ).quantize(Decimal("0.000001"))
    expected_selection = (
        row["executed_weight_sum"] * (row["portfolio_return_proxy"] - row["benchmark_return_proxy"])
    ).quantize(Decimal("0.000001"))
    assert row["allocation_proxy"] == expected_allocation
    assert row["selection_proxy"] == expected_selection
