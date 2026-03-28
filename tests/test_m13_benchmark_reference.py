from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from libs.analytics.benchmark_artifacts import BenchmarkReferenceArtifactStore
from libs.analytics.benchmark_attribution import build_benchmark_reference
from tests.m13_benchmark_helpers import prepare_m13_workspace


def test_benchmark_reference_builds_equal_weight_target_and_custom_weights(tmp_path: Path) -> None:
    workspace, ids = prepare_m13_workspace(tmp_path)
    store = BenchmarkReferenceArtifactStore(workspace)

    equal_weight = build_benchmark_reference(
        project_root=workspace,
        portfolio_analytics_run_id=ids["ticks_partial_day_portfolio_run_id"],
        source_type="equal_weight_target_universe",
    )
    equal_summary = store.load_benchmark_summary(
        trade_date=date(2026, 3, 26),
        benchmark_run_id=str(equal_weight["benchmark_run_id"]),
    )
    equal_rows = store.load_weight_rows(
        trade_date=date(2026, 3, 26),
        benchmark_run_id=str(equal_weight["benchmark_run_id"]),
    )

    custom_path = workspace / "tmp_benchmark.json"
    custom_path.write_text(
        json.dumps(
            [
                {"instrument_key": "EQ_SZ_000001", "benchmark_weight": "0.60"},
                {"instrument_key": "ETF_SH_510300", "benchmark_weight": "0.30"},
            ]
        ),
        encoding="utf-8",
    )
    custom = build_benchmark_reference(
        project_root=workspace,
        portfolio_analytics_run_id=ids["ticks_partial_day_portfolio_run_id"],
        source_type="custom_weights",
        benchmark_path=custom_path,
        benchmark_name="custom_test",
    )
    custom_summary = store.load_benchmark_summary(
        trade_date=date(2026, 3, 26),
        benchmark_run_id=str(custom["benchmark_run_id"]),
    )
    custom_rows = store.load_weight_rows(
        trade_date=date(2026, 3, 26),
        benchmark_run_id=str(custom["benchmark_run_id"]),
    )

    assert equal_rows.shape[0] == 1
    assert equal_rows.iloc[0]["benchmark_weight"] == Decimal("1.000000")
    assert equal_summary.benchmark_cash_weight == Decimal("0.000000")
    assert custom_rows.shape[0] == 2
    assert custom_rows.iloc[0]["benchmark_weight"] == Decimal("0.600000")
    assert custom_rows.iloc[1]["benchmark_weight"] == Decimal("0.300000")
    assert custom_summary.benchmark_cash_weight == Decimal("0.100000")
