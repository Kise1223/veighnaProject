from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore
from libs.analytics.attribution_lineage import (
    resolve_benchmark_analytics_lineage,
    resolve_benchmark_compare_lineage,
)
from libs.analytics.attribution_schemas import BenchmarkRunStatus
from libs.analytics.benchmark_artifacts import BenchmarkReferenceArtifactStore
from libs.analytics.benchmark_attribution import build_benchmark_reference, run_benchmark_analytics
from libs.analytics.benchmark_compare import compare_benchmark_analytics
from libs.analytics.benchmark_lineage import resolve_benchmark_reference_lineage
from tests.m13_benchmark_helpers import prepare_m13_workspace


def test_benchmark_lineage_and_idempotency(tmp_path: Path) -> None:
    workspace, ids = prepare_m13_workspace(tmp_path)

    try:
        build_benchmark_reference(
            project_root=workspace,
            trade_date=date(2026, 3, 26),
            account_id="demo_equity",
            basket_id="baseline_long_only",
            source_type="equal_weight_target_universe",
        )
    except ValueError as exc:
        assert "multiple execution sources match" in str(exc)
    else:
        raise AssertionError("expected multi-source selection to require --latest")

    reference_first = build_benchmark_reference(
        project_root=workspace,
        portfolio_analytics_run_id=ids["ticks_partial_day_portfolio_run_id"],
        source_type="equal_weight_target_universe",
    )
    reference_reused = build_benchmark_reference(
        project_root=workspace,
        portfolio_analytics_run_id=ids["ticks_partial_day_portfolio_run_id"],
        source_type="equal_weight_target_universe",
    )
    reference_store = BenchmarkReferenceArtifactStore(workspace)
    failed_reference = reference_store.load_benchmark_run(
        trade_date=date(2026, 3, 26),
        benchmark_run_id=str(reference_first["benchmark_run_id"]),
    ).model_copy(update={"status": BenchmarkRunStatus.FAILED})
    reference_store.save_benchmark_run(failed_reference)
    reference_rerun = build_benchmark_reference(
        project_root=workspace,
        portfolio_analytics_run_id=ids["ticks_partial_day_portfolio_run_id"],
        source_type="equal_weight_target_universe",
    )
    reference_forced = build_benchmark_reference(
        project_root=workspace,
        portfolio_analytics_run_id=ids["ticks_partial_day_portfolio_run_id"],
        source_type="equal_weight_target_universe",
        force=True,
    )
    reference_lineage = resolve_benchmark_reference_lineage(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        benchmark_run_id=str(reference_first["benchmark_run_id"]),
    )

    analytics_first = run_benchmark_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
        benchmark_run_id=ids["benchmark_run_id"],
    )
    analytics_reused = run_benchmark_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
        benchmark_run_id=ids["benchmark_run_id"],
    )
    analytics_store = BenchmarkAttributionArtifactStore(workspace)
    failed_analytics = analytics_store.load_benchmark_analytics_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        benchmark_analytics_run_id=str(analytics_first["benchmark_analytics_run_id"]),
    ).model_copy(update={"status": BenchmarkRunStatus.FAILED})
    analytics_store.save_benchmark_analytics_run(failed_analytics)
    analytics_rerun = run_benchmark_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
        benchmark_run_id=ids["benchmark_run_id"],
    )
    analytics_forced = run_benchmark_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
        benchmark_run_id=ids["benchmark_run_id"],
        force=True,
    )
    analytics_lineage = resolve_benchmark_analytics_lineage(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        benchmark_analytics_run_id=str(analytics_first["benchmark_analytics_run_id"]),
    )
    compare_first = compare_benchmark_analytics(
        project_root=workspace,
        left_benchmark_analytics_run_id=ids["ticks_crossing_benchmark_analytics_run_id"],
        right_benchmark_analytics_run_id=ids["ticks_partial_day_benchmark_analytics_run_id"],
        compare_basis="full_vs_partial",
    )
    compare_reused = compare_benchmark_analytics(
        project_root=workspace,
        left_benchmark_analytics_run_id=ids["ticks_crossing_benchmark_analytics_run_id"],
        right_benchmark_analytics_run_id=ids["ticks_partial_day_benchmark_analytics_run_id"],
        compare_basis="full_vs_partial",
    )
    compare_lineage = resolve_benchmark_compare_lineage(
        project_root=workspace,
        benchmark_compare_run_id=str(compare_first["benchmark_compare_run_id"]),
    )

    assert reference_reused["reused"] is True
    assert reference_rerun["reused"] is False
    assert reference_forced["reused"] is False
    assert reference_lineage.source_portfolio_analytics_run_id == ids["ticks_partial_day_portfolio_run_id"]
    assert analytics_reused["reused"] is True
    assert analytics_rerun["reused"] is False
    assert analytics_forced["reused"] is False
    assert analytics_lineage.source_portfolio_analytics_run_id == ids["ticks_partial_day_portfolio_run_id"]
    assert analytics_lineage.source_run_id == ids["ticks_partial_day_run_id"]
    assert compare_reused["reused"] is True
    assert compare_lineage.left_benchmark_analytics_run_id == ids["ticks_crossing_benchmark_analytics_run_id"]
    assert compare_lineage.right_benchmark_analytics_run_id == ids["ticks_partial_day_benchmark_analytics_run_id"]
