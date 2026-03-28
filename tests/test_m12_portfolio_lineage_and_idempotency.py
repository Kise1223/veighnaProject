from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.portfolio import run_portfolio_analytics
from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.analytics.portfolio_compare import compare_portfolios
from libs.analytics.portfolio_lineage import (
    resolve_portfolio_analytics_lineage,
    resolve_portfolio_compare_lineage,
)
from libs.analytics.portfolio_schemas import PortfolioAnalyticsStatus
from tests.m12_portfolio_helpers import prepare_m12_workspace


def test_portfolio_analytics_and_compare_support_idempotency_and_lineage(tmp_path: Path) -> None:
    workspace, ids = prepare_m12_workspace(tmp_path)

    try:
        run_portfolio_analytics(
            project_root=workspace,
            trade_date=date(2026, 3, 26),
            account_id="demo_equity",
            basket_id="baseline_long_only",
        )
    except ValueError as exc:
        assert "multiple execution sources match" in str(exc)
    else:
        raise AssertionError("expected multi-source selection to require --latest")

    latest = run_portfolio_analytics(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        latest=True,
    )
    first = run_portfolio_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )
    reused = run_portfolio_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )
    store = PortfolioAnalyticsArtifactStore(workspace)
    failed = store.load_portfolio_run(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        portfolio_analytics_run_id=str(first["portfolio_analytics_run_id"]),
    ).model_copy(update={"status": PortfolioAnalyticsStatus.FAILED})
    store.save_portfolio_run(failed)
    rerun = run_portfolio_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )
    forced = run_portfolio_analytics(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
        force=True,
    )
    lineage = resolve_portfolio_analytics_lineage(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        portfolio_analytics_run_id=str(first["portfolio_analytics_run_id"]),
    )
    compare = compare_portfolios(
        project_root=workspace,
        left_shadow_run_id=ids["ticks_crossing_run_id"],
        right_shadow_run_id=ids["ticks_partial_day_run_id"],
        compare_basis="full_vs_partial",
    )
    compare_reused = compare_portfolios(
        project_root=workspace,
        left_shadow_run_id=ids["ticks_crossing_run_id"],
        right_shadow_run_id=ids["ticks_partial_day_run_id"],
        compare_basis="full_vs_partial",
    )
    compare_lineage = resolve_portfolio_compare_lineage(
        project_root=workspace,
        portfolio_compare_run_id=str(compare["portfolio_compare_run_id"]),
    )

    assert latest["source_type"] in {"paper_run", "shadow_run"}
    assert reused["reused"] is True
    assert rerun["reused"] is False
    assert forced["reused"] is False
    assert lineage.source_execution_task_id == ids["execution_task_id"]
    assert ids["ticks_partial_day_run_id"] in lineage.source_run_ids
    assert compare_reused["reused"] is True
    assert compare_lineage.left_run_id == ids["ticks_crossing_run_id"]
    assert compare_lineage.right_run_id == ids["ticks_partial_day_run_id"]
