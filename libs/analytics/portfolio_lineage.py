"""Lineage helpers for M12 portfolio analytics artifacts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.analytics.portfolio_schemas import PortfolioAnalyticsLineage, PortfolioCompareLineage


def resolve_portfolio_analytics_lineage(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    portfolio_analytics_run_id: str,
) -> PortfolioAnalyticsLineage:
    store = PortfolioAnalyticsArtifactStore(project_root)
    manifest = store.load_portfolio_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        portfolio_analytics_run_id=portfolio_analytics_run_id,
    )
    return PortfolioAnalyticsLineage(
        portfolio_analytics_run_id=manifest.portfolio_analytics_run_id,
        source_run_ids=manifest.source_run_ids,
        source_execution_task_id=manifest.source_execution_task_id,
        source_strategy_run_id=manifest.source_strategy_run_id,
        source_prediction_run_id=manifest.source_prediction_run_id,
        source_qlib_export_run_id=manifest.source_qlib_export_run_id,
        source_standard_build_run_id=manifest.source_standard_build_run_id,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        summary_file_path=manifest.summary_file_path,
        summary_file_hash=manifest.summary_file_hash,
        status=manifest.status,
    )


def resolve_portfolio_compare_lineage(
    *,
    project_root: Path,
    portfolio_compare_run_id: str,
) -> PortfolioCompareLineage:
    store = PortfolioAnalyticsArtifactStore(project_root)
    manifest = store.load_compare_manifest(portfolio_compare_run_id=portfolio_compare_run_id)
    return PortfolioCompareLineage(
        portfolio_compare_run_id=manifest.portfolio_compare_run_id,
        left_run_id=manifest.left_run_id,
        right_run_id=manifest.right_run_id,
        source_execution_task_ids=manifest.source_execution_task_ids,
        source_strategy_run_ids=manifest.source_strategy_run_ids,
        source_prediction_run_ids=manifest.source_prediction_run_ids,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        summary_file_path=manifest.summary_file_path,
        summary_file_hash=manifest.summary_file_hash,
        status=manifest.status,
    )
