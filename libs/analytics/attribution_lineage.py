"""Lineage helpers for M13 benchmark attribution analytics."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore
from libs.analytics.attribution_schemas import BenchmarkAnalyticsLineage, BenchmarkCompareLineage


def resolve_benchmark_analytics_lineage(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    benchmark_analytics_run_id: str,
) -> BenchmarkAnalyticsLineage:
    store = BenchmarkAttributionArtifactStore(project_root)
    manifest = store.load_benchmark_analytics_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        benchmark_analytics_run_id=benchmark_analytics_run_id,
    )
    return BenchmarkAnalyticsLineage(
        benchmark_analytics_run_id=manifest.benchmark_analytics_run_id,
        source_portfolio_analytics_run_id=manifest.source_portfolio_analytics_run_id,
        source_run_id=manifest.source_run_id,
        source_execution_task_id=manifest.source_execution_task_id,
        source_strategy_run_id=manifest.source_strategy_run_id,
        source_prediction_run_id=manifest.source_prediction_run_id,
        benchmark_run_id=manifest.benchmark_run_id,
        source_qlib_export_run_id=manifest.source_qlib_export_run_id,
        source_standard_build_run_id=manifest.source_standard_build_run_id,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        summary_file_path=manifest.summary_file_path,
        summary_file_hash=manifest.summary_file_hash,
        status=manifest.status,
    )


def resolve_benchmark_compare_lineage(
    *,
    project_root: Path,
    benchmark_compare_run_id: str,
) -> BenchmarkCompareLineage:
    store = BenchmarkAttributionArtifactStore(project_root)
    manifest = store.load_benchmark_compare_manifest(benchmark_compare_run_id=benchmark_compare_run_id)
    return BenchmarkCompareLineage(
        benchmark_compare_run_id=manifest.benchmark_compare_run_id,
        left_benchmark_analytics_run_id=manifest.left_benchmark_analytics_run_id,
        right_benchmark_analytics_run_id=manifest.right_benchmark_analytics_run_id,
        source_execution_task_ids=manifest.source_execution_task_ids,
        source_strategy_run_ids=manifest.source_strategy_run_ids,
        source_prediction_run_ids=manifest.source_prediction_run_ids,
        source_portfolio_analytics_run_ids=manifest.source_portfolio_analytics_run_ids,
        benchmark_run_ids=manifest.benchmark_run_ids,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        summary_file_path=manifest.summary_file_path,
        summary_file_hash=manifest.summary_file_hash,
        status=manifest.status,
    )
