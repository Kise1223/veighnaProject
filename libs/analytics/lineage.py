"""Lineage helpers for M11 execution analytics artifacts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.artifacts import ExecutionAnalyticsArtifactStore
from libs.analytics.schemas import ExecutionAnalyticsLineage, ExecutionCompareLineage


def resolve_execution_analytics_lineage(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    analytics_run_id: str,
) -> ExecutionAnalyticsLineage:
    store = ExecutionAnalyticsArtifactStore(project_root)
    manifest = store.load_analytics_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        analytics_run_id=analytics_run_id,
    )
    return ExecutionAnalyticsLineage(
        analytics_run_id=manifest.analytics_run_id,
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


def resolve_execution_compare_lineage(
    *,
    project_root: Path,
    compare_run_id: str,
) -> ExecutionCompareLineage:
    store = ExecutionAnalyticsArtifactStore(project_root)
    manifest = store.load_compare_manifest(compare_run_id=compare_run_id)
    return ExecutionCompareLineage(
        compare_run_id=manifest.compare_run_id,
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
