"""Lineage helpers for M7 paper execution artifacts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.schemas import PaperRunLineage


def resolve_paper_run_lineage(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    paper_run_id: str,
) -> PaperRunLineage:
    store = ExecutionArtifactStore(project_root)
    manifest = store.load_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        paper_run_id=paper_run_id,
    )
    return PaperRunLineage(
        paper_run_id=manifest.paper_run_id,
        execution_task_id=manifest.execution_task_id,
        strategy_run_id=manifest.strategy_run_id,
        source_prediction_run_id=manifest.source_prediction_run_id,
        source_qlib_export_run_id=manifest.source_qlib_export_run_id,
        source_standard_build_run_id=manifest.source_standard_build_run_id,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        report_file_path=manifest.report_file_path,
        report_file_hash=manifest.report_file_hash,
        status=manifest.status,
    )
