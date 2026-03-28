"""Lineage helpers for M8 shadow-session artifacts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.execution.shadow_schemas import ShadowSessionLineage


def resolve_shadow_run_lineage(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    shadow_run_id: str,
) -> ShadowSessionLineage:
    store = ShadowArtifactStore(project_root)
    run = store.load_run(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        shadow_run_id=shadow_run_id,
    )
    manifest = store.load_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        shadow_run_id=shadow_run_id,
    )
    return ShadowSessionLineage(
        shadow_run_id=manifest.shadow_run_id,
        paper_run_id=manifest.paper_run_id,
        execution_task_id=manifest.execution_task_id,
        strategy_run_id=manifest.strategy_run_id,
        market_replay_mode=run.market_replay_mode,
        tick_fill_model=run.tick_fill_model,
        time_in_force=run.time_in_force,
        tick_source_hash=run.tick_source_hash,
        source_prediction_run_id=manifest.source_prediction_run_id,
        source_qlib_export_run_id=manifest.source_qlib_export_run_id,
        source_standard_build_run_id=manifest.source_standard_build_run_id,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        report_file_path=manifest.report_file_path,
        report_file_hash=manifest.report_file_hash,
        paper_report_file_path=manifest.paper_report_file_path,
        paper_report_file_hash=manifest.paper_report_file_hash,
        status=manifest.status,
    )
