"""Lineage helpers for M6 planning artifacts."""

from __future__ import annotations

from datetime import date

from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.schemas import ExecutionTaskLineage, OrderIntentLineage, TargetWeightLineage


def resolve_target_weight_lineage(
    store: PlanningArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    strategy_run_id: str,
) -> TargetWeightLineage:
    manifest = store.load_target_weight_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        strategy_run_id=strategy_run_id,
    )
    return TargetWeightLineage(
        strategy_run_id=manifest.strategy_run_id,
        prediction_run_id=manifest.prediction_run_id,
        file_path=manifest.file_path,
        file_hash=manifest.file_hash,
        source_qlib_export_run_id=manifest.source_qlib_export_run_id,
        source_standard_build_run_id=manifest.source_standard_build_run_id,
    )


def resolve_execution_task_lineage(
    store: PlanningArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str,
) -> ExecutionTaskLineage:
    manifest = store.load_execution_task_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_id,
    )
    return ExecutionTaskLineage(
        execution_task_id=manifest.execution_task_id,
        strategy_run_id=manifest.strategy_run_id,
        source_target_weight_hash=manifest.source_target_weight_hash,
        file_path=manifest.file_path,
        file_hash=manifest.file_hash,
        preview_file_path=manifest.preview_file_path,
        preview_file_hash=manifest.preview_file_hash,
        source_qlib_export_run_id=manifest.source_qlib_export_run_id,
        source_standard_build_run_id=manifest.source_standard_build_run_id,
    )


def resolve_order_intent_lineage(
    store: PlanningArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str,
) -> OrderIntentLineage:
    manifest = store.load_execution_task_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_id,
    )
    return OrderIntentLineage(
        execution_task_id=manifest.execution_task_id,
        strategy_run_id=manifest.strategy_run_id,
        preview_file_path=manifest.preview_file_path,
        preview_file_hash=manifest.preview_file_hash,
        status=manifest.status,
    )
