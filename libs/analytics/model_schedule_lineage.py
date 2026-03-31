"""Lineage helpers for M15 model schedule artifacts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.model_schedule_artifacts import ModelScheduleArtifactStore
from libs.analytics.model_schedule_schemas import ModelScheduleLineage


def resolve_model_schedule_lineage(
    *,
    project_root: Path,
    date_start: date,
    date_end: date,
    account_id: str,
    basket_id: str,
    model_schedule_run_id: str,
) -> ModelScheduleLineage:
    store = ModelScheduleArtifactStore(project_root)
    manifest = store.load_schedule_manifest(
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        model_schedule_run_id=model_schedule_run_id,
    )
    return ModelScheduleLineage(
        model_schedule_run_id=manifest.model_schedule_run_id,
        campaign_run_id=manifest.campaign_run_id,
        day_row_count=manifest.day_row_count,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        status=manifest.status,
    )
