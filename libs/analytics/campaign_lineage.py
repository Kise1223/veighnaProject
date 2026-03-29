"""Lineage helpers for M14 walk-forward campaign artifacts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_schemas import CampaignCompareLineage, CampaignLineage


def resolve_campaign_lineage(
    *,
    project_root: Path,
    date_start: date,
    date_end: date,
    account_id: str,
    basket_id: str,
    campaign_run_id: str,
) -> CampaignLineage:
    store = CampaignArtifactStore(project_root)
    manifest = store.load_campaign_manifest(
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        campaign_run_id=campaign_run_id,
    )
    return CampaignLineage(
        campaign_run_id=manifest.campaign_run_id,
        model_run_id=manifest.model_run_id,
        day_row_count=manifest.day_row_count,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        summary_file_path=manifest.summary_file_path,
        summary_file_hash=manifest.summary_file_hash,
        status=manifest.status,
    )


def resolve_campaign_compare_lineage(
    *,
    project_root: Path,
    campaign_compare_run_id: str,
) -> CampaignCompareLineage:
    store = CampaignArtifactStore(project_root)
    manifest = store.load_campaign_compare_manifest(campaign_compare_run_id=campaign_compare_run_id)
    return CampaignCompareLineage(
        campaign_compare_run_id=manifest.campaign_compare_run_id,
        left_campaign_run_id=manifest.left_campaign_run_id,
        right_campaign_run_id=manifest.right_campaign_run_id,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        summary_file_path=manifest.summary_file_path,
        summary_file_hash=manifest.summary_file_hash,
        status=manifest.status,
    )
