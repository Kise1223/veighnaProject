"""M15 rolling campaign compare wrappers."""

from __future__ import annotations

from pathlib import Path

from libs.analytics.campaign_compare import compare_campaigns


def compare_rolling_campaigns(
    *,
    project_root: Path,
    left_campaign_run_id: str,
    right_campaign_run_id: str,
    compare_basis: str,
    force: bool = False,
) -> dict[str, object]:
    return compare_campaigns(
        project_root=project_root,
        left_campaign_run_id=left_campaign_run_id,
        right_campaign_run_id=right_campaign_run_id,
        compare_basis=compare_basis,
        force=force,
    )
