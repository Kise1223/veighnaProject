from __future__ import annotations

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.rolling_campaign_compare import compare_rolling_campaigns
from tests.m15_rolling_helpers import (
    prepare_m15_workspace,
    run_fixed_campaign,
    run_retrain_campaign,
)


def test_fixed_vs_rolling_and_retrain_compare(tmp_path) -> None:
    workspace, ids = prepare_m15_workspace(tmp_path)
    fixed = run_fixed_campaign(workspace, model_run_id=ids["fixed_model_run_id"])
    rolling_1d = run_retrain_campaign(workspace, retrain_every_n_trade_days=1)
    rolling_2d = run_retrain_campaign(workspace, retrain_every_n_trade_days=2)
    compare_fixed = compare_rolling_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(fixed["campaign_run_id"]),
        right_campaign_run_id=str(rolling_1d["campaign_run_id"]),
        compare_basis="fixed_vs_rolling",
    )
    compare_retrain = compare_rolling_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(rolling_1d["campaign_run_id"]),
        right_campaign_run_id=str(rolling_2d["campaign_run_id"]),
        compare_basis="retrain_1d_vs_retrain_2d",
    )
    store = CampaignArtifactStore(workspace)
    fixed_summary = store.load_campaign_compare_summary(
        campaign_compare_run_id=str(compare_fixed["campaign_compare_run_id"])
    )
    retrain_summary = store.load_campaign_compare_summary(
        campaign_compare_run_id=str(compare_retrain["campaign_compare_run_id"])
    )
    assert fixed_summary.overlapping_day_count == 3
    assert retrain_summary.overlapping_day_count == 3
    assert retrain_summary.delta_unique_model_count != 0
    assert retrain_summary.summary_json["left_only_count"] == 0
    assert retrain_summary.summary_json["right_only_count"] == 0
