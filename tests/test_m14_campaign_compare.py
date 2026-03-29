from __future__ import annotations

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_compare import compare_campaigns
from tests.m14_campaign_helpers import prepare_m14_workspace, run_campaign


def test_campaign_compare_supports_shadow_and_paper_bases(tmp_path) -> None:
    workspace, ids = prepare_m14_workspace(tmp_path)
    bars = run_campaign(workspace, model_run_id=ids["model_run_id"], market_replay_mode="bars_1m")
    ticks_crossing = run_campaign(
        workspace,
        model_run_id=ids["model_run_id"],
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
    )
    ticks_partial_day = run_campaign(
        workspace,
        model_run_id=ids["model_run_id"],
        market_replay_mode="ticks_l1",
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="DAY",
    )
    ticks_partial_ioc = run_campaign(
        workspace,
        model_run_id=ids["model_run_id"],
        market_replay_mode="ticks_l1",
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="IOC",
    )
    paper = run_campaign(
        workspace,
        model_run_id=ids["model_run_id"],
        execution_source_type="paper",
        market_replay_mode=None,
    )
    bars_vs_ticks = compare_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(bars["campaign_run_id"]),
        right_campaign_run_id=str(ticks_crossing["campaign_run_id"]),
        compare_basis="bars_vs_ticks",
    )
    full_vs_partial = compare_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(ticks_crossing["campaign_run_id"]),
        right_campaign_run_id=str(ticks_partial_day["campaign_run_id"]),
        compare_basis="full_vs_partial",
    )
    day_vs_ioc = compare_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(ticks_partial_day["campaign_run_id"]),
        right_campaign_run_id=str(ticks_partial_ioc["campaign_run_id"]),
        compare_basis="day_vs_ioc",
    )
    paper_vs_shadow = compare_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(paper["campaign_run_id"]),
        right_campaign_run_id=str(bars["campaign_run_id"]),
        compare_basis="paper_vs_shadow",
    )
    store = CampaignArtifactStore(workspace)
    for result in (bars_vs_ticks, full_vs_partial, day_vs_ioc, paper_vs_shadow):
        summary = store.load_campaign_compare_summary(
            campaign_compare_run_id=str(result["campaign_compare_run_id"])
        )
        assert summary.overlapping_day_count == 3
        assert summary.summary_json["left_only_count"] == 0
        assert summary.summary_json["right_only_count"] == 0


