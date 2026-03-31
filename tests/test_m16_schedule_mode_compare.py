from __future__ import annotations

from datetime import date

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.rolling_campaign_compare import compare_rolling_campaigns
from tests.m16_schedule_helpers import (
    build_explicit_schedule_file,
    prepare_m16_workspace,
    run_explicit_campaign,
    run_fixed_campaign,
    run_retrain_campaign,
)


def test_schedule_mode_compare_and_unmatched_dates(tmp_path) -> None:
    workspace, ids = prepare_m16_workspace(tmp_path)
    fixed = run_fixed_campaign(workspace, model_run_id=ids["fixed_model_run_id"])
    expanding = run_retrain_campaign(
        workspace,
        retrain_every_n_trade_days=1,
        training_window_mode="expanding_to_prior_day",
    )
    lookback = run_retrain_campaign(
        workspace,
        retrain_every_n_trade_days=1,
        training_window_mode="rolling_lookback",
        lookback_trade_days=2,
    )
    explicit_schedule_path = build_explicit_schedule_file(
        workspace,
        rows=[
            {"trade_date": "2026-03-24", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-25", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-26", "model_run_id": ids["fixed_model_run_id"]},
        ],
        filename="explicit_fixed.json",
    )
    explicit = run_explicit_campaign(workspace, schedule_path=explicit_schedule_path)

    compare_expanding = compare_rolling_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(expanding["campaign_run_id"]),
        right_campaign_run_id=str(lookback["campaign_run_id"]),
        compare_basis="expanding_vs_rolling_lookback",
    )
    compare_explicit = compare_rolling_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(explicit["campaign_run_id"]),
        right_campaign_run_id=str(fixed["campaign_run_id"]),
        compare_basis="explicit_schedule_vs_fixed",
    )
    shorter_fixed = run_fixed_campaign(
        workspace,
        model_run_id=ids["fixed_model_run_id"],
        date_start=date(2026, 3, 25),
        date_end=date(2026, 3, 26),
        force=True,
    )
    compare_short = compare_rolling_campaigns(
        project_root=workspace,
        left_campaign_run_id=str(fixed["campaign_run_id"]),
        right_campaign_run_id=str(shorter_fixed["campaign_run_id"]),
        compare_basis="fixed_vs_rolling",
    )
    store = CampaignArtifactStore(workspace)
    expanding_summary = store.load_campaign_compare_summary(
        campaign_compare_run_id=str(compare_expanding["campaign_compare_run_id"])
    )
    explicit_summary = store.load_campaign_compare_summary(
        campaign_compare_run_id=str(compare_explicit["campaign_compare_run_id"])
    )
    short_summary = store.load_campaign_compare_summary(
        campaign_compare_run_id=str(compare_short["campaign_compare_run_id"])
    )
    assert expanding_summary.overlapping_day_count == 3
    assert explicit_summary.overlapping_day_count == 3
    assert short_summary.overlapping_day_count == 2
    assert short_summary.summary_json["left_only_count"] == 1
    assert short_summary.summary_json["right_only_count"] == 0
