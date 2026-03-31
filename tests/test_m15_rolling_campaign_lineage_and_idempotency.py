from __future__ import annotations

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_schemas import CampaignStatus
from libs.analytics.model_schedule_artifacts import ModelScheduleArtifactStore
from libs.analytics.model_schedule_schemas import ModelScheduleStatus
from tests.m15_rolling_helpers import prepare_m15_workspace, run_retrain_campaign


def test_rolling_campaign_lineage_and_failed_rerun_behavior(tmp_path) -> None:
    workspace, _ = prepare_m15_workspace(tmp_path)
    result = run_retrain_campaign(workspace, retrain_every_n_trade_days=1)
    campaign_store = CampaignArtifactStore(workspace)
    schedule_store = ModelScheduleArtifactStore(workspace)
    campaign_manifest = next(
        item for item in campaign_store.list_campaign_manifests() if item.campaign_run_id == result["campaign_run_id"]
    )
    schedule_manifest = next(
        item
        for item in schedule_store.list_schedule_manifests()
        if item.model_schedule_run_id == result["model_schedule_run_id"]
    )
    day_rows = campaign_store.load_day_rows(
        date_start=campaign_manifest.date_start,
        date_end=campaign_manifest.date_end,
        account_id=campaign_manifest.account_id,
        basket_id=campaign_manifest.basket_id,
        campaign_run_id=campaign_manifest.campaign_run_id,
    )
    schedule_day_rows = schedule_store.load_day_rows(
        date_start=schedule_manifest.date_start,
        date_end=schedule_manifest.date_end,
        account_id=schedule_manifest.account_id,
        basket_id=schedule_manifest.basket_id,
        model_schedule_run_id=schedule_manifest.model_schedule_run_id,
    )
    assert all(value is not None for value in day_rows["prediction_run_id"].tolist())
    assert all(value is not None for value in day_rows["portfolio_analytics_run_id"].tolist())
    assert all(value is not None for value in schedule_day_rows["resolved_model_run_id"].tolist())
    assert set(schedule_day_rows["campaign_run_id"].tolist()) == {result["campaign_run_id"]}

    campaign_run = campaign_store.load_campaign_run(
        date_start=campaign_manifest.date_start,
        date_end=campaign_manifest.date_end,
        account_id=campaign_manifest.account_id,
        basket_id=campaign_manifest.basket_id,
        campaign_run_id=campaign_manifest.campaign_run_id,
    )
    schedule_run = schedule_store.load_schedule_run(
        date_start=schedule_manifest.date_start,
        date_end=schedule_manifest.date_end,
        account_id=schedule_manifest.account_id,
        basket_id=schedule_manifest.basket_id,
        model_schedule_run_id=schedule_manifest.model_schedule_run_id,
    )
    campaign_store.save_campaign_run(campaign_run.model_copy(update={"status": CampaignStatus.FAILED}))
    schedule_store.save_schedule_run(schedule_run.model_copy(update={"status": ModelScheduleStatus.FAILED}))
    rerun = run_retrain_campaign(workspace, retrain_every_n_trade_days=1)
    assert rerun["campaign_run_id"] == result["campaign_run_id"]
    assert rerun["model_schedule_run_id"] == result["model_schedule_run_id"]
    assert rerun["reused"] is False

    rebuilt = run_retrain_campaign(workspace, retrain_every_n_trade_days=1, force=True)
    assert rebuilt["campaign_run_id"] == result["campaign_run_id"]
    assert rebuilt["model_schedule_run_id"] == result["model_schedule_run_id"]
    assert rebuilt["reused"] is False
