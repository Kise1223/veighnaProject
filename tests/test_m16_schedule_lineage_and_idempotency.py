from __future__ import annotations

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_schemas import CampaignStatus
from libs.analytics.model_schedule_artifacts import ModelScheduleArtifactStore
from libs.analytics.model_schedule_schemas import ModelScheduleStatus
from libs.analytics.schedule_audit_artifacts import ScheduleAuditArtifactStore
from tests.m16_schedule_helpers import prepare_m16_workspace, run_retrain_campaign


def test_m16_lineage_and_idempotency(tmp_path) -> None:
    workspace, _ = prepare_m16_workspace(tmp_path)
    result = run_retrain_campaign(
        workspace,
        retrain_every_n_trade_days=1,
        training_window_mode="rolling_lookback",
        lookback_trade_days=2,
    )
    campaign_store = CampaignArtifactStore(workspace)
    schedule_store = ModelScheduleArtifactStore(workspace)
    audit_store = ScheduleAuditArtifactStore(workspace)
    campaign_manifest = next(
        item for item in campaign_store.list_campaign_manifests() if item.campaign_run_id == result["campaign_run_id"]
    )
    schedule_manifest = next(
        item
        for item in schedule_store.list_schedule_manifests()
        if item.model_schedule_run_id == result["model_schedule_run_id"]
    )
    audit_manifest = next(
        item
        for item in audit_store.list_audit_manifests()
        if item.schedule_audit_run_id == result["schedule_audit_run_id"]
    )
    campaign_day_rows = campaign_store.load_day_rows(
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
    assert all(value is not None for value in campaign_day_rows["model_schedule_run_id"].tolist())
    assert all(value is not None for value in campaign_day_rows["prediction_run_id"].tolist())
    assert all(value is not None for value in schedule_day_rows["resolved_model_run_id"].tolist())
    assert audit_manifest.model_schedule_run_id == result["model_schedule_run_id"]

    reused = run_retrain_campaign(
        workspace,
        retrain_every_n_trade_days=1,
        training_window_mode="rolling_lookback",
        lookback_trade_days=2,
    )
    assert reused["campaign_run_id"] == result["campaign_run_id"]
    assert reused["model_schedule_run_id"] == result["model_schedule_run_id"]
    assert reused["schedule_audit_run_id"] == result["schedule_audit_run_id"]
    assert reused["reused"] is True

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
    rerun = run_retrain_campaign(
        workspace,
        retrain_every_n_trade_days=1,
        training_window_mode="rolling_lookback",
        lookback_trade_days=2,
    )
    assert rerun["campaign_run_id"] == result["campaign_run_id"]
    assert rerun["reused"] is False
