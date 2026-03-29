from __future__ import annotations

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_schemas import CampaignStatus
from tests.m14_campaign_helpers import prepare_m14_workspace, run_campaign


def test_campaign_lineage_and_failed_rerun_behavior(tmp_path) -> None:
    workspace, ids = prepare_m14_workspace(tmp_path)
    result = run_campaign(workspace, model_run_id=ids["model_run_id"])
    store = CampaignArtifactStore(workspace)
    manifest = next(
        item
        for item in store.list_campaign_manifests()
        if item.campaign_run_id == result["campaign_run_id"]
    )
    day_rows = store.load_day_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    )
    assert set(day_rows["model_run_id"].tolist()) == {ids["model_run_id"]}
    assert all(value is not None for value in day_rows["prediction_run_id"].tolist())
    assert all(value is not None for value in day_rows["strategy_run_id"].tolist())
    assert all(value is not None for value in day_rows["execution_task_id"].tolist())
    assert all(value is not None for value in day_rows["shadow_run_id"].tolist())
    assert all(value is not None for value in day_rows["portfolio_analytics_run_id"].tolist())
    assert all(value is not None for value in day_rows["source_qlib_export_run_id"].tolist())
    assert all(value is not None for value in day_rows["source_standard_build_run_id"].tolist())

    run = store.load_campaign_run(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    )
    store.save_campaign_run(run.model_copy(update={"status": CampaignStatus.FAILED}))
    rerun = run_campaign(workspace, model_run_id=ids["model_run_id"])
    assert rerun["campaign_run_id"] == result["campaign_run_id"]
    assert rerun["reused"] is False

    rebuilt = run_campaign(workspace, model_run_id=ids["model_run_id"], force=True)
    assert rebuilt["campaign_run_id"] == result["campaign_run_id"]
    assert rebuilt["reused"] is False
