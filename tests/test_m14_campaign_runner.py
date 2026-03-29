from __future__ import annotations

from datetime import date

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from tests.m14_campaign_helpers import prepare_m14_workspace, run_campaign


def test_shadow_campaign_runs_in_trade_date_order_and_reuses(tmp_path) -> None:
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
    assert day_rows["trade_date"].tolist() == [
        date(2026, 3, 24),
        date(2026, 3, 25),
        date(2026, 3, 26),
    ]
    assert set(day_rows["day_status"].tolist()) == {"success"}

    rerun = run_campaign(workspace, model_run_id=ids["model_run_id"])
    assert rerun["campaign_run_id"] == result["campaign_run_id"]
    assert rerun["reused"] is True
