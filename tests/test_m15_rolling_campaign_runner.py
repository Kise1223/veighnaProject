from __future__ import annotations

from datetime import date

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_runner import run_walkforward_campaign
from tests.m15_rolling_helpers import (
    prepare_m15_workspace,
    run_fixed_campaign,
    run_retrain_campaign,
)


def test_fixed_model_campaign_matches_m14_semantics_and_reuses(tmp_path) -> None:
    workspace, ids = prepare_m15_workspace(tmp_path)
    m14 = run_walkforward_campaign(
        project_root=workspace,
        date_start=date(2026, 3, 24),
        date_end=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        model_run_id=ids["fixed_model_run_id"],
        execution_source_type="shadow",
        market_replay_mode="bars_1m",
        benchmark_source_type="equal_weight_target_universe",
    )
    m15 = run_fixed_campaign(workspace, model_run_id=ids["fixed_model_run_id"])
    store = CampaignArtifactStore(workspace)
    m14_manifest = next(
        item for item in store.list_campaign_manifests() if item.campaign_run_id == m14["campaign_run_id"]
    )
    m15_manifest = next(
        item for item in store.list_campaign_manifests() if item.campaign_run_id == m15["campaign_run_id"]
    )
    m14_summary = store.load_campaign_summary(
        date_start=m14_manifest.date_start,
        date_end=m14_manifest.date_end,
        account_id=m14_manifest.account_id,
        basket_id=m14_manifest.basket_id,
        campaign_run_id=m14_manifest.campaign_run_id,
    )
    m15_summary = store.load_campaign_summary(
        date_start=m15_manifest.date_start,
        date_end=m15_manifest.date_end,
        account_id=m15_manifest.account_id,
        basket_id=m15_manifest.basket_id,
        campaign_run_id=m15_manifest.campaign_run_id,
    )
    assert m14_summary.net_liquidation_end == m15_summary.net_liquidation_end
    assert m14_summary.average_fill_rate == m15_summary.average_fill_rate
    rerun = run_retrain_campaign(workspace, retrain_every_n_trade_days=1)
    reused = run_retrain_campaign(workspace, retrain_every_n_trade_days=1)
    assert reused["campaign_run_id"] == rerun["campaign_run_id"]
    assert reused["reused"] is True
