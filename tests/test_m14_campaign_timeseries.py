from __future__ import annotations

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_metrics import compute_max_drawdown
from tests.m14_campaign_helpers import prepare_m14_workspace, run_campaign


def test_campaign_timeseries_and_summary_metrics_are_deterministic(tmp_path) -> None:
    workspace, ids = prepare_m14_workspace(tmp_path)
    result = run_campaign(workspace, model_run_id=ids["model_run_id"])
    store = CampaignArtifactStore(workspace)
    manifest = next(
        item
        for item in store.list_campaign_manifests()
        if item.campaign_run_id == result["campaign_run_id"]
    )
    timeseries = store.load_timeseries_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    )
    summary = store.load_campaign_summary(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    )
    assert timeseries["trade_date"].tolist() == sorted(timeseries["trade_date"].tolist())
    assert summary.average_fill_rate == round(timeseries["daily_fill_rate"].mean(), 4)
    assert summary.max_drawdown == compute_max_drawdown(
        list(timeseries["net_liquidation_end"].tolist())
    )
    assert summary.final_active_share is not None
    assert all(value is not None for value in timeseries["daily_active_share"].tolist())


def test_campaign_active_metrics_are_null_without_benchmark(tmp_path) -> None:
    workspace, ids = prepare_m14_workspace(tmp_path)
    result = run_campaign(
        workspace,
        model_run_id=ids["model_run_id"],
        benchmark_source_type="none",
    )
    store = CampaignArtifactStore(workspace)
    manifest = next(
        item
        for item in store.list_campaign_manifests()
        if item.campaign_run_id == result["campaign_run_id"]
    )
    timeseries = store.load_timeseries_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    )
    summary = store.load_campaign_summary(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    )
    assert all(value is None for value in timeseries["daily_active_share"].tolist())
    assert all(value is None for value in timeseries["daily_active_contribution_proxy"].tolist())
    assert summary.average_active_share is None
    assert summary.final_active_share is None
    assert summary.cumulative_active_contribution_proxy is None
