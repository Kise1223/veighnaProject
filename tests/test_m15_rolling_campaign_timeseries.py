from __future__ import annotations

from decimal import Decimal

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from tests.m15_rolling_helpers import prepare_m15_workspace, run_retrain_campaign


def test_rolling_campaign_timeseries_and_summary_metrics(tmp_path) -> None:
    workspace, _ = prepare_m15_workspace(tmp_path)
    result = run_retrain_campaign(workspace, retrain_every_n_trade_days=2)
    store = CampaignArtifactStore(workspace)
    manifest = next(
        item for item in store.list_campaign_manifests() if item.campaign_run_id == result["campaign_run_id"]
    )
    timeseries = store.load_timeseries_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    ).sort_values("trade_date")
    summary = store.load_campaign_summary(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    )
    assert timeseries["trade_date"].tolist() == sorted(timeseries["trade_date"].tolist())
    assert summary.unique_model_count == 2
    assert summary.retrain_count == 2
    assert summary.average_model_age_trade_days == Decimal("1.333333")
    assert summary.max_model_age_trade_days == 2
    assert all(value is not None for value in timeseries["daily_active_share"].tolist())


def test_rolling_campaign_active_metrics_are_null_without_benchmark(tmp_path) -> None:
    workspace, _ = prepare_m15_workspace(tmp_path)
    result = run_retrain_campaign(
        workspace,
        retrain_every_n_trade_days=1,
        benchmark_source_type="none",
    )
    store = CampaignArtifactStore(workspace)
    manifest = next(
        item for item in store.list_campaign_manifests() if item.campaign_run_id == result["campaign_run_id"]
    )
    timeseries = store.load_timeseries_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        campaign_run_id=manifest.campaign_run_id,
    )
    assert all(value is None for value in timeseries["daily_active_share"].tolist())
    assert all(value is None for value in timeseries["daily_active_contribution_proxy"].tolist())
