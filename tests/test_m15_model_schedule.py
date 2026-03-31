from __future__ import annotations

from datetime import date

from libs.analytics.model_schedule_artifacts import ModelScheduleArtifactStore
from tests.m15_rolling_helpers import (
    prepare_m15_workspace,
    run_fixed_campaign,
    run_retrain_campaign,
)


def test_fixed_model_latest_is_resolved_once_and_frozen(tmp_path) -> None:
    workspace, _ = prepare_m15_workspace(tmp_path)
    result = run_fixed_campaign(workspace, latest_model=True)
    store = ModelScheduleArtifactStore(workspace)
    manifest = next(
        item
        for item in store.list_schedule_manifests()
        if item.model_schedule_run_id == result["model_schedule_run_id"]
    )
    day_rows = store.load_day_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        model_schedule_run_id=manifest.model_schedule_run_id,
    )
    resolved_ids = set(day_rows["resolved_model_run_id"].tolist())
    assert len(resolved_ids) == 1
    assert manifest.latest_model_resolved_run_id in resolved_ids
    assert set(day_rows["schedule_action"].tolist()) == {"fixed_reuse"}


def test_retrain_every_day_uses_prior_trade_date_and_switches_models(tmp_path) -> None:
    workspace, _ = prepare_m15_workspace(tmp_path)
    result = run_retrain_campaign(workspace, retrain_every_n_trade_days=1)
    store = ModelScheduleArtifactStore(workspace)
    manifest = next(
        item
        for item in store.list_schedule_manifests()
        if item.model_schedule_run_id == result["model_schedule_run_id"]
    )
    day_rows = store.load_day_rows(
        date_start=manifest.date_start,
        date_end=manifest.date_end,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        model_schedule_run_id=manifest.model_schedule_run_id,
    ).sort_values("trade_date")
    assert day_rows["train_end"].tolist() == [
        date(2026, 3, 23),
        date(2026, 3, 24),
        date(2026, 3, 25),
    ]
    assert day_rows["model_switch_flag"].tolist() == [False, True, True]
    assert day_rows["model_age_trade_days"].tolist() == [1, 1, 1]
    assert len(set(day_rows["resolved_model_run_id"].tolist())) == 3
