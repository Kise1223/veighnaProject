from __future__ import annotations

from libs.analytics.schedule_audit_artifacts import ScheduleAuditArtifactStore
from tests.m16_schedule_helpers import (
    build_explicit_schedule_file,
    prepare_m16_workspace,
    run_explicit_campaign,
    run_fixed_campaign,
    run_retrain_campaign,
)


def test_fixed_latest_warns_and_retrain_modes_pass_strict_audit(tmp_path) -> None:
    workspace, ids = prepare_m16_workspace(tmp_path)
    fixed = run_fixed_campaign(workspace, latest_model=True)
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
    store = ScheduleAuditArtifactStore(workspace)
    fixed_summary = store.load_audit_summary(schedule_audit_run_id=str(fixed["schedule_audit_run_id"]))
    expanding_summary = store.load_audit_summary(schedule_audit_run_id=str(expanding["schedule_audit_run_id"]))
    lookback_summary = store.load_audit_summary(schedule_audit_run_id=str(lookback["schedule_audit_run_id"]))
    fixed_rows = store.load_audit_day_rows(schedule_audit_run_id=str(fixed["schedule_audit_run_id"]))

    assert fixed_summary.warning_day_count > 0
    assert "fixed_latest_frozen_campaign_start_non_strict" in fixed_rows["schedule_warning_code"].dropna().tolist()
    assert expanding_summary.strict_fail_day_count == 0
    assert lookback_summary.strict_fail_day_count == 0


def test_explicit_schedule_missing_metadata_warns_instead_of_passing(tmp_path, monkeypatch) -> None:
    workspace, ids = prepare_m16_workspace(tmp_path)
    schedule_path = build_explicit_schedule_file(
        workspace,
        rows=[
            {"trade_date": "2026-03-24", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-25", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-26", "model_run_id": ids["fixed_model_run_id"]},
        ],
        filename="explicit_missing_metadata.json",
    )

    monkeypatch.setattr(
        "libs.analytics.model_schedule.load_model_run_metadata",
        lambda **_: None,
    )

    result = run_explicit_campaign(workspace, schedule_path=schedule_path)
    store = ScheduleAuditArtifactStore(workspace)
    summary = store.load_audit_summary(schedule_audit_run_id=str(result["schedule_audit_run_id"]))
    rows = store.load_audit_day_rows(schedule_audit_run_id=str(result["schedule_audit_run_id"]))
    assert summary.strict_fail_day_count == 3
    assert "explicit_schedule_no_train_metadata" in rows["schedule_warning_code"].dropna().tolist()
