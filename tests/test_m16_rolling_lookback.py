from __future__ import annotations

from datetime import date

from libs.analytics.schedule_audit_artifacts import ScheduleAuditArtifactStore
from tests.m16_schedule_helpers import (
    load_schedule_day_rows,
    prepare_m16_workspace,
    run_retrain_campaign,
)


def test_rolling_lookback_window_and_no_lookahead(tmp_path) -> None:
    workspace, _ = prepare_m16_workspace(tmp_path)
    result = run_retrain_campaign(
        workspace,
        retrain_every_n_trade_days=1,
        training_window_mode="rolling_lookback",
        lookback_trade_days=2,
    )
    day_rows = load_schedule_day_rows(workspace, str(result["model_schedule_run_id"]))
    assert day_rows["train_start"].tolist() == [
        date(2026, 3, 20),
        date(2026, 3, 23),
        date(2026, 3, 24),
    ]
    assert day_rows["train_end"].tolist() == [
        date(2026, 3, 23),
        date(2026, 3, 24),
        date(2026, 3, 25),
    ]
    assert day_rows["model_switch_flag"].tolist() == [False, True, True]
    assert day_rows["model_age_trade_days"].tolist() == [1, 1, 1]
    assert day_rows["strict_no_lookahead_expected"].tolist() == [True, True, True]
    assert day_rows["strict_no_lookahead_passed"].tolist() == [True, True, True]

    audit_store = ScheduleAuditArtifactStore(workspace)
    manifest = next(
        item
        for item in audit_store.list_audit_manifests()
        if item.schedule_audit_run_id == result["schedule_audit_run_id"]
    )
    summary = audit_store.load_audit_summary(schedule_audit_run_id=manifest.schedule_audit_run_id)
    assert summary.strict_fail_day_count == 0
