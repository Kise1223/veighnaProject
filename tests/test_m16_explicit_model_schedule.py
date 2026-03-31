from __future__ import annotations

import pytest

from tests.m16_schedule_helpers import (
    build_explicit_schedule_file,
    load_schedule_day_rows,
    prepare_m16_workspace,
    run_explicit_campaign,
    run_retrain_campaign,
)


def test_explicit_model_schedule_runs_with_exact_models(tmp_path) -> None:
    workspace, ids = prepare_m16_workspace(tmp_path)
    rolling = run_retrain_campaign(workspace, retrain_every_n_trade_days=1)
    rolling_rows = load_schedule_day_rows(workspace, str(rolling["model_schedule_run_id"]))
    schedule_path = build_explicit_schedule_file(
        workspace,
        rows=[
            {
                "trade_date": row["trade_date"].isoformat(),
                "model_run_id": str(row["resolved_model_run_id"]),
            }
            for row in rolling_rows.to_dict(orient="records")
        ],
    )
    result = run_explicit_campaign(workspace, schedule_path=schedule_path)
    day_rows = load_schedule_day_rows(workspace, str(result["model_schedule_run_id"]))
    assert day_rows["schedule_action"].tolist() == ["explicit_model", "explicit_model", "explicit_model"]
    assert day_rows["resolved_model_run_id"].tolist() == rolling_rows["resolved_model_run_id"].tolist()


def test_explicit_schedule_rejects_missing_duplicate_and_unknown_dates(tmp_path) -> None:
    workspace, ids = prepare_m16_workspace(tmp_path)
    missing_path = build_explicit_schedule_file(
        workspace,
        rows=[
            {"trade_date": "2026-03-24", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-25", "model_run_id": ids["fixed_model_run_id"]},
        ],
        filename="missing.json",
    )
    with pytest.raises(ValueError, match="missing trade dates"):
        run_explicit_campaign(workspace, schedule_path=missing_path)

    duplicate_path = build_explicit_schedule_file(
        workspace,
        rows=[
            {"trade_date": "2026-03-24", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-24", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-25", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-26", "model_run_id": ids["fixed_model_run_id"]},
        ],
        filename="duplicate.json",
    )
    with pytest.raises(ValueError, match="duplicate explicit schedule entry"):
        run_explicit_campaign(workspace, schedule_path=duplicate_path)

    unknown_path = build_explicit_schedule_file(
        workspace,
        rows=[
            {"trade_date": "2026-03-24", "model_run_id": ids["fixed_model_run_id"]},
            {"trade_date": "2026-03-25", "model_run_id": "model_missing"},
            {"trade_date": "2026-03-26", "model_run_id": ids["fixed_model_run_id"]},
        ],
        filename="unknown.json",
    )
    with pytest.raises(FileNotFoundError):
        run_explicit_campaign(workspace, schedule_path=unknown_path)
