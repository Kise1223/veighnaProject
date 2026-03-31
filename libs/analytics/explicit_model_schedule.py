"""Explicit model schedule loading for M16."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from apps.research_qlib.bootstrap import load_runtime_config
from apps.research_qlib.workflow import DEFAULT_BASE_CONFIG
from libs.research.artifacts import ResearchArtifactStore
from libs.research.schemas import ResearchRunStatus


@dataclass(frozen=True)
class ExplicitModelScheduleRow:
    trade_date: date
    resolved_model_run_id: str


def load_explicit_model_schedule(
    *,
    project_root: Path,
    trade_dates: list[date] | tuple[date, ...],
    schedule_path: Path,
) -> list[ExplicitModelScheduleRow]:
    payload = json.loads(schedule_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        raw_rows = payload.get("schedule")
        if not isinstance(raw_rows, list):
            raise ValueError("explicit schedule must contain a top-level 'schedule' list")
    elif isinstance(payload, list):
        raw_rows = payload
    else:
        raise ValueError("explicit schedule must be a JSON list or {'schedule': [...]} object")
    rows_by_date: dict[date, ExplicitModelScheduleRow] = {}
    for raw in raw_rows:
        if not isinstance(raw, dict):
            raise ValueError("explicit schedule rows must be JSON objects")
        trade_date = date.fromisoformat(str(raw["trade_date"]))
        if trade_date in rows_by_date:
            raise ValueError(f"duplicate explicit schedule entry for trade_date={trade_date.isoformat()}")
        model_run_id = str(raw["model_run_id"])
        rows_by_date[trade_date] = ExplicitModelScheduleRow(
            trade_date=trade_date,
            resolved_model_run_id=_validate_model_run_id(
                project_root=project_root,
                model_run_id=model_run_id,
            ),
        )
    missing_dates = [item.isoformat() for item in trade_dates if item not in rows_by_date]
    if missing_dates:
        raise ValueError(
            "explicit schedule is missing trade dates: " + ", ".join(missing_dates)
        )
    return [rows_by_date[item] for item in trade_dates]


def _validate_model_run_id(*, project_root: Path, model_run_id: str) -> str:
    runtime_config = load_runtime_config(project_root / DEFAULT_BASE_CONFIG)
    store = ResearchArtifactStore(project_root, project_root / runtime_config.artifacts_root)
    run = store.load_run(model_run_id)
    if run.status != ResearchRunStatus.SUCCESS:
        raise ValueError(f"model_run {model_run_id} is not successful")
    return run.run_id
