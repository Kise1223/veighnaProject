"""M16 schedule realism audit helpers."""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

from libs.analytics.model_schedule import ResolvedModelSchedule, ResolvedModelScheduleDay
from libs.analytics.model_schedule_schemas import ModelScheduleMode
from libs.analytics.portfolio_normalize import quantize_weight
from libs.analytics.schedule_audit_artifacts import ScheduleAuditArtifactStore
from libs.analytics.schedule_audit_schemas import (
    JsonScalar,
    ScheduleAuditConfig,
    ScheduleAuditDayRowRecord,
    ScheduleAuditRunRecord,
    ScheduleAuditStatus,
    ScheduleAuditSummaryRecord,
)
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash
from libs.rules_engine.calendar import ExchangeCalendar, is_trade_day, load_calendars
from libs.schemas.master_data import ExchangeCode


def run_schedule_audit(
    *,
    project_root: Path,
    schedule: ResolvedModelSchedule,
    date_start: date,
    date_end: date,
    account_id: str,
    basket_id: str,
    explicit_schedule_path: str | None,
    force: bool = False,
) -> dict[str, object]:
    config_hash = stable_hash(ScheduleAuditConfig().model_dump(mode="json"))
    schedule_audit_run_id = build_schedule_audit_run_id(
        model_schedule_run_id=schedule.model_schedule_run_id,
        audit_config_hash=config_hash,
    )
    store = ScheduleAuditArtifactStore(project_root)
    if store.has_audit_run(schedule_audit_run_id=schedule_audit_run_id):
        existing = store.load_audit_manifest(schedule_audit_run_id=schedule_audit_run_id)
        if existing.status == ScheduleAuditStatus.SUCCESS and not force:
            summary = store.load_audit_summary(schedule_audit_run_id=schedule_audit_run_id)
            return {
                "schedule_audit_run_id": schedule_audit_run_id,
                "day_count": summary.day_count,
                "summary_path": existing.summary_file_path,
                "strict_fail_day_count": summary.strict_fail_day_count,
                "warning_day_count": summary.warning_day_count,
                "reused": True,
            }
        store.clear_audit_run(schedule_audit_run_id=schedule_audit_run_id)
    created_at = ensure_cn_aware(datetime.now())
    run = ScheduleAuditRunRecord(
        schedule_audit_run_id=schedule_audit_run_id,
        model_schedule_run_id=schedule.model_schedule_run_id,
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        schedule_mode=schedule.schedule_mode.value,
        training_window_mode=schedule.training_window_mode.value if schedule.schedule_mode != ModelScheduleMode.EXPLICIT_MODEL_SCHEDULE else None,
        explicit_schedule_path=explicit_schedule_path,
        audit_config_hash=config_hash,
        status=ScheduleAuditStatus.CREATED,
        created_at=created_at,
    )
    store.save_audit_run(run)
    try:
        day_rows, summary = build_schedule_audit(
            project_root=project_root,
            schedule=schedule,
            schedule_audit_run_id=schedule_audit_run_id,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": ScheduleAuditStatus.SUCCESS})
        manifest = store.save_audit_success(
            run=success_run,
            day_rows=day_rows,
            summary=summary,
        )
    except Exception as exc:
        failed_run = run.model_copy(update={"status": ScheduleAuditStatus.FAILED})
        store.save_failed_audit_run(failed_run, error_message=str(exc))
        raise
    return {
        "schedule_audit_run_id": schedule_audit_run_id,
        "day_count": len(day_rows),
        "summary_path": manifest.summary_file_path,
        "strict_fail_day_count": summary.strict_fail_day_count,
        "warning_day_count": summary.warning_day_count,
        "reused": False,
    }


def build_schedule_audit_run_id(*, model_schedule_run_id: str, audit_config_hash: str) -> str:
    return "saudit_" + stable_hash(
        {
            "model_schedule_run_id": model_schedule_run_id,
            "audit_config_hash": audit_config_hash,
        }
    )[:12]


def build_schedule_audit(
    *,
    project_root: Path,
    schedule: ResolvedModelSchedule,
    schedule_audit_run_id: str,
    created_at: datetime,
) -> tuple[list[ScheduleAuditDayRowRecord], ScheduleAuditSummaryRecord]:
    calendars = load_calendars(project_root / "data" / "master" / "bootstrap" / "trading_calendar.json")
    warning_counter: Counter[str] = Counter()
    day_rows: list[ScheduleAuditDayRowRecord] = []
    for day in schedule.days:
        previous_trade_date = _find_previous_trade_date(
            trade_date=day.trade_date,
            calendars=calendars,
        )
        strict_expected, strict_passed, warning_code = evaluate_schedule_day_audit(
            schedule_mode=schedule.schedule_mode,
            latest_model_resolved_run_id=schedule.latest_model_resolved_run_id,
            day=day,
            previous_trade_date=previous_trade_date,
        )
        if warning_code is not None:
            warning_counter[warning_code] += 1
        day_rows.append(
            ScheduleAuditDayRowRecord(
                schedule_audit_run_id=schedule_audit_run_id,
                trade_date=day.trade_date,
                resolved_model_run_id=day.resolved_model_run_id,
                train_start=day.train_start,
                train_end=day.train_end,
                previous_trade_date=previous_trade_date,
                strict_no_lookahead_expected=strict_expected,
                strict_no_lookahead_passed=strict_passed,
                model_switch_flag=day.model_switch_flag,
                model_age_trade_days=day.model_age_trade_days,
                days_since_last_retrain=day.days_since_last_retrain,
                schedule_warning_code=warning_code,
                created_at=created_at,
            )
        )
    model_ages = [Decimal(item.model_age_trade_days) for item in day_rows]
    strict_checked_day_count = sum(1 for item in day_rows if item.strict_no_lookahead_expected)
    strict_pass_day_count = sum(
        1 for item in day_rows if item.strict_no_lookahead_expected and item.strict_no_lookahead_passed
    )
    strict_fail_day_count = strict_checked_day_count - strict_pass_day_count
    summary_json = cast(
        dict[str, JsonScalar | Sequence[JsonScalar]],
        {
            "warning_codes": sorted(warning_counter),
            "warning_counts_json": ",".join(f"{key}:{value}" for key, value in sorted(warning_counter.items())),
            "schedule_mode": schedule.schedule_mode.value,
            "training_window_mode": (
                schedule.training_window_mode.value
                if schedule.schedule_mode != ModelScheduleMode.EXPLICIT_MODEL_SCHEDULE
                else None
            ),
        },
    )
    summary = ScheduleAuditSummaryRecord(
        schedule_audit_run_id=schedule_audit_run_id,
        day_count=len(day_rows),
        strict_checked_day_count=strict_checked_day_count,
        strict_pass_day_count=strict_pass_day_count,
        strict_fail_day_count=strict_fail_day_count,
        warning_day_count=sum(1 for item in day_rows if item.schedule_warning_code is not None),
        unique_model_count=len({item.resolved_model_run_id for item in day_rows}),
        retrain_count=sum(1 for item in schedule.days if item.schedule_action.value == "retrained_new_model"),
        average_model_age_trade_days=(
            quantize_weight(sum(model_ages, Decimal("0")) / Decimal(len(model_ages)))
            if model_ages
            else None
        ),
        max_model_age_trade_days=max((item.model_age_trade_days for item in day_rows), default=None),
        summary_json=summary_json,
        created_at=created_at,
    )
    return day_rows, summary


def evaluate_schedule_day_audit(
    *,
    schedule_mode: ModelScheduleMode,
    latest_model_resolved_run_id: str | None,
    day: ResolvedModelScheduleDay,
    previous_trade_date: date | None,
) -> tuple[bool, bool, str | None]:
    if day.train_start is None or day.train_end is None:
        if schedule_mode == ModelScheduleMode.EXPLICIT_MODEL_SCHEDULE:
            return True, False, "explicit_schedule_no_train_metadata"
        return False, False, "missing_train_window_metadata"
    passed = previous_trade_date is not None and day.train_end <= previous_trade_date
    if schedule_mode == ModelScheduleMode.RETRAIN_EVERY_N_TRADE_DAYS:
        return True, passed, (None if passed else "train_end_after_previous_trade_date")
    if schedule_mode == ModelScheduleMode.EXPLICIT_MODEL_SCHEDULE:
        return True, passed, (None if passed else "train_end_after_previous_trade_date")
    if latest_model_resolved_run_id is not None and day.resolved_model_run_id == latest_model_resolved_run_id and not passed:
        return False, False, "fixed_latest_frozen_campaign_start_non_strict"
    return False, passed, (None if passed else "train_end_after_previous_trade_date")


def _find_previous_trade_date(
    *,
    trade_date: date,
    calendars: dict[ExchangeCode, ExchangeCalendar],
) -> date | None:
    probe = trade_date - timedelta(days=1)
    lower_bound = date(2000, 1, 1)
    while probe >= lower_bound:
        if is_trade_day(probe, ExchangeCode.SSE, calendars):
            return probe
        probe -= timedelta(days=1)
    return None
