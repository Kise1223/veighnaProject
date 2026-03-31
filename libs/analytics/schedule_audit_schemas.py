"""Canonical contracts for M16 schedule realism audit artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class ScheduleAuditStatus(StrEnum):
    CREATED = "created"
    SUCCESS = "success"
    FAILED = "failed"


class ScheduleAuditConfig(BaseModel):
    audit_mode: str = Field(default="strict_no_lookahead_v1", min_length=1)

    model_config = ConfigDict(extra="forbid")


class ScheduleAuditRunRecord(BaseModel):
    schedule_audit_run_id: str = Field(min_length=1)
    model_schedule_run_id: str = Field(min_length=1)
    date_start: date
    date_end: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    schedule_mode: str = Field(min_length=1)
    training_window_mode: str | None = None
    explicit_schedule_path: str | None = None
    audit_config_hash: str = Field(min_length=1)
    status: ScheduleAuditStatus
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class ScheduleAuditManifest(BaseModel):
    schedule_audit_run_id: str = Field(min_length=1)
    model_schedule_run_id: str = Field(min_length=1)
    date_start: date
    date_end: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    schedule_mode: str = Field(min_length=1)
    training_window_mode: str | None = None
    explicit_schedule_path: str | None = None
    audit_config_hash: str = Field(min_length=1)
    status: ScheduleAuditStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    day_rows_file_path: str | None = None
    day_rows_file_hash: str | None = None
    day_row_count: int = Field(default=0, ge=0)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    error_message: str | None = None

    model_config = ConfigDict(extra="forbid")


class ScheduleAuditDayRowRecord(BaseModel):
    schedule_audit_run_id: str = Field(min_length=1)
    trade_date: date
    resolved_model_run_id: str = Field(min_length=1)
    train_start: date | None = None
    train_end: date | None = None
    previous_trade_date: date | None = None
    strict_no_lookahead_expected: bool
    strict_no_lookahead_passed: bool
    model_switch_flag: bool
    model_age_trade_days: int = Field(ge=0)
    days_since_last_retrain: int = Field(ge=0)
    schedule_warning_code: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class ScheduleAuditSummaryRecord(BaseModel):
    schedule_audit_run_id: str = Field(min_length=1)
    day_count: int = Field(ge=0)
    strict_checked_day_count: int = Field(ge=0)
    strict_pass_day_count: int = Field(ge=0)
    strict_fail_day_count: int = Field(ge=0)
    warning_day_count: int = Field(ge=0)
    unique_model_count: int = Field(ge=0)
    retrain_count: int = Field(ge=0)
    average_model_age_trade_days: Decimal | None = None
    max_model_age_trade_days: int | None = Field(default=None, ge=0)
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")
