"""Canonical contracts for M15 rolling model schedule artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class ModelScheduleStatus(StrEnum):
    CREATED = "created"
    SUCCESS = "success"
    FAILED = "failed"


class ModelScheduleMode(StrEnum):
    FIXED_MODEL = "fixed_model"
    RETRAIN_EVERY_N_TRADE_DAYS = "retrain_every_n_trade_days"
    EXPLICIT_MODEL_SCHEDULE = "explicit_model_schedule"


class TrainingWindowMode(StrEnum):
    EXPANDING_TO_PRIOR_DAY = "expanding_to_prior_day"
    ROLLING_LOOKBACK = "rolling_lookback"


class ModelScheduleAction(StrEnum):
    FIXED_REUSE = "fixed_reuse"
    RETRAINED_NEW_MODEL = "retrained_new_model"
    REUSED_PRIOR_MODEL = "reused_prior_model"
    EXPLICIT_MODEL = "explicit_model"


class ModelScheduleConfig(BaseModel):
    latest_model_resolution_mode: str = Field(
        default="resolved_once_at_campaign_start",
        min_length=1,
    )
    lookahead_guard: str = Field(default="train_end_at_most_prior_trade_date", min_length=1)

    model_config = ConfigDict(extra="forbid")


class ModelScheduleRunRecord(BaseModel):
    model_schedule_run_id: str = Field(min_length=1)
    date_start: date
    date_end: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    schedule_mode: ModelScheduleMode
    fixed_model_run_id: str | None = None
    latest_model_resolved_run_id: str | None = None
    retrain_every_n_trade_days: int | None = Field(default=None, ge=1)
    training_window_mode: TrainingWindowMode
    lookback_trade_days: int | None = Field(default=None, ge=1)
    explicit_schedule_path: str | None = None
    benchmark_enabled: bool
    benchmark_source_type: str = Field(min_length=1)
    campaign_config_hash: str = Field(min_length=1)
    campaign_run_id: str | None = None
    status: ModelScheduleStatus
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class ModelScheduleManifest(BaseModel):
    model_schedule_run_id: str = Field(min_length=1)
    date_start: date
    date_end: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    schedule_mode: ModelScheduleMode
    fixed_model_run_id: str | None = None
    latest_model_resolved_run_id: str | None = None
    retrain_every_n_trade_days: int | None = Field(default=None, ge=1)
    training_window_mode: TrainingWindowMode
    lookback_trade_days: int | None = Field(default=None, ge=1)
    explicit_schedule_path: str | None = None
    benchmark_enabled: bool
    benchmark_source_type: str = Field(min_length=1)
    campaign_config_hash: str = Field(min_length=1)
    campaign_run_id: str | None = None
    status: ModelScheduleStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    day_rows_file_path: str | None = None
    day_rows_file_hash: str | None = None
    day_row_count: int = Field(default=0, ge=0)
    error_message: str | None = None

    model_config = ConfigDict(extra="forbid")


class ModelScheduleDayRowRecord(BaseModel):
    model_schedule_run_id: str = Field(min_length=1)
    campaign_run_id: str | None = None
    trade_date: date
    schedule_action: ModelScheduleAction
    resolved_model_run_id: str = Field(min_length=1)
    resolved_prediction_run_id: str | None = None
    train_start: date | None = None
    train_end: date | None = None
    model_switch_flag: bool
    model_age_trade_days: int = Field(ge=0)
    days_since_last_retrain: int = Field(ge=0)
    strict_no_lookahead_expected: bool | None = None
    strict_no_lookahead_passed: bool | None = None
    schedule_warning_code: str | None = None
    day_status: str = Field(min_length=1)
    reused_flags_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    error_summary: str | None = None
    created_at: datetime
    strategy_run_id: str | None = None
    execution_task_id: str | None = None
    paper_run_id: str | None = None
    shadow_run_id: str | None = None
    execution_analytics_run_id: str | None = None
    portfolio_analytics_run_id: str | None = None
    benchmark_analytics_run_id: str | None = None
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ModelScheduleLineage(BaseModel):
    model_schedule_run_id: str = Field(min_length=1)
    campaign_run_id: str | None = None
    day_row_count: int = Field(ge=0)
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    status: ModelScheduleStatus

    model_config = ConfigDict(extra="forbid")
