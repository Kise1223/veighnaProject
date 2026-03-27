"""Canonical contracts for M8 replay-driven shadow sessions."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class ShadowRunStatus(StrEnum):
    CREATED = "created"
    SUCCESS = "success"
    FAILED = "failed"


class ShadowOrderState(StrEnum):
    CREATED = "created"
    WORKING = "working"
    FILLED = "filled"
    EXPIRED_END_OF_SESSION = "expired_end_of_session"
    REJECTED_VALIDATION = "rejected_validation"


class ShadowEventType(StrEnum):
    CREATED = "created"
    WORKING = "working"
    FILLED = "filled"
    EXPIRED_END_OF_SESSION = "expired_end_of_session"
    REJECTED_VALIDATION = "rejected_validation"


class ShadowSessionConfig(BaseModel):
    market_replay_mode: str = Field(default="bars_1m", min_length=1)
    fill_model_name: str = Field(default="bar_limit_shadow_v1", min_length=1)
    order_type: str = Field(default="LIMIT", min_length=1)
    limit_price_source: str = Field(default="reference_price", min_length=1)
    fill_price_rule: str = Field(default="limit_price", min_length=1)
    execution_order: str = Field(default="sell_then_buy", min_length=1)
    missing_bar_behavior: str = Field(default="expire", min_length=1)
    end_of_session_behavior: str = Field(default="expire", min_length=1)
    require_previous_close: bool = True
    allow_partial_fill: bool = False
    broker: str = Field(default="DEFAULT", min_length=1)

    model_config = ConfigDict(extra="forbid")


class ShadowSessionRunRecord(BaseModel):
    shadow_run_id: str = Field(min_length=1)
    paper_run_id: str | None = None
    strategy_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    market_replay_mode: str = Field(min_length=1)
    fill_model_name: str = Field(min_length=1)
    fill_model_config_hash: str = Field(min_length=1)
    market_data_hash: str = Field(min_length=1)
    account_state_hash: str = Field(min_length=1)
    status: ShadowRunStatus
    started_at: datetime | None = None
    ended_at: datetime | None = None
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ShadowSessionManifest(BaseModel):
    shadow_run_id: str = Field(min_length=1)
    paper_run_id: str | None = None
    strategy_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    status: ShadowRunStatus
    created_at: datetime
    started_at: datetime | None = None
    ended_at: datetime | None = None
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    order_events_file_path: str | None = None
    order_events_file_hash: str | None = None
    order_events_count: int = Field(default=0, ge=0)
    fill_events_file_path: str | None = None
    fill_events_file_hash: str | None = None
    fill_events_count: int = Field(default=0, ge=0)
    report_file_path: str | None = None
    report_file_hash: str | None = None
    paper_report_file_path: str | None = None
    paper_report_file_hash: str | None = None
    error_message: str | None = None
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ShadowOrderStateEventRecord(BaseModel):
    shadow_run_id: str = Field(min_length=1)
    paper_run_id: str | None = None
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    event_dt: datetime
    event_type: ShadowEventType
    state_before: ShadowOrderState | None = None
    state_after: ShadowOrderState
    quantity: int = Field(gt=0)
    remaining_quantity: int = Field(ge=0)
    reference_price: Decimal = Field(gt=Decimal("0"))
    limit_price: Decimal = Field(gt=Decimal("0"))
    reason: str | None = None
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ShadowFillEventRecord(BaseModel):
    shadow_run_id: str = Field(min_length=1)
    paper_run_id: str | None = None
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    trade_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    side: str = Field(min_length=1)
    fill_dt: datetime
    price: Decimal = Field(gt=Decimal("0"))
    quantity: int = Field(gt=0)
    notional: Decimal = Field(ge=Decimal("0"))
    cost_breakdown_json: dict[str, JsonScalar]
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ShadowSessionReportRecord(BaseModel):
    shadow_run_id: str = Field(min_length=1)
    paper_run_id: str | None = None
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    order_count: int = Field(ge=0)
    filled_order_count: int = Field(ge=0)
    expired_order_count: int = Field(ge=0)
    rejected_order_count: int = Field(ge=0)
    unfilled_order_count: int = Field(ge=0)
    filled_notional: Decimal = Field(ge=Decimal("0"))
    realized_cost_total: Decimal = Field(ge=Decimal("0"))
    session_summary_json: dict[str, JsonScalar | list[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ShadowSessionLineage(BaseModel):
    shadow_run_id: str = Field(min_length=1)
    paper_run_id: str | None = None
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    report_file_path: str | None = None
    report_file_hash: str | None = None
    paper_report_file_path: str | None = None
    paper_report_file_hash: str | None = None
    status: ShadowRunStatus

    model_config = ConfigDict(extra="forbid")
