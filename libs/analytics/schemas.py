"""Canonical contracts for M11 execution analytics and TCA artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class ExecutionAnalyticsStatus(StrEnum):
    CREATED = "created"
    SUCCESS = "success"
    FAILED = "failed"


class ExecutionSourceType(StrEnum):
    PAPER_RUN = "paper_run"
    SHADOW_RUN = "shadow_run"
    MIXED_COMPARE = "mixed_compare"


class CompareBasis(StrEnum):
    PAPER_VS_SHADOW = "paper_vs_shadow"
    BARS_VS_TICKS = "bars_vs_ticks"
    FULL_VS_PARTIAL = "full_vs_partial"
    DAY_VS_IOC = "day_vs_ioc"


class SessionEndStatus(StrEnum):
    FILLED = "filled"
    PARTIALLY_FILLED_THEN_EXPIRED = "partially_filled_then_expired"
    EXPIRED_END_OF_SESSION = "expired_end_of_session"
    EXPIRED_IOC_REMAINING = "expired_ioc_remaining"
    REJECTED_VALIDATION = "rejected_validation"
    UNFILLED = "unfilled"


class ExecutionAnalyticsConfig(BaseModel):
    implementation_shortfall_mode: str = Field(
        default="executed_notional_plus_cost", min_length=1
    )
    no_fill_shortfall_behavior: str = Field(default="zero", min_length=1)
    compare_intersection_only: bool = True

    model_config = ConfigDict(extra="forbid")


class ExecutionAnalyticsRunRecord(BaseModel):
    analytics_run_id: str = Field(min_length=1)
    trade_date: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    source_type: ExecutionSourceType
    source_run_ids: list[str] = Field(min_length=1)
    source_execution_task_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    analytics_config_hash: str = Field(min_length=1)
    status: ExecutionAnalyticsStatus
    created_at: datetime
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ExecutionAnalyticsManifest(BaseModel):
    analytics_run_id: str = Field(min_length=1)
    trade_date: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    source_type: ExecutionSourceType
    source_run_ids: list[str] = Field(min_length=1)
    source_execution_task_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    analytics_config_hash: str = Field(min_length=1)
    status: ExecutionAnalyticsStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    rows_file_path: str | None = None
    rows_file_hash: str | None = None
    row_count: int = Field(default=0, ge=0)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    error_message: str | None = None
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ExecutionTcaRowRecord(BaseModel):
    analytics_run_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    side: str = Field(min_length=1)
    requested_quantity: int = Field(ge=0)
    filled_quantity: int = Field(ge=0)
    remaining_quantity: int = Field(ge=0)
    fill_rate: float = Field(ge=0)
    partial_fill_count: int = Field(ge=0)
    avg_fill_price: Decimal | None = None
    reference_price: Decimal = Field(gt=Decimal("0"))
    previous_close: Decimal | None = None
    planned_notional: Decimal = Field(ge=Decimal("0"))
    filled_notional: Decimal = Field(ge=Decimal("0"))
    estimated_cost_total: Decimal = Field(ge=Decimal("0"))
    realized_cost_total: Decimal = Field(ge=Decimal("0"))
    implementation_shortfall: Decimal
    first_fill_dt: datetime | None = None
    last_fill_dt: datetime | None = None
    session_end_status: SessionEndStatus
    replay_mode: str | None = None
    fill_model_name: str | None = None
    time_in_force: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class ExecutionTcaSummaryRecord(BaseModel):
    analytics_run_id: str = Field(min_length=1)
    order_count: int = Field(ge=0)
    filled_order_count: int = Field(ge=0)
    partially_filled_order_count: int = Field(ge=0)
    expired_order_count: int = Field(ge=0)
    rejected_order_count: int = Field(ge=0)
    total_requested_notional: Decimal = Field(ge=Decimal("0"))
    total_filled_notional: Decimal = Field(ge=Decimal("0"))
    gross_fill_rate: float = Field(ge=0)
    avg_fill_rate: float = Field(ge=0)
    total_estimated_cost: Decimal = Field(ge=Decimal("0"))
    total_realized_cost: Decimal = Field(ge=Decimal("0"))
    total_implementation_shortfall: Decimal
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class ExecutionCompareRunRecord(BaseModel):
    compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    compare_basis: CompareBasis
    analytics_config_hash: str = Field(min_length=1)
    status: ExecutionAnalyticsStatus
    created_at: datetime
    source_execution_task_ids: list[str] = Field(min_length=1)
    source_strategy_run_ids: list[str] = Field(min_length=1)
    source_prediction_run_ids: list[str] = Field(min_length=1)
    source_qlib_export_run_ids: list[str] = Field(default_factory=list)
    source_standard_build_run_ids: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class ExecutionCompareManifest(BaseModel):
    compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    compare_basis: CompareBasis
    analytics_config_hash: str = Field(min_length=1)
    status: ExecutionAnalyticsStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    rows_file_path: str | None = None
    rows_file_hash: str | None = None
    row_count: int = Field(default=0, ge=0)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    error_message: str | None = None
    source_execution_task_ids: list[str] = Field(min_length=1)
    source_strategy_run_ids: list[str] = Field(min_length=1)
    source_prediction_run_ids: list[str] = Field(min_length=1)
    source_qlib_export_run_ids: list[str] = Field(default_factory=list)
    source_standard_build_run_ids: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class ExecutionCompareRowRecord(BaseModel):
    compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    metric_name: str = Field(min_length=1)
    left_value: JsonScalar
    right_value: JsonScalar
    delta_value: JsonScalar
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class ExecutionCompareSummaryRecord(BaseModel):
    compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    compare_basis: CompareBasis
    order_count: int = Field(ge=0)
    comparable_order_count: int = Field(ge=0)
    delta_filled_notional: Decimal
    delta_fill_rate: float
    delta_realized_cost: Decimal
    delta_implementation_shortfall: Decimal
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class ExecutionAnalyticsLineage(BaseModel):
    analytics_run_id: str = Field(min_length=1)
    source_run_ids: list[str] = Field(min_length=1)
    source_execution_task_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    status: ExecutionAnalyticsStatus

    model_config = ConfigDict(extra="forbid")


class ExecutionCompareLineage(BaseModel):
    compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    source_execution_task_ids: list[str] = Field(min_length=1)
    source_strategy_run_ids: list[str] = Field(min_length=1)
    source_prediction_run_ids: list[str] = Field(min_length=1)
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    status: ExecutionAnalyticsStatus

    model_config = ConfigDict(extra="forbid")
