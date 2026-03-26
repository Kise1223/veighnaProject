"""Canonical contracts for M6 research-to-trade dry-run bridge."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class ApprovedTargetWeightStatus(StrEnum):
    APPROVED = "approved"


class ExecutionTaskStatus(StrEnum):
    CREATED = "created"
    PLANNED = "planned"
    INGESTED_DRY_RUN = "ingested_dry_run"


class ValidationStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    SKIPPED = "skipped"


class TargetWeightConfigModel(BaseModel):
    strategy_name: str = Field(default="baseline_long_only", min_length=1)
    weighting: str = Field(default="equal", min_length=1)
    max_names: int = Field(default=2, ge=1)
    max_weight_per_name: Decimal = Field(default=Decimal("0.45"), ge=Decimal("0"))
    cash_buffer: Decimal = Field(default=Decimal("0.10"), ge=Decimal("0"), le=Decimal("1"))

    model_config = ConfigDict(extra="forbid")


class RebalancePlannerConfigModel(BaseModel):
    exec_style: str = Field(default="close_reference", min_length=1)
    reference_price_field: str = Field(default="previous_close", min_length=1)
    planning_time: time = Field(default=time(9, 30))
    broker: str = Field(default="DEFAULT", min_length=1)
    plan_only: bool = True
    cash_policy: str = Field(default="sequential_sell_then_buy", min_length=1)

    model_config = ConfigDict(extra="forbid")


class ApprovedTargetWeightRecord(BaseModel):
    strategy_run_id: str = Field(min_length=1)
    prediction_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    instrument_key: str = Field(min_length=1)
    qlib_symbol: str = Field(min_length=1)
    score: float
    rank: int = Field(ge=1)
    target_weight: Decimal = Field(ge=Decimal("0"), le=Decimal("1"))
    status: ApprovedTargetWeightStatus
    approved_by: str = Field(min_length=1)
    approved_at: datetime
    model_version: str = Field(min_length=1)
    feature_set_version: str = Field(min_length=1)
    config_hash: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class ApprovedTargetWeightManifest(BaseModel):
    strategy_run_id: str = Field(min_length=1)
    prediction_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    row_count: int = Field(ge=0)
    status: ApprovedTargetWeightStatus
    approved_by: str = Field(min_length=1)
    approved_at: datetime
    model_version: str = Field(min_length=1)
    feature_set_version: str = Field(min_length=1)
    config_hash: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None
    created_at: datetime
    file_path: str = Field(min_length=1)
    file_hash: str = Field(min_length=1)
    prediction_path: str = Field(min_length=1)
    prediction_file_hash: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class ExecutionTaskRecord(BaseModel):
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    exec_style: str = Field(min_length=1)
    status: ExecutionTaskStatus
    created_at: datetime
    source_target_weight_hash: str = Field(min_length=1)
    planner_config_hash: str = Field(min_length=1)
    plan_only: bool
    summary_json: dict[str, JsonScalar | list[JsonScalar]] = Field(default_factory=dict)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ExecutionTaskManifest(BaseModel):
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    status: ExecutionTaskStatus
    created_at: datetime
    source_target_weight_hash: str = Field(min_length=1)
    planner_config_hash: str = Field(min_length=1)
    plan_only: bool
    file_path: str = Field(min_length=1)
    file_hash: str = Field(min_length=1)
    preview_file_path: str = Field(min_length=1)
    preview_file_hash: str = Field(min_length=1)
    preview_row_count: int = Field(ge=0)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class OrderIntentPreviewRecord(BaseModel):
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    side: str = Field(min_length=1)
    current_quantity: int = Field(ge=0)
    sellable_quantity: int = Field(ge=0)
    target_quantity: int = Field(ge=0)
    delta_quantity: int
    reference_price: Decimal | None = None
    previous_close: Decimal | None = None
    estimated_notional: Decimal = Field(ge=Decimal("0"))
    estimated_cost: Decimal = Field(ge=Decimal("0"))
    validation_status: ValidationStatus
    validation_reason: str | None = None
    session_tag: str = Field(min_length=1)
    created_at: datetime
    source_target_weight_hash: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None
    estimated_cost_breakdown: dict[str, JsonScalar] | None = None

    model_config = ConfigDict(extra="forbid")


class OrderRequestPreview(BaseModel):
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    side: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=Decimal("0"))
    reference: str = Field(min_length=1)
    validation_status: ValidationStatus
    validation_reason: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class TargetWeightLineage(BaseModel):
    strategy_run_id: str = Field(min_length=1)
    prediction_run_id: str = Field(min_length=1)
    file_path: str = Field(min_length=1)
    file_hash: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class ExecutionTaskLineage(BaseModel):
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    source_target_weight_hash: str = Field(min_length=1)
    file_path: str = Field(min_length=1)
    file_hash: str = Field(min_length=1)
    preview_file_path: str = Field(min_length=1)
    preview_file_hash: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class OrderIntentLineage(BaseModel):
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    preview_file_path: str = Field(min_length=1)
    preview_file_hash: str = Field(min_length=1)
    status: ExecutionTaskStatus

    model_config = ConfigDict(extra="forbid")
