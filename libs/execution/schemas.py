"""Canonical contracts for M7 paper execution and local ledger artifacts."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class PaperRunStatus(StrEnum):
    CREATED = "created"
    SUCCESS = "success"
    FAILED = "failed"


class PaperOrderStatus(StrEnum):
    CREATED = "created"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    UNFILLED = "unfilled"
    REJECTED = "rejected"


class PaperFillModelConfig(BaseModel):
    fill_model_name: str = Field(default="bar_limit_v1", min_length=1)
    order_type: str = Field(default="LIMIT", min_length=1)
    limit_price_source: str = Field(default="reference_price", min_length=1)
    fill_price_rule: str = Field(default="limit_price", min_length=1)
    execution_order: str = Field(default="sell_then_buy", min_length=1)
    missing_bar_behavior: str = Field(default="unfilled", min_length=1)
    insufficient_cash_behavior: str = Field(default="reject", min_length=1)
    require_previous_close: bool = True
    allow_partial_fill: bool = False
    broker: str = Field(default="DEFAULT", min_length=1)

    model_config = ConfigDict(extra="forbid")


class PaperExecutionRunRecord(BaseModel):
    paper_run_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    fill_model_name: str = Field(min_length=1)
    fill_model_config_hash: str = Field(min_length=1)
    market_data_hash: str = Field(min_length=1)
    account_state_hash: str = Field(min_length=1)
    status: PaperRunStatus
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PaperExecutionManifest(BaseModel):
    paper_run_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    status: PaperRunStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    orders_file_path: str | None = None
    orders_file_hash: str | None = None
    orders_count: int = Field(default=0, ge=0)
    trades_file_path: str | None = None
    trades_file_hash: str | None = None
    trades_count: int = Field(default=0, ge=0)
    account_file_path: str | None = None
    account_file_hash: str | None = None
    positions_file_path: str | None = None
    positions_file_hash: str | None = None
    positions_count: int = Field(default=0, ge=0)
    report_file_path: str | None = None
    report_file_hash: str | None = None
    error_message: str | None = None
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PaperOrderRecord(BaseModel):
    order_id: str = Field(min_length=1)
    paper_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    side: str = Field(min_length=1)
    order_type: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    limit_price: Decimal = Field(gt=Decimal("0"))
    reference_price: Decimal = Field(gt=Decimal("0"))
    previous_close: Decimal | None = None
    source_order_intent_hash: str = Field(min_length=1)
    status: PaperOrderStatus
    created_at: datetime
    status_reason: str | None = None
    estimated_cost: Decimal = Field(default=Decimal("0"), ge=Decimal("0"))
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PaperTradeRecord(BaseModel):
    paper_run_id: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    trade_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    side: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=Decimal("0"))
    notional: Decimal = Field(ge=Decimal("0"))
    cost_breakdown_json: dict[str, JsonScalar]
    fill_bar_dt: datetime
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PaperAccountSnapshotRecord(BaseModel):
    paper_run_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    trade_date: date
    cash_start: Decimal
    cash_end: Decimal
    fees_total: Decimal
    realized_pnl: Decimal
    market_value_end: Decimal
    net_liquidation_end: Decimal
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PaperPositionSnapshotRecord(BaseModel):
    paper_run_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    trade_date: date
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    quantity: int = Field(ge=0)
    sellable_quantity: int = Field(ge=0)
    avg_price: Decimal = Field(ge=Decimal("0"))
    market_value: Decimal = Field(ge=Decimal("0"))
    unrealized_pnl: Decimal
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PaperReconcileReportRecord(BaseModel):
    paper_run_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    planned_order_count: int = Field(ge=0)
    filled_order_count: int = Field(ge=0)
    partially_filled_order_count: int = Field(default=0, ge=0)
    rejected_order_count: int = Field(ge=0)
    unfilled_order_count: int = Field(ge=0)
    planned_notional: Decimal = Field(ge=Decimal("0"))
    filled_notional: Decimal = Field(ge=Decimal("0"))
    estimated_cost_total: Decimal = Field(ge=Decimal("0"))
    realized_cost_total: Decimal = Field(ge=Decimal("0"))
    summary_json: dict[str, JsonScalar | list[JsonScalar]]
    created_at: datetime
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PaperRunLineage(BaseModel):
    paper_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    report_file_path: str | None = None
    report_file_hash: str | None = None
    status: PaperRunStatus

    model_config = ConfigDict(extra="forbid")
