"""Canonical contracts for M12 portfolio / risk analytics artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class PortfolioAnalyticsStatus(StrEnum):
    CREATED = "created"
    SUCCESS = "success"
    FAILED = "failed"


class PortfolioSourceType(StrEnum):
    PAPER_RUN = "paper_run"
    SHADOW_RUN = "shadow_run"
    MIXED_COMPARE = "mixed_compare"


class PortfolioCompareBasis(StrEnum):
    PLANNED_VS_EXECUTED = "planned_vs_executed"
    PAPER_VS_SHADOW = "paper_vs_shadow"
    BARS_VS_TICKS = "bars_vs_ticks"
    FULL_VS_PARTIAL = "full_vs_partial"
    DAY_VS_IOC = "day_vs_ioc"


class PortfolioAnalyticsConfig(BaseModel):
    tracking_error_proxy_mode: str = Field(default="half_l1_weight_drift", min_length=1)
    planned_notional_priority: str = Field(default="planning_first", min_length=1)
    compare_intersection_only: bool = True

    model_config = ConfigDict(extra="forbid")


class PortfolioAnalyticsRunRecord(BaseModel):
    portfolio_analytics_run_id: str = Field(min_length=1)
    trade_date: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    source_type: PortfolioSourceType
    source_run_ids: list[str] = Field(min_length=1)
    source_execution_task_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    analytics_config_hash: str = Field(min_length=1)
    status: PortfolioAnalyticsStatus
    created_at: datetime
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PortfolioAnalyticsManifest(BaseModel):
    portfolio_analytics_run_id: str = Field(min_length=1)
    trade_date: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    source_type: PortfolioSourceType
    source_run_ids: list[str] = Field(min_length=1)
    source_execution_task_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    analytics_config_hash: str = Field(min_length=1)
    status: PortfolioAnalyticsStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    positions_file_path: str | None = None
    positions_file_hash: str | None = None
    position_row_count: int = Field(default=0, ge=0)
    groups_file_path: str | None = None
    groups_file_hash: str | None = None
    group_row_count: int = Field(default=0, ge=0)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    error_message: str | None = None
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class PortfolioPositionRowRecord(BaseModel):
    portfolio_analytics_run_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    target_weight: Decimal = Field(ge=Decimal("0"))
    executed_weight: Decimal = Field(ge=Decimal("0"))
    weight_drift: Decimal = Field(ge=Decimal("0"))
    target_rank: int | None = Field(default=None, ge=1)
    target_score: float | None = None
    quantity_end: int = Field(ge=0)
    sellable_quantity_end: int = Field(ge=0)
    avg_price_end: Decimal = Field(ge=Decimal("0"))
    market_value_end: Decimal = Field(ge=Decimal("0"))
    executed_price_reference: Decimal | None = None
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    planned_notional: Decimal = Field(ge=Decimal("0"))
    filled_notional: Decimal = Field(ge=Decimal("0"))
    fill_rate: float = Field(ge=0)
    session_end_status: str | None = None
    replay_mode: str | None = None
    fill_model_name: str | None = None
    time_in_force: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class PortfolioGroupRowRecord(BaseModel):
    portfolio_analytics_run_id: str = Field(min_length=1)
    group_type: str = Field(min_length=1)
    group_key: str = Field(min_length=1)
    target_weight_sum: Decimal = Field(ge=Decimal("0"))
    executed_weight_sum: Decimal = Field(ge=Decimal("0"))
    weight_drift_sum: Decimal = Field(ge=Decimal("0"))
    market_value_end: Decimal = Field(ge=Decimal("0"))
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class PortfolioSummaryRecord(BaseModel):
    portfolio_analytics_run_id: str = Field(min_length=1)
    holdings_count_target: int = Field(ge=0)
    holdings_count_end: int = Field(ge=0)
    target_cash_weight: Decimal = Field(ge=Decimal("0"))
    executed_cash_weight: Decimal = Field(ge=Decimal("0"))
    gross_exposure_end: Decimal = Field(ge=Decimal("0"))
    net_exposure_end: Decimal
    realized_turnover: Decimal = Field(ge=Decimal("0"))
    filled_notional_total: Decimal = Field(ge=Decimal("0"))
    planned_notional_total: Decimal = Field(ge=Decimal("0"))
    fill_rate_gross: float = Field(ge=0)
    top1_concentration: Decimal = Field(ge=Decimal("0"))
    top3_concentration: Decimal = Field(ge=Decimal("0"))
    top5_concentration: Decimal = Field(ge=Decimal("0"))
    hhi_concentration: Decimal = Field(ge=Decimal("0"))
    total_realized_pnl: Decimal
    total_unrealized_pnl: Decimal
    total_weight_drift_l1: Decimal = Field(ge=Decimal("0"))
    tracking_error_proxy: Decimal = Field(ge=Decimal("0"))
    net_liquidation_start: Decimal = Field(ge=Decimal("0"))
    net_liquidation_end: Decimal = Field(ge=Decimal("0"))
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class PortfolioCompareRunRecord(BaseModel):
    portfolio_compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    compare_basis: PortfolioCompareBasis
    analytics_config_hash: str = Field(min_length=1)
    status: PortfolioAnalyticsStatus
    created_at: datetime
    source_execution_task_ids: list[str] = Field(min_length=1)
    source_strategy_run_ids: list[str] = Field(min_length=1)
    source_prediction_run_ids: list[str] = Field(min_length=1)
    source_qlib_export_run_ids: list[str] = Field(default_factory=list)
    source_standard_build_run_ids: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class PortfolioCompareManifest(BaseModel):
    portfolio_compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    compare_basis: PortfolioCompareBasis
    analytics_config_hash: str = Field(min_length=1)
    status: PortfolioAnalyticsStatus
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


class PortfolioCompareRowRecord(BaseModel):
    portfolio_compare_run_id: str = Field(min_length=1)
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


class PortfolioCompareSummaryRecord(BaseModel):
    portfolio_compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    compare_basis: PortfolioCompareBasis
    comparable_count: int = Field(ge=0)
    delta_cash_weight: Decimal
    delta_fill_rate: float
    delta_weight_drift_l1: Decimal
    delta_top1_concentration: Decimal
    delta_top5_concentration: Decimal
    delta_hhi_concentration: Decimal
    delta_realized_turnover: Decimal
    delta_net_liquidation_end: Decimal
    delta_total_realized_pnl: Decimal
    delta_total_unrealized_pnl: Decimal
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class PortfolioAnalyticsLineage(BaseModel):
    portfolio_analytics_run_id: str = Field(min_length=1)
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
    status: PortfolioAnalyticsStatus

    model_config = ConfigDict(extra="forbid")


class PortfolioCompareLineage(BaseModel):
    portfolio_compare_run_id: str = Field(min_length=1)
    left_run_id: str = Field(min_length=1)
    right_run_id: str = Field(min_length=1)
    source_execution_task_ids: list[str] = Field(min_length=1)
    source_strategy_run_ids: list[str] = Field(min_length=1)
    source_prediction_run_ids: list[str] = Field(min_length=1)
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    status: PortfolioAnalyticsStatus

    model_config = ConfigDict(extra="forbid")
