"""Canonical contracts for M13 benchmark-relative attribution analytics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from libs.analytics.benchmark_schemas import BenchmarkRunStatus, JsonScalar


class BenchmarkCompareBasis(StrEnum):
    BARS_VS_TICKS = "bars_vs_ticks"
    FULL_VS_PARTIAL = "full_vs_partial"
    DAY_VS_IOC = "day_vs_ioc"
    PAPER_VS_SHADOW = "paper_vs_shadow"
    TARGET_VS_EXECUTED_RELATIVE = "target_vs_executed_relative"


class BenchmarkAnalyticsConfig(BaseModel):
    return_proxy_mode: str = Field(default="mark_to_previous_close", min_length=1)
    compare_intersection_only: bool = True
    group_interaction_mode: str = Field(default="allocation_plus_selection_only", min_length=1)

    model_config = ConfigDict(extra="forbid")


class BenchmarkAnalyticsRunRecord(BaseModel):
    benchmark_analytics_run_id: str = Field(min_length=1)
    trade_date: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    source_portfolio_analytics_run_id: str = Field(min_length=1)
    source_run_type: str = Field(min_length=1)
    source_run_id: str = Field(min_length=1)
    source_execution_task_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    benchmark_run_id: str = Field(min_length=1)
    analytics_config_hash: str = Field(min_length=1)
    status: BenchmarkRunStatus
    created_at: datetime
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class BenchmarkAnalyticsManifest(BaseModel):
    benchmark_analytics_run_id: str = Field(min_length=1)
    trade_date: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    source_portfolio_analytics_run_id: str = Field(min_length=1)
    source_run_type: str = Field(min_length=1)
    source_run_id: str = Field(min_length=1)
    source_execution_task_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    benchmark_run_id: str = Field(min_length=1)
    analytics_config_hash: str = Field(min_length=1)
    status: BenchmarkRunStatus
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


class BenchmarkPositionRowRecord(BaseModel):
    benchmark_analytics_run_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    target_weight: Decimal
    executed_weight: Decimal
    benchmark_weight: Decimal
    active_weight_target: Decimal
    active_weight_executed: Decimal
    target_rank: int | None = Field(default=None, ge=1)
    target_score: float | None = None
    market_value_end: Decimal = Field(ge=Decimal("0"))
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    portfolio_contribution_proxy: Decimal
    benchmark_contribution_proxy: Decimal
    active_contribution_proxy: Decimal
    instrument_return_proxy: Decimal
    replay_mode: str | None = None
    fill_model_name: str | None = None
    time_in_force: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class BenchmarkGroupRowRecord(BaseModel):
    benchmark_analytics_run_id: str = Field(min_length=1)
    group_type: str = Field(min_length=1)
    group_key: str = Field(min_length=1)
    target_weight_sum: Decimal
    executed_weight_sum: Decimal
    benchmark_weight_sum: Decimal
    active_weight_sum: Decimal
    portfolio_return_proxy: Decimal
    benchmark_return_proxy: Decimal
    allocation_proxy: Decimal
    selection_proxy: Decimal
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class BenchmarkSummaryRowRecord(BaseModel):
    benchmark_analytics_run_id: str = Field(min_length=1)
    holdings_count_benchmark: int = Field(ge=0)
    holdings_overlap_count: int = Field(ge=0)
    target_active_share: Decimal = Field(ge=Decimal("0"))
    executed_active_share: Decimal = Field(ge=Decimal("0"))
    active_cash_weight: Decimal
    benchmark_cash_weight: Decimal = Field(ge=Decimal("0"))
    delta_top1_concentration: Decimal
    delta_top5_concentration: Decimal
    delta_hhi_concentration: Decimal
    total_portfolio_contribution_proxy: Decimal
    total_benchmark_contribution_proxy: Decimal
    total_active_contribution_proxy: Decimal
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class BenchmarkCompareRunRecord(BaseModel):
    benchmark_compare_run_id: str = Field(min_length=1)
    left_benchmark_analytics_run_id: str = Field(min_length=1)
    right_benchmark_analytics_run_id: str = Field(min_length=1)
    compare_basis: BenchmarkCompareBasis
    analytics_config_hash: str = Field(min_length=1)
    status: BenchmarkRunStatus
    created_at: datetime
    source_execution_task_ids: list[str] = Field(min_length=1)
    source_strategy_run_ids: list[str] = Field(min_length=1)
    source_prediction_run_ids: list[str] = Field(min_length=1)
    source_portfolio_analytics_run_ids: list[str] = Field(min_length=1)
    benchmark_run_ids: list[str] = Field(min_length=1)
    source_qlib_export_run_ids: list[str] = Field(default_factory=list)
    source_standard_build_run_ids: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class BenchmarkCompareManifest(BaseModel):
    benchmark_compare_run_id: str = Field(min_length=1)
    left_benchmark_analytics_run_id: str = Field(min_length=1)
    right_benchmark_analytics_run_id: str = Field(min_length=1)
    compare_basis: BenchmarkCompareBasis
    analytics_config_hash: str = Field(min_length=1)
    status: BenchmarkRunStatus
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
    source_portfolio_analytics_run_ids: list[str] = Field(min_length=1)
    benchmark_run_ids: list[str] = Field(min_length=1)
    source_qlib_export_run_ids: list[str] = Field(default_factory=list)
    source_standard_build_run_ids: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class BenchmarkCompareRowRecord(BaseModel):
    benchmark_compare_run_id: str = Field(min_length=1)
    left_benchmark_analytics_run_id: str = Field(min_length=1)
    right_benchmark_analytics_run_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    metric_name: str = Field(min_length=1)
    left_value: JsonScalar
    right_value: JsonScalar
    delta_value: JsonScalar
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class BenchmarkCompareSummaryRecord(BaseModel):
    benchmark_compare_run_id: str = Field(min_length=1)
    left_benchmark_analytics_run_id: str = Field(min_length=1)
    right_benchmark_analytics_run_id: str = Field(min_length=1)
    compare_basis: BenchmarkCompareBasis
    comparable_count: int = Field(ge=0)
    delta_executed_active_share: Decimal
    delta_active_cash_weight: Decimal
    delta_total_active_contribution_proxy: Decimal
    delta_delta_hhi_concentration: Decimal
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class BenchmarkAnalyticsLineage(BaseModel):
    benchmark_analytics_run_id: str = Field(min_length=1)
    source_portfolio_analytics_run_id: str = Field(min_length=1)
    source_run_id: str = Field(min_length=1)
    source_execution_task_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    benchmark_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    status: BenchmarkRunStatus

    model_config = ConfigDict(extra="forbid")


class BenchmarkCompareLineage(BaseModel):
    benchmark_compare_run_id: str = Field(min_length=1)
    left_benchmark_analytics_run_id: str = Field(min_length=1)
    right_benchmark_analytics_run_id: str = Field(min_length=1)
    source_execution_task_ids: list[str] = Field(min_length=1)
    source_strategy_run_ids: list[str] = Field(min_length=1)
    source_prediction_run_ids: list[str] = Field(min_length=1)
    source_portfolio_analytics_run_ids: list[str] = Field(min_length=1)
    benchmark_run_ids: list[str] = Field(min_length=1)
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    status: BenchmarkRunStatus

    model_config = ConfigDict(extra="forbid")
