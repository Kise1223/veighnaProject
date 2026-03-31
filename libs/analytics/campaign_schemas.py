"""Canonical contracts for M14 walk-forward campaign artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class CampaignStatus(StrEnum):
    CREATED = "created"
    SUCCESS = "success"
    FAILED = "failed"


class CampaignDayStatus(StrEnum):
    SUCCESS = "success"
    REUSED = "reused"
    FAILED = "failed"
    SKIPPED = "skipped"


class CampaignExecutionSourceType(StrEnum):
    SHADOW = "shadow"
    PAPER = "paper"


class CampaignCompareBasis(StrEnum):
    BARS_VS_TICKS = "bars_vs_ticks"
    FULL_VS_PARTIAL = "full_vs_partial"
    DAY_VS_IOC = "day_vs_ioc"
    PAPER_VS_SHADOW = "paper_vs_shadow"
    FIXED_VS_ROLLING = "fixed_vs_rolling"
    RETRAIN_1D_VS_RETRAIN_2D = "retrain_1d_vs_retrain_2d"
    EXPANDING_VS_ROLLING_LOOKBACK = "expanding_vs_rolling_lookback"
    EXPLICIT_SCHEDULE_VS_FIXED = "explicit_schedule_vs_fixed"
    EXPLICIT_SCHEDULE_VS_RETRAIN_1D = "explicit_schedule_vs_retrain_1d"


class CampaignConfig(BaseModel):
    continue_on_error: bool = False
    max_drawdown_mode: str = Field(default="peak_to_trough_net_liquidation_end", min_length=1)
    active_metric_mode: str = Field(default="nullable_without_benchmark", min_length=1)

    model_config = ConfigDict(extra="forbid")


class CampaignCompareConfig(BaseModel):
    compare_overlap_only: bool = True

    model_config = ConfigDict(extra="forbid")


class CampaignRunRecord(BaseModel):
    campaign_run_id: str = Field(min_length=1)
    date_start: date
    date_end: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    execution_source_type: CampaignExecutionSourceType
    market_replay_mode: str | None = None
    tick_fill_model: str | None = None
    time_in_force: str | None = None
    benchmark_enabled: bool
    benchmark_source_type: str = Field(min_length=1)
    model_run_id: str | None = None
    model_schedule_run_id: str | None = None
    schedule_mode: str | None = None
    fixed_model_run_id: str | None = None
    latest_model_resolved_run_id: str | None = None
    retrain_every_n_trade_days: int | None = Field(default=None, ge=1)
    training_window_mode: str | None = None
    lookback_trade_days: int | None = Field(default=None, ge=1)
    campaign_config_hash: str = Field(min_length=1)
    status: CampaignStatus
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class CampaignManifest(BaseModel):
    campaign_run_id: str = Field(min_length=1)
    date_start: date
    date_end: date
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    execution_source_type: CampaignExecutionSourceType
    market_replay_mode: str | None = None
    tick_fill_model: str | None = None
    time_in_force: str | None = None
    benchmark_enabled: bool
    benchmark_source_type: str = Field(min_length=1)
    model_run_id: str | None = None
    model_schedule_run_id: str | None = None
    schedule_mode: str | None = None
    fixed_model_run_id: str | None = None
    latest_model_resolved_run_id: str | None = None
    retrain_every_n_trade_days: int | None = Field(default=None, ge=1)
    training_window_mode: str | None = None
    lookback_trade_days: int | None = Field(default=None, ge=1)
    campaign_config_hash: str = Field(min_length=1)
    status: CampaignStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    day_rows_file_path: str | None = None
    day_rows_file_hash: str | None = None
    day_row_count: int = Field(default=0, ge=0)
    timeseries_file_path: str | None = None
    timeseries_file_hash: str | None = None
    timeseries_row_count: int = Field(default=0, ge=0)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    error_message: str | None = None

    model_config = ConfigDict(extra="forbid")


class CampaignDayRowRecord(BaseModel):
    campaign_run_id: str = Field(min_length=1)
    trade_date: date
    day_status: CampaignDayStatus
    model_run_id: str = Field(min_length=1)
    model_schedule_run_id: str | None = None
    schedule_action: str | None = None
    prediction_run_id: str | None = None
    strategy_run_id: str | None = None
    execution_task_id: str | None = None
    paper_run_id: str | None = None
    shadow_run_id: str | None = None
    execution_analytics_run_id: str | None = None
    portfolio_analytics_run_id: str | None = None
    benchmark_run_id: str | None = None
    benchmark_analytics_run_id: str | None = None
    train_start: date | None = None
    train_end: date | None = None
    model_switch_flag: bool | None = None
    model_age_trade_days: int | None = Field(default=None, ge=0)
    days_since_last_retrain: int | None = Field(default=None, ge=0)
    strict_no_lookahead_expected: bool | None = None
    strict_no_lookahead_passed: bool | None = None
    schedule_warning_code: str | None = None
    reused_flags_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    error_summary: str | None = None
    created_at: datetime
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class CampaignTimeseriesRowRecord(BaseModel):
    campaign_run_id: str = Field(min_length=1)
    trade_date: date
    net_liquidation_end: Decimal = Field(ge=Decimal("0"))
    cash_weight_end: Decimal = Field(ge=Decimal("0"))
    daily_realized_pnl: Decimal
    daily_unrealized_pnl: Decimal
    daily_filled_notional: Decimal = Field(ge=Decimal("0"))
    daily_realized_cost: Decimal = Field(ge=Decimal("0"))
    daily_turnover: Decimal = Field(ge=Decimal("0"))
    daily_fill_rate: float = Field(ge=0)
    daily_weight_drift_l1: Decimal = Field(ge=Decimal("0"))
    daily_top5_concentration: Decimal = Field(ge=Decimal("0"))
    daily_hhi_concentration: Decimal = Field(ge=Decimal("0"))
    daily_active_share: Decimal | None = None
    daily_active_contribution_proxy: Decimal | None = None
    daily_model_switch_flag: bool | None = None
    daily_model_age_trade_days: int | None = Field(default=None, ge=0)
    daily_days_since_last_retrain: int | None = Field(default=None, ge=0)
    replay_mode: str | None = None
    fill_model_name: str | None = None
    time_in_force: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class CampaignSummaryRecord(BaseModel):
    campaign_run_id: str = Field(min_length=1)
    day_count: int = Field(ge=0)
    success_day_count: int = Field(ge=0)
    reused_day_count: int = Field(ge=0)
    failed_day_count: int = Field(ge=0)
    net_liquidation_start: Decimal = Field(ge=Decimal("0"))
    net_liquidation_end: Decimal = Field(ge=Decimal("0"))
    cumulative_realized_pnl: Decimal
    final_unrealized_pnl: Decimal
    cumulative_filled_notional: Decimal = Field(ge=Decimal("0"))
    cumulative_realized_cost: Decimal = Field(ge=Decimal("0"))
    average_fill_rate: float = Field(ge=0)
    average_turnover: Decimal = Field(ge=Decimal("0"))
    average_weight_drift_l1: Decimal = Field(ge=Decimal("0"))
    final_top5_concentration: Decimal = Field(ge=Decimal("0"))
    final_hhi_concentration: Decimal = Field(ge=Decimal("0"))
    average_active_share: Decimal | None = None
    final_active_share: Decimal | None = None
    cumulative_active_contribution_proxy: Decimal | None = None
    max_drawdown: Decimal = Field(ge=Decimal("0"))
    unique_model_count: int = Field(default=0, ge=0)
    retrain_count: int = Field(default=0, ge=0)
    average_model_age_trade_days: Decimal | None = None
    max_model_age_trade_days: int | None = Field(default=None, ge=0)
    strict_checked_day_count: int = Field(default=0, ge=0)
    strict_pass_day_count: int = Field(default=0, ge=0)
    strict_fail_day_count: int = Field(default=0, ge=0)
    warning_day_count: int = Field(default=0, ge=0)
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class CampaignCompareRunRecord(BaseModel):
    campaign_compare_run_id: str = Field(min_length=1)
    left_campaign_run_id: str = Field(min_length=1)
    right_campaign_run_id: str = Field(min_length=1)
    compare_basis: CampaignCompareBasis
    compare_config_hash: str = Field(min_length=1)
    status: CampaignStatus
    created_at: datetime
    source_model_run_ids: list[str] = Field(default_factory=list)
    source_model_schedule_run_ids: list[str] = Field(default_factory=list)
    source_prediction_run_ids: list[str] = Field(default_factory=list)
    source_strategy_run_ids: list[str] = Field(default_factory=list)
    source_execution_task_ids: list[str] = Field(default_factory=list)
    source_paper_run_ids: list[str] = Field(default_factory=list)
    source_shadow_run_ids: list[str] = Field(default_factory=list)
    source_execution_analytics_run_ids: list[str] = Field(default_factory=list)
    source_portfolio_analytics_run_ids: list[str] = Field(default_factory=list)
    source_benchmark_analytics_run_ids: list[str] = Field(default_factory=list)
    source_qlib_export_run_ids: list[str] = Field(default_factory=list)
    source_standard_build_run_ids: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class CampaignCompareManifest(BaseModel):
    campaign_compare_run_id: str = Field(min_length=1)
    left_campaign_run_id: str = Field(min_length=1)
    right_campaign_run_id: str = Field(min_length=1)
    compare_basis: CampaignCompareBasis
    compare_config_hash: str = Field(min_length=1)
    status: CampaignStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    day_rows_file_path: str | None = None
    day_rows_file_hash: str | None = None
    day_row_count: int = Field(default=0, ge=0)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    error_message: str | None = None
    source_model_run_ids: list[str] = Field(default_factory=list)
    source_model_schedule_run_ids: list[str] = Field(default_factory=list)
    source_prediction_run_ids: list[str] = Field(default_factory=list)
    source_strategy_run_ids: list[str] = Field(default_factory=list)
    source_execution_task_ids: list[str] = Field(default_factory=list)
    source_paper_run_ids: list[str] = Field(default_factory=list)
    source_shadow_run_ids: list[str] = Field(default_factory=list)
    source_execution_analytics_run_ids: list[str] = Field(default_factory=list)
    source_portfolio_analytics_run_ids: list[str] = Field(default_factory=list)
    source_benchmark_analytics_run_ids: list[str] = Field(default_factory=list)
    source_qlib_export_run_ids: list[str] = Field(default_factory=list)
    source_standard_build_run_ids: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class CampaignCompareDayRowRecord(BaseModel):
    campaign_compare_run_id: str = Field(min_length=1)
    trade_date: date
    left_campaign_run_id: str = Field(min_length=1)
    right_campaign_run_id: str = Field(min_length=1)
    left_net_liquidation_end: Decimal = Field(ge=Decimal("0"))
    right_net_liquidation_end: Decimal = Field(ge=Decimal("0"))
    delta_net_liquidation_end: Decimal
    left_fill_rate: float = Field(ge=0)
    right_fill_rate: float = Field(ge=0)
    delta_fill_rate: float
    left_active_share: Decimal | None = None
    right_active_share: Decimal | None = None
    delta_active_share: Decimal | None = None
    left_top5_concentration: Decimal = Field(ge=Decimal("0"))
    right_top5_concentration: Decimal = Field(ge=Decimal("0"))
    delta_top5_concentration: Decimal
    left_active_contribution_proxy: Decimal | None = None
    right_active_contribution_proxy: Decimal | None = None
    delta_active_contribution_proxy: Decimal | None = None
    left_model_age_trade_days: int | None = Field(default=None, ge=0)
    right_model_age_trade_days: int | None = Field(default=None, ge=0)
    delta_model_age_trade_days: int | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class CampaignCompareSummaryRecord(BaseModel):
    campaign_compare_run_id: str = Field(min_length=1)
    left_campaign_run_id: str = Field(min_length=1)
    right_campaign_run_id: str = Field(min_length=1)
    compare_basis: CampaignCompareBasis
    overlapping_day_count: int = Field(ge=0)
    delta_net_liquidation_end: Decimal
    delta_cumulative_realized_pnl: Decimal
    delta_cumulative_realized_cost: Decimal
    delta_average_fill_rate: float
    delta_average_turnover: Decimal
    delta_final_active_share: Decimal | None = None
    delta_final_top5_concentration: Decimal
    delta_max_drawdown: Decimal
    delta_cumulative_active_contribution_proxy: Decimal | None = None
    delta_unique_model_count: int = 0
    delta_retrain_count: int = 0
    delta_average_model_age_trade_days: Decimal | None = None
    delta_strict_fail_day_count: int = 0
    delta_warning_day_count: int = 0
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class CampaignLineage(BaseModel):
    campaign_run_id: str = Field(min_length=1)
    model_run_id: str | None = None
    day_row_count: int = Field(ge=0)
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    status: CampaignStatus

    model_config = ConfigDict(extra="forbid")


class CampaignCompareLineage(BaseModel):
    campaign_compare_run_id: str = Field(min_length=1)
    left_campaign_run_id: str = Field(min_length=1)
    right_campaign_run_id: str = Field(min_length=1)
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    status: CampaignStatus

    model_config = ConfigDict(extra="forbid")
