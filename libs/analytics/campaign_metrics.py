"""Metric helpers for M14 walk-forward campaign analytics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import cast

from libs.analytics.attribution_schemas import BenchmarkSummaryRowRecord
from libs.analytics.campaign_schemas import (
    CampaignDayRowRecord,
    CampaignSummaryRecord,
    CampaignTimeseriesRowRecord,
    JsonScalar,
)
from libs.analytics.portfolio_normalize import quantize_weight
from libs.analytics.portfolio_schemas import PortfolioSummaryRecord
from libs.analytics.schemas import ExecutionTcaSummaryRecord

_ZERO = Decimal("0")


def build_campaign_timeseries_row(
    *,
    campaign_run_id: str,
    trade_date: date,
    execution_summary: ExecutionTcaSummaryRecord,
    portfolio_summary: PortfolioSummaryRecord,
    benchmark_summary: BenchmarkSummaryRowRecord | None,
    model_switch_flag: bool | None,
    model_age_trade_days: int | None,
    days_since_last_retrain: int | None,
    replay_mode: str | None,
    fill_model_name: str | None,
    time_in_force: str | None,
    created_at: datetime,
) -> CampaignTimeseriesRowRecord:
    return CampaignTimeseriesRowRecord(
        campaign_run_id=campaign_run_id,
        trade_date=trade_date,
        net_liquidation_end=portfolio_summary.net_liquidation_end,
        cash_weight_end=portfolio_summary.executed_cash_weight,
        daily_realized_pnl=portfolio_summary.total_realized_pnl,
        daily_unrealized_pnl=portfolio_summary.total_unrealized_pnl,
        daily_filled_notional=execution_summary.total_filled_notional,
        daily_realized_cost=execution_summary.total_realized_cost,
        daily_turnover=portfolio_summary.realized_turnover,
        daily_fill_rate=execution_summary.gross_fill_rate,
        daily_weight_drift_l1=portfolio_summary.total_weight_drift_l1,
        daily_top5_concentration=portfolio_summary.top5_concentration,
        daily_hhi_concentration=portfolio_summary.hhi_concentration,
        daily_active_share=(
            benchmark_summary.executed_active_share if benchmark_summary is not None else None
        ),
        daily_active_contribution_proxy=(
            benchmark_summary.total_active_contribution_proxy
            if benchmark_summary is not None
            else None
        ),
        daily_model_switch_flag=model_switch_flag,
        daily_model_age_trade_days=model_age_trade_days,
        daily_days_since_last_retrain=days_since_last_retrain,
        replay_mode=replay_mode,
        fill_model_name=fill_model_name,
        time_in_force=time_in_force,
        created_at=created_at,
    )


def build_campaign_summary(
    *,
    campaign_run_id: str,
    day_rows: Sequence[CampaignDayRowRecord],
    timeseries_rows: Sequence[CampaignTimeseriesRowRecord],
    net_liquidation_start: Decimal,
    created_at: datetime,
) -> CampaignSummaryRecord:
    ordered = sorted(timeseries_rows, key=lambda item: item.trade_date)
    if not ordered:
        raise ValueError("campaign summary requires at least one timeseries row")
    success_day_count = sum(1 for item in day_rows if item.day_status.value == "success")
    reused_day_count = sum(1 for item in day_rows if item.day_status.value == "reused")
    failed_day_count = sum(1 for item in day_rows if item.day_status.value == "failed")
    active_shares = [item.daily_active_share for item in ordered if item.daily_active_share is not None]
    model_ages = [
        Decimal(str(item.daily_model_age_trade_days))
        for item in ordered
        if item.daily_model_age_trade_days is not None
    ]
    active_contribution_values = [
        item.daily_active_contribution_proxy
        for item in ordered
        if item.daily_active_contribution_proxy is not None
    ]
    average_active_share = _mean_decimal(active_shares)
    average_model_age_trade_days = _mean_decimal(model_ages)
    cumulative_active_contribution_proxy = (
        sum(active_contribution_values, _ZERO).quantize(Decimal("0.000001"))
        if active_contribution_values
        else None
    )
    final_row = ordered[-1]
    unique_model_count = len({item.model_run_id for item in day_rows if item.model_run_id})
    retrain_count = sum(
        1
        for item in day_rows
        if item.schedule_action == "retrained_new_model"
    )
    strict_checked_day_count = sum(
        1 for item in day_rows if item.strict_no_lookahead_expected is True
    )
    strict_pass_day_count = sum(
        1
        for item in day_rows
        if item.strict_no_lookahead_expected is True and item.strict_no_lookahead_passed is True
    )
    warning_day_count = sum(1 for item in day_rows if item.schedule_warning_code is not None)
    summary_json = cast(
        dict[str, JsonScalar | Sequence[JsonScalar]],
        {
            "trade_dates": [item.trade_date.isoformat() for item in ordered],
            "active_metrics_enabled": bool(active_shares or active_contribution_values),
            "max_drawdown_mode": "peak_to_trough_net_liquidation_end",
            "model_schedule_enabled": any(item.model_schedule_run_id is not None for item in day_rows),
            "warning_codes": sorted(
                {
                    item.schedule_warning_code
                    for item in day_rows
                    if item.schedule_warning_code is not None
                }
            ),
        },
    )
    return CampaignSummaryRecord(
        campaign_run_id=campaign_run_id,
        day_count=len(day_rows),
        success_day_count=success_day_count,
        reused_day_count=reused_day_count,
        failed_day_count=failed_day_count,
        net_liquidation_start=net_liquidation_start.quantize(Decimal("0.01")),
        net_liquidation_end=final_row.net_liquidation_end.quantize(Decimal("0.01")),
        cumulative_realized_pnl=sum((item.daily_realized_pnl for item in ordered), _ZERO).quantize(
            Decimal("0.01")
        ),
        final_unrealized_pnl=final_row.daily_unrealized_pnl.quantize(Decimal("0.01")),
        cumulative_filled_notional=sum(
            (item.daily_filled_notional for item in ordered), _ZERO
        ).quantize(Decimal("0.01")),
        cumulative_realized_cost=sum((item.daily_realized_cost for item in ordered), _ZERO).quantize(
            Decimal("0.01")
        ),
        average_fill_rate=_mean_float([item.daily_fill_rate for item in ordered]),
        average_turnover=_mean_decimal([item.daily_turnover for item in ordered]) or _ZERO,
        average_weight_drift_l1=_mean_decimal([item.daily_weight_drift_l1 for item in ordered]) or _ZERO,
        final_top5_concentration=final_row.daily_top5_concentration,
        final_hhi_concentration=final_row.daily_hhi_concentration,
        average_active_share=average_active_share,
        final_active_share=final_row.daily_active_share,
        cumulative_active_contribution_proxy=cumulative_active_contribution_proxy,
        max_drawdown=compute_max_drawdown([item.net_liquidation_end for item in ordered]),
        unique_model_count=unique_model_count,
        retrain_count=retrain_count,
        average_model_age_trade_days=average_model_age_trade_days,
        max_model_age_trade_days=max(
            (item.daily_model_age_trade_days for item in ordered if item.daily_model_age_trade_days is not None),
            default=None,
        ),
        strict_checked_day_count=strict_checked_day_count,
        strict_pass_day_count=strict_pass_day_count,
        strict_fail_day_count=max(0, strict_checked_day_count - strict_pass_day_count),
        warning_day_count=warning_day_count,
        summary_json=summary_json,
        created_at=created_at,
    )


def compute_max_drawdown(values: Sequence[Decimal]) -> Decimal:
    peak = _ZERO
    max_drawdown = _ZERO
    for value in values:
        if value > peak:
            peak = value
        if peak <= _ZERO:
            continue
        drawdown = (peak - value) / peak
        if drawdown > max_drawdown:
            max_drawdown = drawdown
    return quantize_weight(max_drawdown)


def _mean_decimal(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    return quantize_weight(sum(values, _ZERO) / Decimal(len(values)))


def _mean_float(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 4)
