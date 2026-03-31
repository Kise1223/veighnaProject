"""M15 rolling retrain campaign orchestration on top of M14 campaigns."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import cast

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_config import default_campaign_config
from libs.analytics.campaign_metrics import build_campaign_summary
from libs.analytics.campaign_runner import (
    _build_failed_day_row,  # noqa: PLC2701
    _resolve_execution_config,  # noqa: PLC2701
    _resolve_trade_dates,  # noqa: PLC2701
    _run_campaign_day,  # noqa: PLC2701
)
from libs.analytics.campaign_schemas import (
    CampaignDayRowRecord,
    CampaignExecutionSourceType,
    CampaignRunRecord,
    CampaignStatus,
    CampaignTimeseriesRowRecord,
    JsonScalar,
)
from libs.analytics.model_schedule import resolve_model_schedule
from libs.analytics.model_schedule_artifacts import ModelScheduleArtifactStore
from libs.analytics.model_schedule_schemas import (
    ModelScheduleDayRowRecord,
    ModelScheduleRunRecord,
    ModelScheduleStatus,
)
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import file_sha256, stable_hash


def run_rolling_campaign(
    *,
    project_root: Path,
    date_start: date,
    date_end: date,
    account_id: str,
    basket_id: str,
    schedule_mode: str,
    model_run_id: str | None = None,
    latest_model: bool = False,
    retrain_every_n_trade_days: int | None = None,
    training_window_mode: str = "expanding_to_prior_day",
    lookback_trade_days: int | None = None,
    execution_source_type: str = "shadow",
    market_replay_mode: str | None = None,
    tick_fill_model: str | None = None,
    time_in_force: str | None = None,
    benchmark_source_type: str = "none",
    benchmark_path: Path | None = None,
    force: bool = False,
) -> dict[str, object]:
    if date_end < date_start:
        raise ValueError("date_end must be on or after date_start")
    if benchmark_source_type == "custom_weights" and benchmark_path is None:
        raise ValueError("--benchmark-path is required when --benchmark-source-type=custom_weights")
    benchmark_enabled = benchmark_source_type != "none"
    benchmark_source_hash = file_sha256(benchmark_path) if benchmark_path is not None else None
    campaign_config_hash = stable_hash(
        {
            "campaign_config": default_campaign_config().model_dump(mode="json"),
            "benchmark_source_hash": benchmark_source_hash,
        }
    )
    resolved_execution = _resolve_execution_config(
        project_root=project_root,
        execution_source_type=execution_source_type,
        market_replay_mode=market_replay_mode,
        tick_fill_model=tick_fill_model,
        time_in_force=time_in_force,
    )
    trade_dates = _resolve_trade_dates(
        project_root=project_root,
        date_start=date_start,
        date_end=date_end,
    )
    if not trade_dates:
        raise ValueError(
            f"no trade dates found between {date_start.isoformat()} and {date_end.isoformat()}"
        )
    schedule = resolve_model_schedule(
        project_root=project_root,
        trade_dates=trade_dates,
        account_id=account_id,
        basket_id=basket_id,
        schedule_mode=schedule_mode,
        model_run_id=model_run_id,
        latest_model=latest_model,
        retrain_every_n_trade_days=retrain_every_n_trade_days,
        training_window_mode=training_window_mode,
        lookback_trade_days=lookback_trade_days,
        benchmark_enabled=benchmark_enabled,
        benchmark_source_type=benchmark_source_type,
        campaign_config_hash=campaign_config_hash,
        force=force,
    )
    campaign_run_id = _build_rolling_campaign_run_id(
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        execution_source_type=resolved_execution.execution_source_type,
        market_replay_mode=resolved_execution.market_replay_mode,
        tick_fill_model=resolved_execution.tick_fill_model,
        time_in_force=resolved_execution.time_in_force,
        benchmark_source_type=benchmark_source_type,
        model_schedule_run_id=schedule.model_schedule_run_id,
        schedule_mode=schedule.schedule_mode.value,
        fixed_model_run_id=schedule.fixed_model_run_id,
        latest_model_resolved_run_id=schedule.latest_model_resolved_run_id,
        retrain_every_n_trade_days=schedule.retrain_every_n_trade_days,
        training_window_mode=schedule.training_window_mode.value,
        lookback_trade_days=schedule.lookback_trade_days,
        campaign_config_hash=campaign_config_hash,
    )
    campaign_store = CampaignArtifactStore(project_root)
    schedule_store = ModelScheduleArtifactStore(project_root)
    if (
        schedule_store.has_schedule_run(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            model_schedule_run_id=schedule.model_schedule_run_id,
        )
        and campaign_store.has_campaign_run(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            campaign_run_id=campaign_run_id,
        )
        and not force
    ):
        existing_schedule = schedule_store.load_schedule_run(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            model_schedule_run_id=schedule.model_schedule_run_id,
        )
        existing_campaign = campaign_store.load_campaign_run(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            campaign_run_id=campaign_run_id,
        )
        if (
            existing_schedule.status == ModelScheduleStatus.SUCCESS
            and existing_campaign.status == CampaignStatus.SUCCESS
        ):
            manifest = campaign_store.load_campaign_manifest(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            summary = campaign_store.load_campaign_summary(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            return {
                "model_schedule_run_id": schedule.model_schedule_run_id,
                "campaign_run_id": campaign_run_id,
                "trade_date_count": manifest.timeseries_row_count,
                "summary_path": manifest.summary_file_path,
                "status": existing_campaign.status.value,
                "average_fill_rate": summary.average_fill_rate,
                "reused": True,
            }
    if schedule_store.has_schedule_run(
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        model_schedule_run_id=schedule.model_schedule_run_id,
    ):
        schedule_store.clear_schedule_run(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            model_schedule_run_id=schedule.model_schedule_run_id,
        )
    if campaign_store.has_campaign_run(
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        campaign_run_id=campaign_run_id,
    ):
        campaign_store.clear_campaign_run(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            campaign_run_id=campaign_run_id,
        )
    created_at = ensure_cn_aware(datetime.now())
    schedule_run = ModelScheduleRunRecord(
        model_schedule_run_id=schedule.model_schedule_run_id,
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        schedule_mode=schedule.schedule_mode,
        fixed_model_run_id=schedule.fixed_model_run_id,
        latest_model_resolved_run_id=schedule.latest_model_resolved_run_id,
        retrain_every_n_trade_days=schedule.retrain_every_n_trade_days,
        training_window_mode=schedule.training_window_mode,
        lookback_trade_days=schedule.lookback_trade_days,
        explicit_schedule_path=None,
        benchmark_enabled=benchmark_enabled,
        benchmark_source_type=benchmark_source_type,
        campaign_config_hash=schedule.config_hash,
        campaign_run_id=campaign_run_id,
        status=ModelScheduleStatus.CREATED,
        created_at=created_at,
    )
    schedule_store.save_schedule_run(schedule_run)
    campaign_run = CampaignRunRecord(
        campaign_run_id=campaign_run_id,
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        execution_source_type=resolved_execution.execution_source_type,
        market_replay_mode=resolved_execution.market_replay_mode,
        tick_fill_model=resolved_execution.tick_fill_model,
        time_in_force=resolved_execution.time_in_force,
        benchmark_enabled=benchmark_enabled,
        benchmark_source_type=benchmark_source_type,
        model_run_id=schedule.days[0].resolved_model_run_id,
        model_schedule_run_id=schedule.model_schedule_run_id,
        schedule_mode=schedule.schedule_mode.value,
        fixed_model_run_id=schedule.fixed_model_run_id,
        latest_model_resolved_run_id=schedule.latest_model_resolved_run_id,
        retrain_every_n_trade_days=schedule.retrain_every_n_trade_days,
        training_window_mode=schedule.training_window_mode.value,
        lookback_trade_days=schedule.lookback_trade_days,
        campaign_config_hash=campaign_config_hash,
        status=CampaignStatus.CREATED,
        created_at=created_at,
    )
    campaign_store.save_campaign_run(campaign_run)

    campaign_day_rows: list[CampaignDayRowRecord] = []
    timeseries_rows: list[CampaignTimeseriesRowRecord] = []
    schedule_day_rows: list[ModelScheduleDayRowRecord] = []
    net_liquidation_start = None
    try:
        for scheduled_day in schedule.days:
            day_artifacts = _run_campaign_day(
                project_root=project_root,
                campaign_run_id=campaign_run_id,
                trade_date=scheduled_day.trade_date,
                account_id=account_id,
                basket_id=basket_id,
                model_run_id=scheduled_day.resolved_model_run_id,
                execution=resolved_execution,
                benchmark_source_type=benchmark_source_type,
                benchmark_path=benchmark_path,
                created_at=created_at,
            )
            merged_reused_flags: dict[str, JsonScalar | list[JsonScalar]] = cast(
                dict[str, JsonScalar | list[JsonScalar]],
                dict(day_artifacts.day_row.reused_flags_json),
            )
            merged_reused_flags["model_train_reused"] = scheduled_day.model_train_reused
            patched_day_row = day_artifacts.day_row.model_copy(
                update={
                    "model_schedule_run_id": schedule.model_schedule_run_id,
                    "schedule_action": scheduled_day.schedule_action.value,
                    "train_start": scheduled_day.train_start,
                    "train_end": scheduled_day.train_end,
                    "model_switch_flag": scheduled_day.model_switch_flag,
                    "model_age_trade_days": scheduled_day.model_age_trade_days,
                    "days_since_last_retrain": scheduled_day.days_since_last_retrain,
                    "reused_flags_json": merged_reused_flags,
                }
            )
            patched_timeseries_row = day_artifacts.timeseries_row.model_copy(
                update={
                    "daily_model_switch_flag": scheduled_day.model_switch_flag,
                    "daily_model_age_trade_days": scheduled_day.model_age_trade_days,
                    "daily_days_since_last_retrain": scheduled_day.days_since_last_retrain,
                }
            )
            schedule_reused_flags_json: dict[str, JsonScalar | Sequence[JsonScalar]] = cast(
                dict[str, JsonScalar | Sequence[JsonScalar]],
                merged_reused_flags,
            )
            schedule_day_rows.append(
                ModelScheduleDayRowRecord(
                    model_schedule_run_id=schedule.model_schedule_run_id,
                    campaign_run_id=campaign_run_id,
                    trade_date=scheduled_day.trade_date,
                    schedule_action=scheduled_day.schedule_action,
                    resolved_model_run_id=scheduled_day.resolved_model_run_id,
                    resolved_prediction_run_id=patched_day_row.prediction_run_id,
                    train_start=scheduled_day.train_start,
                    train_end=scheduled_day.train_end,
                    model_switch_flag=scheduled_day.model_switch_flag,
                    model_age_trade_days=scheduled_day.model_age_trade_days,
                    days_since_last_retrain=scheduled_day.days_since_last_retrain,
                    day_status=patched_day_row.day_status.value,
                    reused_flags_json=schedule_reused_flags_json,
                    error_summary=None,
                    created_at=created_at,
                    strategy_run_id=patched_day_row.strategy_run_id,
                    execution_task_id=patched_day_row.execution_task_id,
                    paper_run_id=patched_day_row.paper_run_id,
                    shadow_run_id=patched_day_row.shadow_run_id,
                    execution_analytics_run_id=patched_day_row.execution_analytics_run_id,
                    portfolio_analytics_run_id=patched_day_row.portfolio_analytics_run_id,
                    benchmark_analytics_run_id=patched_day_row.benchmark_analytics_run_id,
                    source_qlib_export_run_id=patched_day_row.source_qlib_export_run_id,
                    source_standard_build_run_id=patched_day_row.source_standard_build_run_id,
                )
            )
            campaign_day_rows.append(patched_day_row)
            timeseries_rows.append(patched_timeseries_row)
            if net_liquidation_start is None:
                net_liquidation_start = day_artifacts.net_liquidation_start
    except Exception as exc:
        failed_campaign_run = campaign_run.model_copy(update={"status": CampaignStatus.FAILED})
        failed_day_row = _build_failed_day_row(
            campaign_run_id=campaign_run_id,
            trade_date=scheduled_day.trade_date,
            model_run_id=scheduled_day.resolved_model_run_id,
            created_at=created_at,
            error_summary=str(exc),
        ).model_copy(
            update={
                "model_schedule_run_id": schedule.model_schedule_run_id,
                "schedule_action": scheduled_day.schedule_action.value,
                "train_start": scheduled_day.train_start,
                "train_end": scheduled_day.train_end,
                "model_switch_flag": scheduled_day.model_switch_flag,
                "model_age_trade_days": scheduled_day.model_age_trade_days,
                "days_since_last_retrain": scheduled_day.days_since_last_retrain,
            }
        )
        campaign_day_rows.append(failed_day_row)
        schedule_day_rows.append(
            ModelScheduleDayRowRecord(
                model_schedule_run_id=schedule.model_schedule_run_id,
                campaign_run_id=campaign_run_id,
                trade_date=scheduled_day.trade_date,
                schedule_action=scheduled_day.schedule_action,
                resolved_model_run_id=scheduled_day.resolved_model_run_id,
                resolved_prediction_run_id=None,
                train_start=scheduled_day.train_start,
                train_end=scheduled_day.train_end,
                model_switch_flag=scheduled_day.model_switch_flag,
                model_age_trade_days=scheduled_day.model_age_trade_days,
                days_since_last_retrain=scheduled_day.days_since_last_retrain,
                day_status="failed",
                reused_flags_json={"model_train_reused": scheduled_day.model_train_reused},
                error_summary=str(exc),
                created_at=created_at,
            )
        )
        campaign_store.save_failed_campaign_run(
            failed_campaign_run,
            error_message=str(exc),
            day_rows=campaign_day_rows,
            timeseries_rows=timeseries_rows,
        )
        schedule_store.save_failed_schedule_run(
            schedule_run.model_copy(update={"status": ModelScheduleStatus.FAILED}),
            error_message=str(exc),
            day_rows=schedule_day_rows,
        )
        raise
    if net_liquidation_start is None:
        raise ValueError("rolling campaign did not produce any successful day rows")
    summary = build_campaign_summary(
        campaign_run_id=campaign_run_id,
        day_rows=campaign_day_rows,
        timeseries_rows=timeseries_rows,
        net_liquidation_start=net_liquidation_start,
        created_at=created_at,
    )
    success_campaign_run = campaign_run.model_copy(update={"status": CampaignStatus.SUCCESS})
    manifest = campaign_store.save_campaign_success(
        run=success_campaign_run,
        day_rows=campaign_day_rows,
        timeseries_rows=timeseries_rows,
        summary=summary,
    )
    schedule_store.save_schedule_success(
        run=schedule_run.model_copy(
            update={
                "status": ModelScheduleStatus.SUCCESS,
                "campaign_run_id": campaign_run_id,
            }
        ),
        day_rows=schedule_day_rows,
    )
    return {
        "model_schedule_run_id": schedule.model_schedule_run_id,
        "campaign_run_id": campaign_run_id,
        "trade_date_count": len(timeseries_rows),
        "summary_path": manifest.summary_file_path,
        "status": success_campaign_run.status.value,
        "average_fill_rate": summary.average_fill_rate,
        "reused": False,
    }


def _build_rolling_campaign_run_id(
    *,
    date_start: date,
    date_end: date,
    account_id: str,
    basket_id: str,
    execution_source_type: CampaignExecutionSourceType,
    market_replay_mode: str | None,
    tick_fill_model: str | None,
    time_in_force: str | None,
    benchmark_source_type: str,
    model_schedule_run_id: str,
    schedule_mode: str,
    fixed_model_run_id: str | None,
    latest_model_resolved_run_id: str | None,
    retrain_every_n_trade_days: int | None,
    training_window_mode: str,
    lookback_trade_days: int | None,
    campaign_config_hash: str,
) -> str:
    return "campaign_" + stable_hash(
        {
            "date_start": date_start.isoformat(),
            "date_end": date_end.isoformat(),
            "account_id": account_id,
            "basket_id": basket_id,
            "execution_source_type": execution_source_type.value,
            "market_replay_mode": market_replay_mode,
            "tick_fill_model": tick_fill_model,
            "time_in_force": time_in_force,
            "benchmark_source_type": benchmark_source_type,
            "model_schedule_run_id": model_schedule_run_id,
            "schedule_mode": schedule_mode,
            "fixed_model_run_id": fixed_model_run_id,
            "latest_model_resolved_run_id": latest_model_resolved_run_id,
            "retrain_every_n_trade_days": retrain_every_n_trade_days,
            "training_window_mode": training_window_mode,
            "lookback_trade_days": lookback_trade_days,
            "campaign_config_hash": campaign_config_hash,
        }
    )[:12]
