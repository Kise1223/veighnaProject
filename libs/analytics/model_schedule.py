"""M15 rolling model schedule helpers."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from apps.research_qlib.bootstrap import load_dataset_config, load_runtime_config
from apps.research_qlib.workflow import (
    DEFAULT_BASE_CONFIG,
    DEFAULT_DATASET_CONFIG,
    train_baseline_workflow,
)
from libs.analytics.explicit_model_schedule import load_explicit_model_schedule
from libs.analytics.model_schedule_config import default_model_schedule_config
from libs.analytics.model_schedule_schemas import (
    ModelScheduleAction,
    ModelScheduleMode,
    TrainingWindowMode,
)
from libs.marketdata.raw_store import file_sha256, stable_hash
from libs.research.artifacts import ResearchArtifactStore
from libs.research.schemas import BaselineDatasetConfig, ModelRunRecord, ResearchRunStatus
from libs.rules_engine.calendar import is_trade_day, load_calendars
from libs.schemas.master_data import ExchangeCode


@dataclass(frozen=True)
class ResolvedModelScheduleDay:
    trade_date: date
    schedule_action: ModelScheduleAction
    resolved_model_run_id: str
    train_start: date | None
    train_end: date | None
    model_switch_flag: bool
    model_age_trade_days: int
    days_since_last_retrain: int
    model_train_reused: bool


@dataclass(frozen=True)
class ResolvedModelSchedule:
    model_schedule_run_id: str
    schedule_mode: ModelScheduleMode
    fixed_model_run_id: str | None
    latest_model_resolved_run_id: str | None
    retrain_every_n_trade_days: int | None
    training_window_mode: TrainingWindowMode
    lookback_trade_days: int | None
    days: list[ResolvedModelScheduleDay]
    config_hash: str


def build_model_schedule_run_id(
    *,
    date_start: date,
    date_end: date,
    account_id: str,
    basket_id: str,
    schedule_mode: ModelScheduleMode,
    fixed_model_run_id: str | None,
    latest_model_resolved_run_id: str | None,
    retrain_every_n_trade_days: int | None,
    training_window_mode: TrainingWindowMode,
    lookback_trade_days: int | None,
    explicit_schedule_path: str | None,
    explicit_schedule_hash: str | None,
    benchmark_enabled: bool,
    benchmark_source_type: str,
    campaign_config_hash: str,
) -> str:
    return "mschedule_" + stable_hash(
        {
            "date_start": date_start.isoformat(),
            "date_end": date_end.isoformat(),
            "account_id": account_id,
            "basket_id": basket_id,
            "schedule_mode": schedule_mode.value,
            "fixed_model_run_id": fixed_model_run_id,
            "latest_model_resolved_run_id": latest_model_resolved_run_id,
            "retrain_every_n_trade_days": retrain_every_n_trade_days,
            "training_window_mode": training_window_mode.value,
            "lookback_trade_days": lookback_trade_days,
            "explicit_schedule_path": explicit_schedule_path,
            "explicit_schedule_hash": explicit_schedule_hash,
            "benchmark_enabled": benchmark_enabled,
            "benchmark_source_type": benchmark_source_type,
            "campaign_config_hash": campaign_config_hash,
        }
    )[:12]


def resolve_model_schedule(
    *,
    project_root: Path,
    trade_dates: Sequence[date],
    account_id: str,
    basket_id: str,
    schedule_mode: str,
    model_run_id: str | None = None,
    latest_model: bool = False,
    retrain_every_n_trade_days: int | None = None,
    training_window_mode: str = "expanding_to_prior_day",
    lookback_trade_days: int | None = None,
    schedule_path: Path | None = None,
    benchmark_enabled: bool,
    benchmark_source_type: str,
    campaign_config_hash: str,
    force: bool = False,
) -> ResolvedModelSchedule:
    if not trade_dates:
        raise ValueError("trade_dates must not be empty")
    mode = ModelScheduleMode(schedule_mode)
    window_mode = TrainingWindowMode(training_window_mode)
    if mode == ModelScheduleMode.FIXED_MODEL and model_run_id is not None and latest_model:
        raise ValueError("--model-run-id and --latest-model are mutually exclusive")
    if mode == ModelScheduleMode.RETRAIN_EVERY_N_TRADE_DAYS:
        if retrain_every_n_trade_days is None or retrain_every_n_trade_days < 1:
            raise ValueError("--retrain-every-n-trade-days must be >= 1")
        if model_run_id is not None or latest_model:
            raise ValueError("fixed model selectors are not allowed for retrain_every_n_trade_days")
        if window_mode == TrainingWindowMode.ROLLING_LOOKBACK and (
            lookback_trade_days is None or lookback_trade_days < 1
        ):
            raise ValueError("--lookback-trade-days must be >= 1 for rolling_lookback")
    if mode == ModelScheduleMode.EXPLICIT_MODEL_SCHEDULE:
        if schedule_path is None:
            raise ValueError("--schedule-path is required for explicit_model_schedule")
        if model_run_id is not None or latest_model or retrain_every_n_trade_days is not None:
            raise ValueError("explicit_model_schedule does not accept fixed-model or retrain cadence selectors")
    dataset_config = load_dataset_config(project_root / DEFAULT_DATASET_CONFIG)
    trade_dates_all = _resolve_trade_dates_between(
        project_root=project_root,
        date_start=dataset_config.train_start,
        date_end=trade_dates[-1],
    )
    trade_index_by_date = {value: index for index, value in enumerate(trade_dates_all)}
    fixed_model_run_id: str | None = None
    latest_model_resolved_run_id: str | None = None
    if mode == ModelScheduleMode.FIXED_MODEL:
        resolved_fixed_model = _resolve_successful_model_run_id(
            project_root=project_root,
            model_run_id=model_run_id,
        )
        fixed_model_run_id = model_run_id if model_run_id is not None else resolved_fixed_model
        latest_model_resolved_run_id = resolved_fixed_model if model_run_id is None else None
        model_record = _load_model_run(project_root=project_root, run_id=resolved_fixed_model)
        days = _build_fixed_schedule_days(
            trade_dates=trade_dates,
            model_record=model_record,
            trade_index_by_date=trade_index_by_date,
        )
    elif mode == ModelScheduleMode.RETRAIN_EVERY_N_TRADE_DAYS:
        days = _build_retrain_schedule_days(
            project_root=project_root,
            trade_dates=trade_dates,
            dataset_config=dataset_config,
            retrain_every_n_trade_days=retrain_every_n_trade_days or 1,
            training_window_mode=window_mode,
            lookback_trade_days=lookback_trade_days,
            trade_dates_all=trade_dates_all,
            trade_index_by_date=trade_index_by_date,
            force=force,
        )
    else:
        days = _build_explicit_schedule_days(
            project_root=project_root,
            trade_dates=list(trade_dates),
            schedule_path=schedule_path or Path(""),
            trade_index_by_date=trade_index_by_date,
        )
    config_hash = stable_hash(
        {
            "model_schedule_config": default_model_schedule_config().model_dump(mode="json"),
            "schedule_mode": mode.value,
            "fixed_model_run_id": fixed_model_run_id,
            "latest_model_resolved_run_id": latest_model_resolved_run_id,
            "retrain_every_n_trade_days": retrain_every_n_trade_days,
            "training_window_mode": window_mode.value,
            "lookback_trade_days": lookback_trade_days,
            "explicit_schedule_path": str(schedule_path) if schedule_path is not None else None,
            "explicit_schedule_hash": file_sha256(schedule_path) if schedule_path is not None else None,
            "benchmark_enabled": benchmark_enabled,
            "benchmark_source_type": benchmark_source_type,
            "campaign_config_hash": campaign_config_hash,
        }
    )
    return ResolvedModelSchedule(
        model_schedule_run_id=build_model_schedule_run_id(
            date_start=trade_dates[0],
            date_end=trade_dates[-1],
            account_id=account_id,
            basket_id=basket_id,
            schedule_mode=mode,
            fixed_model_run_id=fixed_model_run_id,
            latest_model_resolved_run_id=latest_model_resolved_run_id,
            retrain_every_n_trade_days=retrain_every_n_trade_days,
            training_window_mode=window_mode,
            lookback_trade_days=lookback_trade_days,
            explicit_schedule_path=str(schedule_path) if schedule_path is not None else None,
            explicit_schedule_hash=file_sha256(schedule_path) if schedule_path is not None else None,
            benchmark_enabled=benchmark_enabled,
            benchmark_source_type=benchmark_source_type,
            campaign_config_hash=campaign_config_hash,
        ),
        schedule_mode=mode,
        fixed_model_run_id=fixed_model_run_id,
        latest_model_resolved_run_id=latest_model_resolved_run_id,
        retrain_every_n_trade_days=retrain_every_n_trade_days,
        training_window_mode=window_mode,
        lookback_trade_days=lookback_trade_days,
        days=days,
        config_hash=config_hash,
    )


def _build_fixed_schedule_days(
    *,
    trade_dates: Sequence[date],
    model_record: ModelRunRecord,
    trade_index_by_date: dict[date, int],
) -> list[ResolvedModelScheduleDay]:
    age = _trade_day_distance(
        trade_index_by_date=trade_index_by_date,
        start_date=model_record.train_end,
        end_date=trade_dates[0],
    )
    days: list[ResolvedModelScheduleDay] = []
    for index, trade_date in enumerate(trade_dates):
        if index > 0:
            age = max(
                age,
                _trade_day_distance(
                    trade_index_by_date=trade_index_by_date,
                    start_date=model_record.train_end,
                    end_date=trade_date,
                ),
            )
        days.append(
            ResolvedModelScheduleDay(
                trade_date=trade_date,
                schedule_action=ModelScheduleAction.FIXED_REUSE,
                resolved_model_run_id=model_record.run_id,
                train_start=model_record.train_start,
                train_end=model_record.train_end,
                model_switch_flag=False,
                model_age_trade_days=age,
                days_since_last_retrain=age,
                model_train_reused=True,
            )
        )
    return days


def _build_retrain_schedule_days(
    *,
    project_root: Path,
    trade_dates: Sequence[date],
    dataset_config: BaselineDatasetConfig,
    retrain_every_n_trade_days: int,
    training_window_mode: TrainingWindowMode,
    lookback_trade_days: int | None,
    trade_dates_all: Sequence[date],
    trade_index_by_date: dict[date, int],
    force: bool,
) -> list[ResolvedModelScheduleDay]:
    days: list[ResolvedModelScheduleDay] = []
    previous_model_run_id: str | None = None
    last_retrain_trade_date: date | None = None
    for index, trade_date in enumerate(trade_dates):
        refresh_day = index == 0 or index % retrain_every_n_trade_days == 0
        if refresh_day:
            prior_trade_date = _previous_trade_date(trade_dates_all=trade_dates_all, trade_date=trade_date)
            if prior_trade_date is None:
                raise ValueError(
                    f"no prior trade date available for retrain trade_date={trade_date.isoformat()}"
                )
            dataset_override = _build_dataset_override(
                dataset_config=dataset_config,
                trade_date=trade_date,
                training_window_mode=training_window_mode,
                lookback_trade_days=lookback_trade_days,
                prior_trade_date=prior_trade_date,
                trade_dates_all=trade_dates_all,
            )
            train_result = train_baseline_workflow(
                project_root=project_root,
                dataset_config_override=dataset_override,
                force=force,
            )
            resolved_model_run_id = str(train_result["run_id"])
            model_record = _load_model_run(project_root=project_root, run_id=resolved_model_run_id)
            days.append(
                ResolvedModelScheduleDay(
                    trade_date=trade_date,
                    schedule_action=ModelScheduleAction.RETRAINED_NEW_MODEL,
                    resolved_model_run_id=resolved_model_run_id,
                    train_start=model_record.train_start,
                    train_end=model_record.train_end,
                    model_switch_flag=False if index == 0 else resolved_model_run_id != previous_model_run_id,
                    model_age_trade_days=_trade_day_distance(
                        trade_index_by_date=trade_index_by_date,
                        start_date=model_record.train_end,
                        end_date=trade_date,
                    ),
                    days_since_last_retrain=0,
                    model_train_reused=bool(train_result["reused"]),
                )
            )
            previous_model_run_id = resolved_model_run_id
            last_retrain_trade_date = trade_date
        else:
            if previous_model_run_id is None or last_retrain_trade_date is None:
                raise ValueError("retrain schedule has no prior model to reuse")
            model_record = _load_model_run(project_root=project_root, run_id=previous_model_run_id)
            days.append(
                ResolvedModelScheduleDay(
                    trade_date=trade_date,
                    schedule_action=ModelScheduleAction.REUSED_PRIOR_MODEL,
                    resolved_model_run_id=previous_model_run_id,
                    train_start=model_record.train_start,
                    train_end=model_record.train_end,
                    model_switch_flag=False,
                    model_age_trade_days=_trade_day_distance(
                        trade_index_by_date=trade_index_by_date,
                        start_date=model_record.train_end,
                        end_date=trade_date,
                    ),
                    days_since_last_retrain=_trade_day_distance(
                        trade_index_by_date=trade_index_by_date,
                        start_date=last_retrain_trade_date,
                        end_date=trade_date,
                    ),
                    model_train_reused=True,
                )
            )
    return days


def _build_dataset_override(
    *,
    dataset_config: BaselineDatasetConfig,
    trade_date: date,
    training_window_mode: TrainingWindowMode,
    lookback_trade_days: int | None,
    prior_trade_date: date,
    trade_dates_all: Sequence[date],
) -> BaselineDatasetConfig:
    if training_window_mode == TrainingWindowMode.EXPANDING_TO_PRIOR_DAY:
        train_start = dataset_config.train_start
    elif training_window_mode == TrainingWindowMode.ROLLING_LOOKBACK:
        if lookback_trade_days is None or lookback_trade_days < 1:
            raise ValueError("lookback_trade_days is required and must be >= 1 for rolling_lookback")
        prior_index = trade_dates_all.index(prior_trade_date)
        train_start = trade_dates_all[max(0, prior_index - lookback_trade_days + 1)]
        min_train_rows = min(
            dataset_config.min_train_rows,
            max(1, lookback_trade_days * dataset_config.min_instruments),
        )
    else:
        raise ValueError(f"unsupported training_window_mode: {training_window_mode.value}")
    if training_window_mode == TrainingWindowMode.EXPANDING_TO_PRIOR_DAY:
        min_train_rows = dataset_config.min_train_rows
    return dataset_config.model_copy(
        update={
            "train_start": train_start,
            "train_end": prior_trade_date,
            "infer_trade_date": trade_date,
            "min_train_rows": min_train_rows,
        }
    )


def _resolve_successful_model_run_id(*, project_root: Path, model_run_id: str | None) -> str:
    runtime_config = load_runtime_config(project_root / DEFAULT_BASE_CONFIG)
    store = ResearchArtifactStore(project_root, project_root / runtime_config.artifacts_root)
    if model_run_id is not None:
        run = store.load_run(model_run_id)
        if run.status != ResearchRunStatus.SUCCESS:
            raise ValueError(f"model_run {model_run_id} is not successful")
        return run.run_id
    for run in store.list_runs():
        if run.status == ResearchRunStatus.SUCCESS:
            return run.run_id
    raise FileNotFoundError("no successful model_run artifact found")


def _load_model_run(*, project_root: Path, run_id: str) -> ModelRunRecord:
    runtime_config = load_runtime_config(project_root / DEFAULT_BASE_CONFIG)
    store = ResearchArtifactStore(project_root, project_root / runtime_config.artifacts_root)
    return store.load_run(run_id)


def load_model_run_metadata(*, project_root: Path, run_id: str) -> ModelRunRecord | None:
    return _load_model_run(project_root=project_root, run_id=run_id)


def _resolve_trade_dates_between(
    *, project_root: Path, date_start: date, date_end: date
) -> list[date]:
    calendars = load_calendars(project_root / "data" / "master" / "bootstrap" / "trading_calendar.json")
    results: list[date] = []
    current = date_start
    while current <= date_end:
        if is_trade_day(current, ExchangeCode.SSE, calendars):
            results.append(current)
        current += timedelta(days=1)
    return results


def _previous_trade_date(*, trade_dates_all: Sequence[date], trade_date: date) -> date | None:
    try:
        current_index = trade_dates_all.index(trade_date)
    except ValueError as exc:
        raise ValueError(f"trade_date={trade_date.isoformat()} is missing from trade calendar") from exc
    if current_index == 0:
        return None
    return trade_dates_all[current_index - 1]


def _build_explicit_schedule_days(
    *,
    project_root: Path,
    trade_dates: Sequence[date],
    schedule_path: Path,
    trade_index_by_date: dict[date, int],
) -> list[ResolvedModelScheduleDay]:
    resolved_rows = load_explicit_model_schedule(
        project_root=project_root,
        trade_dates=list(trade_dates),
        schedule_path=schedule_path,
    )
    days: list[ResolvedModelScheduleDay] = []
    previous_model_run_id: str | None = None
    last_switch_trade_date: date | None = None
    for index, item in enumerate(resolved_rows):
        metadata = load_model_run_metadata(project_root=project_root, run_id=item.resolved_model_run_id)
        train_start = metadata.train_start if metadata is not None else None
        train_end = metadata.train_end if metadata is not None else None
        model_switch_flag = False if index == 0 else item.resolved_model_run_id != previous_model_run_id
        if index == 0 or model_switch_flag:
            last_switch_trade_date = item.trade_date
        days.append(
            ResolvedModelScheduleDay(
                trade_date=item.trade_date,
                schedule_action=ModelScheduleAction.EXPLICIT_MODEL,
                resolved_model_run_id=item.resolved_model_run_id,
                train_start=train_start,
                train_end=train_end,
                model_switch_flag=model_switch_flag,
                model_age_trade_days=(
                    _trade_day_distance(
                        trade_index_by_date=trade_index_by_date,
                        start_date=train_end,
                        end_date=item.trade_date,
                    )
                    if train_end is not None
                    else 0
                ),
                days_since_last_retrain=(
                    _trade_day_distance(
                        trade_index_by_date=trade_index_by_date,
                        start_date=last_switch_trade_date,
                        end_date=item.trade_date,
                    )
                    if last_switch_trade_date is not None
                    else 0
                ),
                model_train_reused=True,
            )
        )
        previous_model_run_id = item.resolved_model_run_id
    return days


def _trade_day_distance(
    *,
    trade_index_by_date: dict[date, int],
    start_date: date,
    end_date: date,
) -> int:
    start_index = trade_index_by_date.get(start_date)
    end_index = trade_index_by_date.get(end_date)
    if start_index is None or end_index is None or end_index <= start_index:
        return 0
    return end_index - start_index
