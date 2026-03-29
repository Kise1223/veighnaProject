"""M14 walk-forward campaign orchestration over existing M5-M13 artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from apps.research_qlib.bootstrap import load_runtime_config
from apps.research_qlib.workflow import run_daily_inference
from apps.trade_server.app.paper.runner import run_paper_execution
from apps.trade_server.app.shadow.session import run_shadow_session
from libs.analytics.artifacts import ExecutionAnalyticsArtifactStore
from libs.analytics.benchmark_attribution import (
    build_benchmark_reference,
    run_benchmark_analytics,
)
from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_config import default_campaign_config
from libs.analytics.campaign_metrics import build_campaign_summary, build_campaign_timeseries_row
from libs.analytics.campaign_schemas import (
    CampaignDayRowRecord,
    CampaignDayStatus,
    CampaignExecutionSourceType,
    CampaignRunRecord,
    CampaignStatus,
    CampaignTimeseriesRowRecord,
    JsonScalar,
)
from libs.analytics.portfolio import run_portfolio_analytics
from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.analytics.tca import run_execution_tca
from libs.common.time import ensure_cn_aware
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.execution.shadow_config import load_shadow_session_config
from libs.marketdata.raw_store import file_sha256, stable_hash
from libs.planning.rebalance import plan_rebalance
from libs.planning.target_weights import build_target_weights
from libs.research.artifacts import ResearchArtifactStore
from libs.research.schemas import ResearchRunStatus
from libs.rules_engine.calendar import is_trade_day, load_calendars
from libs.schemas.master_data import ExchangeCode

_DEFAULT_QLIB_BASE_CONFIG = Path("configs/qlib/base.yaml")


@dataclass(frozen=True)
class _ResolvedExecutionConfig:
    execution_source_type: CampaignExecutionSourceType
    market_replay_mode: str | None
    tick_fill_model: str | None
    time_in_force: str | None


@dataclass(frozen=True)
class _DayArtifacts:
    day_row: CampaignDayRowRecord
    timeseries_row: CampaignTimeseriesRowRecord
    net_liquidation_start: Decimal


def run_walkforward_campaign(
    *,
    project_root: Path,
    date_start: date,
    date_end: date,
    account_id: str,
    basket_id: str,
    model_run_id: str | None = None,
    latest_model: bool = False,
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
    if model_run_id is not None and latest_model:
        raise ValueError("--model-run-id and --latest-model are mutually exclusive")
    if benchmark_source_type == "custom_weights" and benchmark_path is None:
        raise ValueError("--benchmark-path is required when --benchmark-source-type=custom_weights")
    resolved_model_run_id = _resolve_model_run_id(
        project_root=project_root,
        model_run_id=model_run_id,
    )
    resolved_execution = _resolve_execution_config(
        project_root=project_root,
        execution_source_type=execution_source_type,
        market_replay_mode=market_replay_mode,
        tick_fill_model=tick_fill_model,
        time_in_force=time_in_force,
    )
    benchmark_enabled = benchmark_source_type != "none"
    benchmark_source_hash = file_sha256(benchmark_path) if benchmark_path is not None else None
    config_hash = stable_hash(
        {
            "campaign_config": default_campaign_config().model_dump(mode="json"),
            "benchmark_source_hash": benchmark_source_hash,
        }
    )
    campaign_run_id = build_campaign_run_id(
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        execution_source_type=resolved_execution.execution_source_type,
        market_replay_mode=resolved_execution.market_replay_mode,
        tick_fill_model=resolved_execution.tick_fill_model,
        time_in_force=resolved_execution.time_in_force,
        benchmark_source_type=benchmark_source_type,
        model_run_id=resolved_model_run_id,
        campaign_config_hash=config_hash,
    )
    store = CampaignArtifactStore(project_root)
    if store.has_campaign_run(
        date_start=date_start,
        date_end=date_end,
        account_id=account_id,
        basket_id=basket_id,
        campaign_run_id=campaign_run_id,
    ):
        existing = store.load_campaign_run(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            campaign_run_id=campaign_run_id,
        )
        if existing.status == CampaignStatus.SUCCESS and not force:
            manifest = store.load_campaign_manifest(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            summary = store.load_campaign_summary(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            return {
                "campaign_run_id": campaign_run_id,
                "trade_date_count": manifest.timeseries_row_count,
                "summary_path": manifest.summary_file_path,
                "status": existing.status.value,
                "average_fill_rate": summary.average_fill_rate,
                "reused": True,
            }
        store.clear_campaign_run(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            campaign_run_id=campaign_run_id,
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
    created_at = ensure_cn_aware(datetime.now())
    run = CampaignRunRecord(
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
        model_run_id=resolved_model_run_id,
        campaign_config_hash=config_hash,
        status=CampaignStatus.CREATED,
        created_at=created_at,
    )
    store.save_campaign_run(run)

    day_rows: list[CampaignDayRowRecord] = []
    timeseries_rows = []
    net_liquidation_start = None
    try:
        for trade_date in trade_dates:
            day_artifacts = _run_campaign_day(
                project_root=project_root,
                campaign_run_id=campaign_run_id,
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                model_run_id=resolved_model_run_id,
                execution=resolved_execution,
                benchmark_source_type=benchmark_source_type,
                benchmark_path=benchmark_path,
                created_at=created_at,
            )
            day_rows.append(day_artifacts.day_row)
            timeseries_rows.append(day_artifacts.timeseries_row)
            if net_liquidation_start is None:
                net_liquidation_start = day_artifacts.net_liquidation_start
    except Exception as exc:
        failed_run = run.model_copy(update={"status": CampaignStatus.FAILED})
        failed_day_row = _build_failed_day_row(
            campaign_run_id=campaign_run_id,
            trade_date=trade_date,
            model_run_id=resolved_model_run_id,
            created_at=created_at,
            error_summary=str(exc),
        )
        day_rows.append(failed_day_row)
        store.save_failed_campaign_run(
            failed_run,
            error_message=str(exc),
            day_rows=day_rows,
            timeseries_rows=timeseries_rows,
        )
        raise

    if net_liquidation_start is None:
        raise ValueError("campaign did not produce any successful day rows")
    summary = build_campaign_summary(
        campaign_run_id=campaign_run_id,
        day_rows=day_rows,
        timeseries_rows=timeseries_rows,
        net_liquidation_start=net_liquidation_start,
        created_at=created_at,
    )
    success_run = run.model_copy(update={"status": CampaignStatus.SUCCESS})
    manifest = store.save_campaign_success(
        run=success_run,
        day_rows=day_rows,
        timeseries_rows=timeseries_rows,
        summary=summary,
    )
    return {
        "campaign_run_id": campaign_run_id,
        "trade_date_count": len(timeseries_rows),
        "summary_path": manifest.summary_file_path,
        "status": success_run.status.value,
        "average_fill_rate": summary.average_fill_rate,
        "reused": False,
    }


def build_campaign_run_id(
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
    model_run_id: str,
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
            "model_run_id": model_run_id,
            "campaign_config_hash": campaign_config_hash,
        }
    )[:12]


def _run_campaign_day(
    *,
    project_root: Path,
    campaign_run_id: str,
    trade_date: date,
    account_id: str,
    basket_id: str,
    model_run_id: str,
    execution: _ResolvedExecutionConfig,
    benchmark_source_type: str,
    benchmark_path: Path | None,
    created_at: datetime,
) -> _DayArtifacts:
    prediction_run_id = None
    strategy_run_id = None
    execution_task_id = None
    paper_run_id = None
    shadow_run_id = None
    execution_analytics_run_id = None
    portfolio_analytics_run_id = None
    benchmark_run_id = None
    benchmark_analytics_run_id = None
    source_qlib_export_run_id = None
    source_standard_build_run_id = None
    reused_flags: dict[str, bool] = {}

    inference_result = run_daily_inference(
        project_root=project_root,
        trade_date=trade_date,
        run_id=model_run_id,
    )
    prediction_run_id = str(inference_result["run_id"])
    reused_flags["prediction"] = bool(inference_result["reused"])

    target_result = build_target_weights(
        project_root=project_root,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        approved_by="walkforward_campaign",
        prediction_run_id=prediction_run_id,
    )
    strategy_run_id = str(target_result["strategy_run_id"])
    reused_flags["approved_target_weight"] = bool(target_result["reused"])

    rebalance_result = plan_rebalance(
        project_root=project_root,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        strategy_run_id=strategy_run_id,
    )
    execution_task_id = str(rebalance_result["execution_task_id"])
    reused_flags["execution_task"] = bool(rebalance_result["reused"])

    if execution.execution_source_type == CampaignExecutionSourceType.SHADOW:
        shadow_result = run_shadow_session(
            project_root=project_root,
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
            market_replay_mode=execution.market_replay_mode,
            tick_fill_model=execution.tick_fill_model,
            time_in_force=execution.time_in_force,
        )
        shadow_run_id = shadow_result.shadow_run_id
        paper_run_id = shadow_result.paper_run_id
        reused_flags["execution_source"] = bool(shadow_result.reused)
        analytics_result = run_execution_tca(
            project_root=project_root,
            shadow_run_id=shadow_run_id,
        )
        execution_analytics_run_id = str(analytics_result["analytics_run_id"])
        reused_flags["execution_analytics"] = bool(analytics_result["reused"])
        portfolio_result = run_portfolio_analytics(
            project_root=project_root,
            shadow_run_id=shadow_run_id,
        )
        portfolio_analytics_run_id = str(portfolio_result["portfolio_analytics_run_id"])
        reused_flags["portfolio_analytics"] = bool(portfolio_result["reused"])
        shadow_store = ShadowArtifactStore(project_root)
        shadow_run = shadow_store.load_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        )
        replay_mode = shadow_run.market_replay_mode
        fill_model_name = shadow_run.fill_model_name
        time_in_force = shadow_run.time_in_force
        source_qlib_export_run_id = shadow_run.source_qlib_export_run_id
        source_standard_build_run_id = shadow_run.source_standard_build_run_id
    else:
        paper_result = run_paper_execution(
            project_root=project_root,
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        )
        paper_run_id = paper_result.paper_run_id
        reused_flags["execution_source"] = bool(paper_result.reused)
        analytics_result = run_execution_tca(
            project_root=project_root,
            paper_run_id=paper_run_id,
        )
        execution_analytics_run_id = str(analytics_result["analytics_run_id"])
        reused_flags["execution_analytics"] = bool(analytics_result["reused"])
        portfolio_result = run_portfolio_analytics(
            project_root=project_root,
            paper_run_id=paper_run_id,
        )
        portfolio_analytics_run_id = str(portfolio_result["portfolio_analytics_run_id"])
        reused_flags["portfolio_analytics"] = bool(portfolio_result["reused"])
        execution_store = ExecutionArtifactStore(project_root)
        paper_run = execution_store.load_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        )
        replay_mode = None
        fill_model_name = paper_run.fill_model_name
        time_in_force = None
        source_qlib_export_run_id = paper_run.source_qlib_export_run_id
        source_standard_build_run_id = paper_run.source_standard_build_run_id

    execution_summary_store = ExecutionAnalyticsArtifactStore(project_root)
    execution_summary = execution_summary_store.load_analytics_summary(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        analytics_run_id=execution_analytics_run_id,
    )
    portfolio_store = PortfolioAnalyticsArtifactStore(project_root)
    portfolio_summary = portfolio_store.load_portfolio_summary(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        portfolio_analytics_run_id=portfolio_analytics_run_id,
    )
    benchmark_summary = None
    if benchmark_source_type != "none":
        benchmark_result = build_benchmark_reference(
            project_root=project_root,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
            source_type=benchmark_source_type,
            benchmark_path=benchmark_path,
        )
        benchmark_run_id = str(benchmark_result["benchmark_run_id"])
        reused_flags["benchmark_reference"] = bool(benchmark_result["reused"])
        benchmark_analytics_result = run_benchmark_analytics(
            project_root=project_root,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
            benchmark_run_id=benchmark_run_id,
        )
        benchmark_analytics_run_id = str(
            benchmark_analytics_result["benchmark_analytics_run_id"]
        )
        reused_flags["benchmark_analytics"] = bool(benchmark_analytics_result["reused"])
        from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore

        benchmark_store = BenchmarkAttributionArtifactStore(project_root)
        benchmark_summary = benchmark_store.load_benchmark_summary(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            benchmark_analytics_run_id=benchmark_analytics_run_id,
        )
    day_status = (
        CampaignDayStatus.REUSED if reused_flags and all(reused_flags.values()) else CampaignDayStatus.SUCCESS
    )
    reused_flags_payload: dict[str, JsonScalar | Sequence[JsonScalar]] = dict(reused_flags)
    day_row = CampaignDayRowRecord(
        campaign_run_id=campaign_run_id,
        trade_date=trade_date,
        day_status=day_status,
        model_run_id=model_run_id,
        prediction_run_id=prediction_run_id,
        strategy_run_id=strategy_run_id,
        execution_task_id=execution_task_id,
        paper_run_id=paper_run_id,
        shadow_run_id=shadow_run_id,
        execution_analytics_run_id=execution_analytics_run_id,
        portfolio_analytics_run_id=portfolio_analytics_run_id,
        benchmark_run_id=benchmark_run_id,
        benchmark_analytics_run_id=benchmark_analytics_run_id,
        reused_flags_json=reused_flags_payload,
        error_summary=None,
        created_at=created_at,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )
    timeseries_row = build_campaign_timeseries_row(
        campaign_run_id=campaign_run_id,
        trade_date=trade_date,
        execution_summary=execution_summary,
        portfolio_summary=portfolio_summary,
        benchmark_summary=benchmark_summary,
        replay_mode=replay_mode,
        fill_model_name=fill_model_name,
        time_in_force=time_in_force,
        created_at=created_at,
    )
    return _DayArtifacts(
        day_row=day_row,
        timeseries_row=timeseries_row,
        net_liquidation_start=portfolio_summary.net_liquidation_start,
    )


def _build_failed_day_row(
    *,
    campaign_run_id: str,
    trade_date: date,
    model_run_id: str,
    created_at: datetime,
    error_summary: str,
) -> CampaignDayRowRecord:
    return CampaignDayRowRecord(
        campaign_run_id=campaign_run_id,
        trade_date=trade_date,
        day_status=CampaignDayStatus.FAILED,
        model_run_id=model_run_id,
        reused_flags_json={},
        error_summary=error_summary,
        created_at=created_at,
    )


def _resolve_model_run_id(*, project_root: Path, model_run_id: str | None) -> str:
    runtime_config = load_runtime_config(project_root / _DEFAULT_QLIB_BASE_CONFIG)
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


def _resolve_execution_config(
    *,
    project_root: Path,
    execution_source_type: str,
    market_replay_mode: str | None,
    tick_fill_model: str | None,
    time_in_force: str | None,
) -> _ResolvedExecutionConfig:
    source_type = CampaignExecutionSourceType(execution_source_type)
    if source_type == CampaignExecutionSourceType.PAPER:
        return _ResolvedExecutionConfig(
            execution_source_type=source_type,
            market_replay_mode=None,
            tick_fill_model=None,
            time_in_force=None,
        )
    config = load_shadow_session_config(project_root / "configs" / "execution" / "shadow_session.yaml")
    resolved_market_replay_mode = market_replay_mode or config.market_replay_mode
    resolved_tick_fill_model = (
        tick_fill_model or config.tick_fill_model if resolved_market_replay_mode == "ticks_l1" else None
    )
    resolved_time_in_force = (
        (time_in_force or config.time_in_force).upper()
        if resolved_market_replay_mode == "ticks_l1"
        else None
    )
    return _ResolvedExecutionConfig(
        execution_source_type=source_type,
        market_replay_mode=resolved_market_replay_mode,
        tick_fill_model=resolved_tick_fill_model,
        time_in_force=resolved_time_in_force,
    )


def _resolve_trade_dates(
    *, project_root: Path, date_start: date, date_end: date
) -> list[date]:
    calendars = load_calendars(project_root / "data" / "master" / "bootstrap" / "trading_calendar.json")
    current = date_start
    results: list[date] = []
    while current <= date_end:
        if is_trade_day(current, ExchangeCode.SSE, calendars):
            results.append(current)
        current += timedelta(days=1)
    return results
