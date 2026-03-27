"""Replay-driven paper-only shadow session runner for M8."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from apps.trade_server.app.paper.runner import (
    _build_market_data_hash,
    _load_accepted_order_intents,
    _load_avg_price_seed,
    _resolve_avg_price_seed_path,
    _resolve_execution_input_paths,
    _resolve_execution_task_manifest,
)
from apps.trade_server.app.planning.ingest import ingest_execution_task_dry_run
from apps.trade_server.app.shadow.bootstrap import ShadowSessionBootstrap
from apps.trade_server.app.shadow.contracts import ShadowSessionResult
from libs.common.time import ensure_cn_aware
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.schemas import PaperExecutionRunRecord, PaperRunStatus
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.execution.shadow_config import load_shadow_session_config
from libs.execution.shadow_engine import ShadowEngineOutcome, run_shadow_engine
from libs.execution.shadow_schemas import (
    ShadowRunStatus,
    ShadowSessionReportRecord,
    ShadowSessionRunRecord,
)
from libs.execution.tick_replay import resolve_tick_replay_source
from libs.marketdata.raw_store import stable_hash
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.planning.rebalance import (
    load_account_snapshot,
    load_market_snapshots,
    load_position_snapshots,
)
from libs.planning.schemas import OrderIntentPreviewRecord
from libs.rules_engine.calendar import load_calendars
from libs.rules_engine.market_rules import RulesRepository
from scripts.load_master_data import load_bootstrap

DEFAULT_SHADOW_CONFIG = Path("configs/execution/shadow_session.yaml")


def run_shadow_session(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str | None = None,
    config_path: Path = DEFAULT_SHADOW_CONFIG,
    account_snapshot_path: Path | None = None,
    positions_path: Path | None = None,
    market_snapshot_path: Path | None = None,
    position_cost_basis_path: Path | None = None,
    market_replay_mode: str | None = None,
    tick_input_path: Path | None = None,
    force: bool = False,
) -> ShadowSessionResult:
    context = ShadowSessionBootstrap(project_root).bootstrap()
    planning_store = context.planning_store
    shadow_store = context.shadow_store
    execution_store = context.execution_store

    ingest_result = ingest_execution_task_dry_run(
        project_root=context.project_root,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_id,
    )
    execution_task_manifest = _resolve_execution_task_manifest(
        planning_store,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_id,
    )
    execution_task = planning_store.load_execution_task(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_manifest.execution_task_id,
    )
    target_manifest = planning_store.load_target_weight_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        strategy_run_id=execution_task.strategy_run_id,
    )
    config = load_shadow_session_config(context.project_root / config_path)
    if market_replay_mode is not None:
        config = config.model_copy(update={"market_replay_mode": market_replay_mode})
    if config.market_replay_mode not in {"bars_1m", "ticks_l1"}:
        raise ValueError(f"unsupported market_replay_mode: {config.market_replay_mode}")
    payload = load_bootstrap(context.project_root / "data" / "master" / "bootstrap")
    catalog = InstrumentCatalog(payload)
    rules_repo = RulesRepository(
        payload.market_rules,
        load_calendars(context.project_root / "data" / "master" / "bootstrap" / "trading_calendar.json"),
    )
    resolved_account_snapshot_path, resolved_positions_path, resolved_market_snapshot_path = (
        _resolve_execution_input_paths(
            project_root=context.project_root,
            trade_date=trade_date,
            account_snapshot_path=account_snapshot_path,
            positions_path=positions_path,
            market_snapshot_path=market_snapshot_path,
        )
    )
    account_snapshot = load_account_snapshot(resolved_account_snapshot_path)
    positions = load_position_snapshots(resolved_positions_path)
    market_snapshots = load_market_snapshots(resolved_market_snapshot_path)
    avg_price_by_instrument = _load_avg_price_seed(
        seed_path=_resolve_avg_price_seed_path(
            project_root=context.project_root,
            positions_path=positions_path,
            position_cost_basis_path=position_cost_basis_path,
        ),
        positions=positions,
        market_snapshots=market_snapshots,
    )
    preview_frame = _load_accepted_order_intents(
        planning_store,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task.execution_task_id,
    )
    preview_records = [
        OrderIntentPreviewRecord.model_validate(item)
        for item in preview_frame.to_dict(orient="records")
    ]
    fill_model_config_hash = stable_hash(config.model_dump(mode="json"))
    wanted_instruments = {
        (item.instrument_key, item.symbol, item.exchange) for item in preview_records
    }
    market_snapshot_hash = stable_hash(
        {
            key: value.model_dump(mode="json")
            for key, value in sorted(market_snapshots.items())
        }
    )
    tick_source = None
    tick_source_hash: str | None = None
    if config.market_replay_mode == "ticks_l1":
        tick_source = resolve_tick_replay_source(
            project_root=context.project_root,
            trade_date=trade_date,
            wanted_instruments=wanted_instruments,
            tick_input_path=tick_input_path,
        )
        tick_source_hash = tick_source.tick_source_hash
        market_data_hash = stable_hash(
            {
                "mode": config.market_replay_mode,
                "tick_source_hash": tick_source_hash,
                "market_snapshots": market_snapshot_hash,
            }
        )
    else:
        market_data_hash = _build_market_data_hash(
            project_root=context.project_root,
            trade_date=trade_date,
            intents=preview_frame.to_dict(orient="records"),
            source_standard_build_run_id=execution_task.source_standard_build_run_id,
            market_snapshots=market_snapshots,
        )
    account_state_hash = stable_hash(
        {
            "account": account_snapshot.model_dump(mode="json"),
            "positions": {
                key: value.model_dump(mode="json") for key, value in sorted(positions.items())
            },
            "avg_price": {key: str(value) for key, value in sorted(avg_price_by_instrument.items())},
        }
    )
    shadow_run_id = "shadow_" + stable_hash(
        {
            "execution_task_id": execution_task.execution_task_id,
            "fill_model_config_hash": fill_model_config_hash,
            "market_data_hash": market_data_hash,
            "account_state_hash": account_state_hash,
            "market_replay_mode": config.market_replay_mode,
            "tick_source_hash": tick_source_hash,
        }
    )[:12]
    paper_run_id = "paper_" + stable_hash(
        {
            "shadow_run_id": shadow_run_id,
            "mode": "shadow",
        }
    )[:12]

    if shadow_store.has_run(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        shadow_run_id=shadow_run_id,
    ):
        existing_run = shadow_store.load_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        )
        if existing_run.status == ShadowRunStatus.SUCCESS and not force:
            manifest = shadow_store.load_manifest(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                shadow_run_id=shadow_run_id,
            )
            report = shadow_store.load_report(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                shadow_run_id=shadow_run_id,
            )
            return ShadowSessionResult(
                shadow_run_id=shadow_run_id,
                paper_run_id=existing_run.paper_run_id or paper_run_id,
                execution_task_id=execution_task.execution_task_id,
                strategy_run_id=execution_task.strategy_run_id,
                account_id=account_id,
                basket_id=basket_id,
                trade_date=trade_date,
                status=ShadowRunStatus.SUCCESS,
                order_count=report.order_count,
                fill_count=manifest.fill_events_count,
                report_path=manifest.report_file_path,
                paper_report_path=manifest.paper_report_file_path,
                send_order_called=False,
                reused=True,
            )
        shadow_store.clear_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        )
    if execution_store.has_run(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        paper_run_id=paper_run_id,
    ):
        execution_store.clear_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        )

    created_at = ensure_cn_aware(datetime.now())
    base_run = ShadowSessionRunRecord(
        shadow_run_id=shadow_run_id,
        paper_run_id=paper_run_id,
        strategy_run_id=execution_task.strategy_run_id,
        execution_task_id=execution_task.execution_task_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        market_replay_mode=config.market_replay_mode,
        fill_model_name=config.fill_model_name,
        fill_model_config_hash=fill_model_config_hash,
        market_data_hash=market_data_hash,
        account_state_hash=account_state_hash,
        tick_source_hash=tick_source_hash,
        status=ShadowRunStatus.CREATED,
        started_at=None,
        ended_at=None,
        created_at=created_at,
        source_prediction_run_id=target_manifest.prediction_run_id,
        source_qlib_export_run_id=execution_task.source_qlib_export_run_id,
        source_standard_build_run_id=execution_task.source_standard_build_run_id,
    )
    shadow_store.save_run(base_run)

    try:
        outcome = run_shadow_engine(
            project_root=context.project_root,
            trade_date=trade_date,
            shadow_run_id=shadow_run_id,
            paper_run_id=paper_run_id,
            execution_task_id=execution_task.execution_task_id,
            strategy_run_id=execution_task.strategy_run_id,
            account_id=account_id,
            basket_id=basket_id,
            previews=preview_records,
            account_snapshot=account_snapshot,
            positions=positions,
            market_snapshots=market_snapshots,
            avg_price_by_instrument=avg_price_by_instrument,
            catalog=catalog,
            payload=payload,
            rules_repo=rules_repo,
            config=config,
            created_at=created_at,
            tick_frames_by_instrument=(
                tick_source.frames_by_instrument if tick_source is not None else None
            ),
            source_prediction_run_id=target_manifest.prediction_run_id,
            source_qlib_export_run_id=execution_task.source_qlib_export_run_id,
            source_standard_build_run_id=execution_task.source_standard_build_run_id,
        )
        started_at, ended_at = _resolve_shadow_bounds(created_at=created_at, outcome=outcome)
        paper_run = PaperExecutionRunRecord(
            paper_run_id=paper_run_id,
            strategy_run_id=execution_task.strategy_run_id,
            execution_task_id=execution_task.execution_task_id,
            account_id=account_id,
            basket_id=basket_id,
            trade_date=trade_date,
            fill_model_name=config.fill_model_name,
            fill_model_config_hash=fill_model_config_hash,
            market_data_hash=market_data_hash,
            account_state_hash=account_state_hash,
            status=PaperRunStatus.SUCCESS,
            created_at=created_at,
            source_prediction_run_id=target_manifest.prediction_run_id,
            source_qlib_export_run_id=execution_task.source_qlib_export_run_id,
            source_standard_build_run_id=execution_task.source_standard_build_run_id,
        )
        paper_manifest = execution_store.save_success(
            run=paper_run,
            orders=outcome.paper_orders,
            trades=outcome.paper_trades,
            account_snapshot=outcome.account_snapshot,
            positions=outcome.position_snapshots,
            report=outcome.paper_reconcile_report,
        )
        shadow_report = _build_shadow_report(
            run=base_run,
            outcome=outcome,
            created_at=created_at,
        ).model_copy(update={"paper_run_id": paper_run_id})
        success_run = base_run.model_copy(
            update={
                "status": ShadowRunStatus.SUCCESS,
                "paper_run_id": paper_run_id,
                "started_at": started_at,
                "ended_at": ended_at,
            }
        )
        manifest = shadow_store.save_success(
            run=success_run,
            order_events=outcome.order_events,
            fill_events=outcome.fill_events,
            report=shadow_report,
            paper_report_path=paper_manifest.report_file_path,
            paper_report_hash=paper_manifest.report_file_hash,
        )
    except Exception as exc:
        if execution_store.has_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        ):
            execution_store.clear_run(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                paper_run_id=paper_run_id,
            )
        failed_run = base_run.model_copy(update={"status": ShadowRunStatus.FAILED})
        shadow_store.save_failed_run(failed_run, error_message=str(exc))
        raise

    return ShadowSessionResult(
        shadow_run_id=success_run.shadow_run_id,
        paper_run_id=paper_run_id,
        execution_task_id=execution_task.execution_task_id,
        strategy_run_id=execution_task.strategy_run_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        status=success_run.status,
        order_count=shadow_report.order_count,
        fill_count=len(outcome.fill_events),
        report_path=manifest.report_file_path,
        paper_report_path=manifest.paper_report_file_path,
        send_order_called=context.send_order_called or ingest_result.send_order_called,
        reused=False,
    )


def load_shadow_session_reconcile(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    shadow_run_id: str | None = None,
    execution_task_id: str | None = None,
    latest: bool = False,
) -> dict[str, object]:
    shadow_store = ShadowArtifactStore(project_root)
    execution_store = ExecutionArtifactStore(project_root)
    resolved_run = _select_shadow_run(
        store=shadow_store,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        shadow_run_id=shadow_run_id,
        execution_task_id=execution_task_id,
        latest=latest,
    )
    manifest = shadow_store.load_manifest(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        shadow_run_id=resolved_run.shadow_run_id,
    )
    shadow_report = shadow_store.load_report(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        shadow_run_id=resolved_run.shadow_run_id,
    )
    paper_report = None
    if (
        resolved_run.paper_run_id is not None
        and execution_store.has_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=resolved_run.paper_run_id,
        )
    ):
        paper_report = execution_store.load_reconcile_report(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=resolved_run.paper_run_id,
        )
    return {
        "shadow_run": resolved_run.model_dump(mode="json"),
        "shadow_manifest": manifest.model_dump(mode="json"),
        "shadow_report": shadow_report.model_dump(mode="json"),
        "paper_report": paper_report.model_dump(mode="json") if paper_report is not None else None,
    }


def _select_shadow_run(
    store: ShadowArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    shadow_run_id: str | None,
    execution_task_id: str | None,
    latest: bool,
) -> ShadowSessionRunRecord:
    runs = [
        run
        for run in store.list_runs()
        if run.trade_date == trade_date
        and run.account_id == account_id
        and run.basket_id == basket_id
    ]
    if not runs:
        raise FileNotFoundError(
            f"no shadow run found for trade_date={trade_date.isoformat()} "
            f"account_id={account_id} basket_id={basket_id}"
        )
    if shadow_run_id is not None:
        for run in runs:
            if run.shadow_run_id == shadow_run_id:
                return run
        raise FileNotFoundError(
            f"no shadow run found for shadow_run_id={shadow_run_id} "
            f"trade_date={trade_date.isoformat()} account_id={account_id} basket_id={basket_id}"
        )
    filtered_runs = runs
    if execution_task_id is not None:
        filtered_runs = [run for run in runs if run.execution_task_id == execution_task_id]
        if not filtered_runs:
            raise FileNotFoundError(
                f"no shadow run found for execution_task_id={execution_task_id} "
                f"trade_date={trade_date.isoformat()} account_id={account_id} basket_id={basket_id}"
            )
    if len(filtered_runs) == 1:
        return filtered_runs[0]
    if latest or shadow_run_id is None:
        return filtered_runs[0]
    return filtered_runs[0]


def _resolve_shadow_bounds(
    *,
    created_at: datetime,
    outcome: ShadowEngineOutcome,
) -> tuple[datetime, datetime]:
    timestamps = [item.event_dt for item in outcome.order_events]
    timestamps.extend(item.fill_dt for item in outcome.fill_events)
    if not timestamps:
        return created_at, created_at
    started_at = min(timestamps)
    ended_at = max(timestamps)
    return started_at, ended_at


def _build_shadow_report(
    *,
    run: ShadowSessionRunRecord,
    outcome: ShadowEngineOutcome,
    created_at: datetime,
) -> ShadowSessionReportRecord:
    filled_order_ids = {item.order_id for item in outcome.paper_trades}
    filled_order_id_list: list[str | int | float | bool | None] = []
    filled_order_id_list.extend(sorted(filled_order_ids))
    expired_orders = [
        item
        for item in outcome.paper_orders
        if item.status.value == "unfilled" and item.status_reason == "expired_end_of_session"
    ]
    rejected_orders = [item for item in outcome.paper_orders if item.status.value == "rejected"]
    unfilled_orders = [
        item
        for item in outcome.paper_orders
        if item.status.value == "unfilled" and item.status_reason != "expired_end_of_session"
    ]
    filled_notional = sum((item.notional for item in outcome.paper_trades), Decimal("0")).quantize(
        Decimal("0.01")
    )
    realized_cost_total = sum(
        (Decimal(str(item.cost_breakdown_json["total"])) for item in outcome.paper_trades),
        Decimal("0"),
    ).quantize(Decimal("0.01"))
    session_summary: dict[
        str,
        str | int | float | bool | list[str | int | float | bool | None] | None,
    ] = {
        "paper_run_id": run.paper_run_id,
        "fill_event_count": len(outcome.fill_events),
        "filled_order_ids": filled_order_id_list,
        "session_mode": run.market_replay_mode,
        "tick_source_hash": run.tick_source_hash,
    }
    return ShadowSessionReportRecord(
        shadow_run_id=run.shadow_run_id,
        paper_run_id=run.paper_run_id,
        execution_task_id=run.execution_task_id,
        strategy_run_id=run.strategy_run_id,
        account_id=run.account_id,
        basket_id=run.basket_id,
        trade_date=run.trade_date,
        order_count=len(outcome.paper_orders),
        filled_order_count=len(filled_order_ids),
        expired_order_count=len(expired_orders),
        rejected_order_count=len(rejected_orders),
        unfilled_order_count=len(unfilled_orders),
        filled_notional=filled_notional,
        realized_cost_total=realized_cost_total,
        session_summary_json=session_summary,
        created_at=created_at,
        source_prediction_run_id=run.source_prediction_run_id,
        source_qlib_export_run_id=run.source_qlib_export_run_id,
        source_standard_build_run_id=run.source_standard_build_run_id,
    )
