"""Load and normalize M7-M10 execution artifacts for M11 analytics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from libs.analytics.normalize import (
    NormalizedExecutionFill,
    NormalizedExecutionOrder,
    NormalizedExecutionSource,
)
from libs.analytics.schemas import ExecutionSourceType, SessionEndStatus
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.schemas import PaperExecutionRunRecord, PaperOrderStatus, PaperRunStatus
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.execution.shadow_schemas import ShadowOrderState, ShadowRunStatus, ShadowSessionRunRecord


@dataclass(frozen=True)
class LoadedExecutionSource:
    source: NormalizedExecutionSource
    source_run_ids: list[str]
    source_execution_task_ids: list[str]
    source_strategy_run_ids: list[str]
    source_prediction_run_ids: list[str]
    source_qlib_export_run_ids: list[str]
    source_standard_build_run_ids: list[str]


def select_execution_source(
    *,
    project_root: Path,
    trade_date: date | None = None,
    account_id: str | None = None,
    basket_id: str | None = None,
    paper_run_id: str | None = None,
    shadow_run_id: str | None = None,
    latest: bool = False,
) -> LoadedExecutionSource:
    if paper_run_id and shadow_run_id:
        raise ValueError("--paper-run-id and --shadow-run-id are mutually exclusive")
    if paper_run_id is not None:
        paper_run = _find_paper_run_by_id(project_root=project_root, paper_run_id=paper_run_id)
        return _load_paper_source(project_root=project_root, run=paper_run)
    if shadow_run_id is not None:
        shadow_run = _find_shadow_run_by_id(project_root=project_root, shadow_run_id=shadow_run_id)
        return _load_shadow_source(project_root=project_root, run=shadow_run)
    if trade_date is None or account_id is None or basket_id is None:
        raise ValueError(
            "either --paper-run-id/--shadow-run-id or --trade-date/--account-id/--basket-id is required"
        )
    matches = _select_matching_runs(
        project_root=project_root,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
    )
    if not matches:
        raise FileNotFoundError(
            f"no execution source found for trade_date={trade_date.isoformat()} "
            f"account_id={account_id} basket_id={basket_id}"
        )
    if len(matches) > 1 and not latest:
        raise ValueError(
            "multiple execution sources match; pass --paper-run-id, --shadow-run-id, or --latest"
        )
    selected_type = matches[0][0]
    if selected_type == "paper":
        return _load_paper_source(
            project_root=project_root,
            run=cast(PaperExecutionRunRecord, matches[0][1]),
        )
    return _load_shadow_source(
        project_root=project_root,
        run=cast(ShadowSessionRunRecord, matches[0][1]),
    )


def _select_matching_runs(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
) -> list[tuple[str, Any]]:
    paper_store = ExecutionArtifactStore(project_root)
    shadow_store = ShadowArtifactStore(project_root)
    matches: list[tuple[str, Any]] = []
    for paper_run in paper_store.list_runs():
        if (
            paper_run.trade_date == trade_date
            and paper_run.account_id == account_id
            and paper_run.basket_id == basket_id
        ):
            matches.append(("paper", paper_run))
    for shadow_run in shadow_store.list_runs():
        if (
            shadow_run.trade_date == trade_date
            and shadow_run.account_id == account_id
            and shadow_run.basket_id == basket_id
        ):
            matches.append(("shadow", shadow_run))
    matches.sort(key=lambda item: item[1].created_at, reverse=True)
    return matches


def _find_paper_run_by_id(*, project_root: Path, paper_run_id: str) -> PaperExecutionRunRecord:
    store = ExecutionArtifactStore(project_root)
    for run in store.list_runs():
        if run.paper_run_id == paper_run_id:
            return run
    raise FileNotFoundError(f"no paper run found for paper_run_id={paper_run_id}")


def _find_shadow_run_by_id(*, project_root: Path, shadow_run_id: str) -> ShadowSessionRunRecord:
    store = ShadowArtifactStore(project_root)
    for run in store.list_runs():
        if run.shadow_run_id == shadow_run_id:
            return run
    raise FileNotFoundError(f"no shadow run found for shadow_run_id={shadow_run_id}")


def _load_paper_source(
    *, project_root: Path, run: PaperExecutionRunRecord
) -> LoadedExecutionSource:
    paper_store = ExecutionArtifactStore(project_root)
    if run.status != PaperRunStatus.SUCCESS:
        raise ValueError(f"paper run {run.paper_run_id} is not successful")
    orders_frame = paper_store.load_paper_orders(
        trade_date=run.trade_date,
        account_id=run.account_id,
        basket_id=run.basket_id,
        paper_run_id=run.paper_run_id,
    )
    trades_frame = paper_store.load_paper_trades(
        trade_date=run.trade_date,
        account_id=run.account_id,
        basket_id=run.basket_id,
        paper_run_id=run.paper_run_id,
    )
    orders: list[NormalizedExecutionOrder] = []
    for order_row in orders_frame.to_dict(orient="records"):
        fills = _paper_fills_for_order(
            trades_frame=trades_frame,
            order_id=str(order_row["order_id"]),
        )
        session_end_status = _paper_session_end_status(
            status=str(order_row["status"]),
            status_reason=order_row.get("status_reason"),
            fill_count=len(fills),
        )
        orders.append(
            NormalizedExecutionOrder(
                order_id=str(order_row["order_id"]),
                instrument_key=str(order_row["instrument_key"]),
                symbol=str(order_row["symbol"]),
                exchange=str(order_row["exchange"]),
                side=str(order_row["side"]),
                requested_quantity=int(order_row["quantity"]),
                reference_price=Decimal(str(order_row["reference_price"])),
                previous_close=_decimal_or_none(order_row.get("previous_close")),
                estimated_cost_total=Decimal(str(order_row["estimated_cost"])),
                session_end_status=session_end_status,
                replay_mode="paper_one_shot",
                fill_model_name=str(run.fill_model_name),
                time_in_force=None,
                fills=fills,
                partial_fill_count=_partial_fill_count_from_fills(
                    fill_count=len(fills),
                    filled_quantity=sum(item.quantity for item in fills),
                    requested_quantity=int(order_row["quantity"]),
                    session_end_status=session_end_status,
                ),
            )
        )
    source = NormalizedExecutionSource(
        source_type=ExecutionSourceType.PAPER_RUN,
        source_run_id=str(run.paper_run_id),
        trade_date=run.trade_date,
        account_id=str(run.account_id),
        basket_id=str(run.basket_id),
        execution_task_id=str(run.execution_task_id),
        strategy_run_id=str(run.strategy_run_id),
        prediction_run_id=str(run.source_prediction_run_id),
        source_qlib_export_run_id=run.source_qlib_export_run_id,
        source_standard_build_run_id=run.source_standard_build_run_id,
        replay_mode="paper_one_shot",
        fill_model_name=str(run.fill_model_name),
        time_in_force=None,
        orders=orders,
    )
    return LoadedExecutionSource(
        source=source,
        source_run_ids=[source.source_run_id],
        source_execution_task_ids=[source.execution_task_id],
        source_strategy_run_ids=[source.strategy_run_id],
        source_prediction_run_ids=[source.prediction_run_id],
        source_qlib_export_run_ids=_compact_ids([source.source_qlib_export_run_id]),
        source_standard_build_run_ids=_compact_ids([source.source_standard_build_run_id]),
    )


def _load_shadow_source(
    *, project_root: Path, run: ShadowSessionRunRecord
) -> LoadedExecutionSource:
    shadow_store = ShadowArtifactStore(project_root)
    paper_store = ExecutionArtifactStore(project_root)
    if run.status != ShadowRunStatus.SUCCESS:
        raise ValueError(f"shadow run {run.shadow_run_id} is not successful")
    if run.paper_run_id is None:
        raise ValueError(f"shadow run {run.shadow_run_id} is missing paper_run_id")
    orders_frame = paper_store.load_paper_orders(
        trade_date=run.trade_date,
        account_id=run.account_id,
        basket_id=run.basket_id,
        paper_run_id=run.paper_run_id,
    )
    trades_frame = paper_store.load_paper_trades(
        trade_date=run.trade_date,
        account_id=run.account_id,
        basket_id=run.basket_id,
        paper_run_id=run.paper_run_id,
    )
    order_events = shadow_store.load_order_events(
        trade_date=run.trade_date,
        account_id=run.account_id,
        basket_id=run.basket_id,
        shadow_run_id=run.shadow_run_id,
    )
    fill_events = shadow_store.load_fill_events(
        trade_date=run.trade_date,
        account_id=run.account_id,
        basket_id=run.basket_id,
        shadow_run_id=run.shadow_run_id,
    )
    fill_records = fill_events.to_dict(orient="records")
    orders: list[NormalizedExecutionOrder] = []
    for order_row in orders_frame.to_dict(orient="records"):
        order_id = str(order_row["order_id"])
        fills = _paper_fills_for_order(trades_frame=trades_frame, order_id=order_id)
        final_event = _final_shadow_order_event(order_events=order_events, order_id=order_id)
        fill_count = len([item for item in fill_records if str(item["order_id"]) == order_id])
        session_end_status = _shadow_session_end_status(
            state_after=str(final_event["state_after"]) if final_event is not None else None,
            fill_count=fill_count,
        )
        orders.append(
            NormalizedExecutionOrder(
                order_id=order_id,
                instrument_key=str(order_row["instrument_key"]),
                symbol=str(order_row["symbol"]),
                exchange=str(order_row["exchange"]),
                side=str(order_row["side"]),
                requested_quantity=int(order_row["quantity"]),
                reference_price=Decimal(str(order_row["reference_price"])),
                previous_close=_decimal_or_none(order_row.get("previous_close")),
                estimated_cost_total=Decimal(str(order_row["estimated_cost"])),
                session_end_status=session_end_status,
                replay_mode=str(run.market_replay_mode),
                fill_model_name=str(run.fill_model_name),
                time_in_force=run.time_in_force,
                fills=fills,
                partial_fill_count=_partial_fill_count_from_fills(
                    fill_count=fill_count,
                    filled_quantity=sum(item.quantity for item in fills),
                    requested_quantity=int(order_row["quantity"]),
                    session_end_status=session_end_status,
                ),
            )
        )
    source = NormalizedExecutionSource(
        source_type=ExecutionSourceType.SHADOW_RUN,
        source_run_id=str(run.shadow_run_id),
        trade_date=run.trade_date,
        account_id=str(run.account_id),
        basket_id=str(run.basket_id),
        execution_task_id=str(run.execution_task_id),
        strategy_run_id=str(run.strategy_run_id),
        prediction_run_id=str(run.source_prediction_run_id),
        source_qlib_export_run_id=run.source_qlib_export_run_id,
        source_standard_build_run_id=run.source_standard_build_run_id,
        replay_mode=str(run.market_replay_mode),
        fill_model_name=str(run.fill_model_name),
        time_in_force=run.time_in_force,
        orders=orders,
    )
    return LoadedExecutionSource(
        source=source,
        source_run_ids=[source.source_run_id, str(run.paper_run_id)],
        source_execution_task_ids=[source.execution_task_id],
        source_strategy_run_ids=[source.strategy_run_id],
        source_prediction_run_ids=[source.prediction_run_id],
        source_qlib_export_run_ids=_compact_ids([source.source_qlib_export_run_id]),
        source_standard_build_run_ids=_compact_ids([source.source_standard_build_run_id]),
    )


def _paper_fills_for_order(*, trades_frame: Any, order_id: str) -> list[NormalizedExecutionFill]:
    fills: list[NormalizedExecutionFill] = []
    records = [item for item in trades_frame.to_dict(orient="records") if str(item["order_id"]) == order_id]
    for row in sorted(records, key=lambda item: item["fill_bar_dt"]):
        fills.append(
            NormalizedExecutionFill(
                order_id=order_id,
                fill_dt=row["fill_bar_dt"],
                quantity=int(row["quantity"]),
                price=Decimal(str(row["price"])),
                notional=Decimal(str(row["notional"])),
                realized_cost_total=Decimal(str(row["cost_breakdown_json"]["total"])),
            )
        )
    return fills


def _paper_session_end_status(
    *, status: str, status_reason: object | None, fill_count: int
) -> SessionEndStatus:
    if status == PaperOrderStatus.FILLED.value:
        return SessionEndStatus.FILLED
    if status == PaperOrderStatus.PARTIALLY_FILLED.value:
        if status_reason == "expired_ioc_remaining":
            return SessionEndStatus.EXPIRED_IOC_REMAINING
        return SessionEndStatus.PARTIALLY_FILLED_THEN_EXPIRED
    if status == PaperOrderStatus.REJECTED.value:
        return SessionEndStatus.REJECTED_VALIDATION
    if status_reason == "expired_ioc_remaining":
        return SessionEndStatus.EXPIRED_IOC_REMAINING
    if status_reason == "expired_end_of_session":
        return SessionEndStatus.EXPIRED_END_OF_SESSION
    if fill_count > 0:
        return SessionEndStatus.PARTIALLY_FILLED_THEN_EXPIRED
    return SessionEndStatus.UNFILLED


def _shadow_session_end_status(*, state_after: str | None, fill_count: int) -> SessionEndStatus:
    if state_after == ShadowOrderState.FILLED.value:
        return SessionEndStatus.FILLED
    if state_after == ShadowOrderState.EXPIRED_IOC_REMAINING.value:
        return SessionEndStatus.EXPIRED_IOC_REMAINING
    if state_after == ShadowOrderState.EXPIRED_END_OF_SESSION.value:
        if fill_count > 0:
            return SessionEndStatus.PARTIALLY_FILLED_THEN_EXPIRED
        return SessionEndStatus.EXPIRED_END_OF_SESSION
    if state_after == ShadowOrderState.REJECTED_VALIDATION.value:
        return SessionEndStatus.REJECTED_VALIDATION
    if fill_count > 0:
        return SessionEndStatus.PARTIALLY_FILLED_THEN_EXPIRED
    return SessionEndStatus.UNFILLED


def _final_shadow_order_event(*, order_events: Any, order_id: str) -> dict[str, object] | None:
    rows = [item for item in order_events.to_dict(orient="records") if str(item["order_id"]) == order_id]
    if not rows:
        return None
    rows.sort(key=lambda item: (item["event_dt"], item["created_at"]))
    return {str(key): value for key, value in rows[-1].items()}


def _partial_fill_count_from_fills(
    *,
    fill_count: int,
    filled_quantity: int,
    requested_quantity: int,
    session_end_status: SessionEndStatus,
) -> int:
    if fill_count <= 0:
        return 0
    if filled_quantity < requested_quantity:
        return fill_count
    if session_end_status == SessionEndStatus.PARTIALLY_FILLED_THEN_EXPIRED:
        return fill_count
    return max(fill_count - 1, 0)


def _decimal_or_none(value: object | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _compact_ids(values: list[str | None]) -> list[str]:
    results: list[str] = []
    for value in values:
        if value is None:
            continue
        if value not in results:
            results.append(value)
    return results
