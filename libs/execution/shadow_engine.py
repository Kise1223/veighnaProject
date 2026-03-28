"""Replay-driven session-aware shadow execution engine for M8."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from libs.common.time import ensure_cn_aware
from libs.execution import PaperLedger
from libs.execution.fill_model import load_bars_for_order, simulate_limit_fill_on_bar
from libs.execution.reconcile import (
    build_account_snapshot,
    build_position_snapshots,
    build_reconcile_report,
)
from libs.execution.schemas import (
    PaperAccountSnapshotRecord,
    PaperOrderRecord,
    PaperOrderStatus,
    PaperPositionSnapshotRecord,
    PaperReconcileReportRecord,
    PaperTradeRecord,
)
from libs.execution.shadow_clock import (
    collect_replay_datetimes,
    resolve_activation_dt,
    resolve_session_end_dt,
)
from libs.execution.shadow_schemas import (
    ShadowEventType,
    ShadowFillEventRecord,
    ShadowOrderState,
    ShadowOrderStateEventRecord,
    ShadowSessionConfig,
)
from libs.execution.shadow_state import (
    ShadowWorkingOrder,
    shadow_order_fifo_key,
    shadow_order_sort_key,
)
from libs.execution.tick_replay import (
    collect_tick_replay_events,
    last_tick_price,
    resolve_tick_liquidity,
    simulate_limit_fill_on_tick,
)
from libs.execution.validation import (
    validate_sellable_quantity,
    validate_static_order_inputs,
)
from libs.marketdata.raw_store import stable_hash
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.planning.schemas import OrderIntentPreviewRecord, ValidationStatus
from libs.rules_engine.market_rules import RulesRepository
from libs.schemas.master_data import Instrument
from libs.schemas.trading import AccountSnapshot, MarketSnapshot, OrderSide, PositionSnapshot
from scripts.load_master_data import BootstrapPayload


@dataclass
class ShadowEngineOutcome:
    order_events: list[ShadowOrderStateEventRecord]
    fill_events: list[ShadowFillEventRecord]
    paper_orders: list[PaperOrderRecord]
    paper_trades: list[PaperTradeRecord]
    account_snapshot: PaperAccountSnapshotRecord
    position_snapshots: list[PaperPositionSnapshotRecord]
    paper_reconcile_report: PaperReconcileReportRecord


@dataclass(frozen=True)
class ResolvedShadowOrderParameters:
    reference_price: Decimal
    limit_price: Decimal
    source_order_intent_hash: str


def run_shadow_engine(
    *,
    project_root: Path,
    trade_date: date,
    shadow_run_id: str,
    paper_run_id: str,
    execution_task_id: str,
    strategy_run_id: str,
    account_id: str,
    basket_id: str,
    previews: list[OrderIntentPreviewRecord],
    account_snapshot: AccountSnapshot,
    positions: dict[str, PositionSnapshot],
    market_snapshots: dict[str, MarketSnapshot],
    avg_price_by_instrument: dict[str, Decimal],
    catalog: InstrumentCatalog,
    payload: BootstrapPayload,
    rules_repo: RulesRepository,
    config: ShadowSessionConfig,
    created_at: datetime,
    tick_frames_by_instrument: dict[str, Any] | None,
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> ShadowEngineOutcome:
    instruments: dict[str, Instrument] = {}
    for instrument_key in sorted(positions):
        instruments[instrument_key] = catalog.resolve(instrument_key=instrument_key).instrument
    ledger = PaperLedger.from_snapshots(
        trade_date=trade_date,
        account_snapshot=account_snapshot,
        positions=positions,
        avg_price_by_instrument=avg_price_by_instrument,
        instruments=instruments,
        rules_repo=rules_repo,
        payload=payload,
        broker=config.broker,
    )
    order_events: list[ShadowOrderStateEventRecord] = []
    fill_events: list[ShadowFillEventRecord] = []
    paper_order_map: dict[str, PaperOrderRecord] = {}
    paper_trade_records: list[PaperTradeRecord] = []
    working_orders: list[ShadowWorkingOrder] = []

    ordered_previews = sorted(
        [item for item in previews if abs(item.delta_quantity) > 0],
        key=lambda item: (
            0 if item.side == OrderSide.SELL.value else 1,
            item.symbol,
            item.instrument_key,
        ),
    )
    for index, preview in enumerate(ordered_previews, start=1):
        previous_close = _resolve_previous_close(
            preview=preview,
            market_snapshots=market_snapshots,
        )
        order_id, paper_order = _initialize_shadow_order(
            shadow_run_id=shadow_run_id,
            paper_run_id=paper_run_id,
            execution_task_id=execution_task_id,
            strategy_run_id=strategy_run_id,
            account_id=account_id,
            basket_id=basket_id,
            preview=preview,
            catalog=catalog,
            rules_repo=rules_repo,
            ledger=ledger,
            created_at=created_at,
            order_index=index,
            config=config,
            source_prediction_run_id=source_prediction_run_id,
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
            instruments=instruments,
            resolved_previous_close=previous_close,
        )
        instrument = instruments.get(preview.instrument_key)
        preview_created_at = ensure_cn_aware(preview.created_at)
        if instrument is not None:
            preview_created_at = _normalize_preview_created_at(
                trade_date=trade_date,
                created_at=preview_created_at,
                instrument=instrument,
                rules_repo=rules_repo,
            )
        paper_order_map[order_id] = paper_order
        order_events.append(
            _make_order_event(
                shadow_run_id=shadow_run_id,
                paper_run_id=paper_run_id,
                execution_task_id=execution_task_id,
                strategy_run_id=strategy_run_id,
                order_id=order_id,
                instrument_key=preview.instrument_key,
                symbol=preview.symbol,
                exchange=preview.exchange,
                event_dt=preview_created_at,
                event_type=ShadowEventType.CREATED,
                state_before=None,
                state_after=ShadowOrderState.CREATED,
                quantity=abs(preview.delta_quantity),
                remaining_quantity=abs(preview.delta_quantity),
                reference_price=_required_decimal(preview.reference_price),
                limit_price=_required_decimal(paper_order.limit_price),
                reason=None,
                created_at=created_at,
                source_prediction_run_id=source_prediction_run_id,
                source_qlib_export_run_id=source_qlib_export_run_id,
                source_standard_build_run_id=source_standard_build_run_id,
            )
        )
        static_reason = (
            preview.validation_reason
            if preview.validation_status != ValidationStatus.ACCEPTED
            else validate_static_order_inputs(
                instrument=instrument,
                previous_close=previous_close,
            )
        )
        if static_reason is None and instrument is not None and preview.side == OrderSide.SELL.value:
            sellable_reason = validate_sellable_quantity(
                requested=abs(preview.delta_quantity),
                sellable_quantity=ledger.get_position(
                    instrument,
                    previous_close or Decimal("0"),
                ).sellable_quantity,
            )
            static_reason = sellable_reason
        if static_reason is not None or instrument is None or previous_close is None:
            reason = static_reason or "invalid_shadow_order"
            paper_order_map[order_id] = paper_order.model_copy(
                update={"status": PaperOrderStatus.REJECTED, "status_reason": reason}
            )
            order_events.append(
                _make_order_event(
                    shadow_run_id=shadow_run_id,
                    paper_run_id=paper_run_id,
                    execution_task_id=execution_task_id,
                    strategy_run_id=strategy_run_id,
                    order_id=order_id,
                    instrument_key=preview.instrument_key,
                    symbol=preview.symbol,
                    exchange=preview.exchange,
                    event_dt=preview_created_at,
                    event_type=ShadowEventType.REJECTED_VALIDATION,
                    state_before=ShadowOrderState.CREATED,
                    state_after=ShadowOrderState.REJECTED_VALIDATION,
                    quantity=abs(preview.delta_quantity),
                    remaining_quantity=abs(preview.delta_quantity),
                    reference_price=_required_decimal(preview.reference_price),
                    limit_price=_required_decimal(paper_order.limit_price),
                    reason=reason,
                    created_at=created_at,
                    source_prediction_run_id=source_prediction_run_id,
                    source_qlib_export_run_id=source_qlib_export_run_id,
                    source_standard_build_run_id=source_standard_build_run_id,
                )
            )
            continue
        activation_dt = resolve_activation_dt(
            created_at=preview_created_at,
            instrument=instrument,
            rules_repo=rules_repo,
        )
        if activation_dt is None:
            reason = "no_actionable_session"
            paper_order_map[order_id] = paper_order.model_copy(
                update={"status": PaperOrderStatus.REJECTED, "status_reason": reason}
            )
            order_events.append(
                _make_order_event(
                    shadow_run_id=shadow_run_id,
                    paper_run_id=paper_run_id,
                    execution_task_id=execution_task_id,
                    strategy_run_id=strategy_run_id,
                    order_id=order_id,
                    instrument_key=preview.instrument_key,
                    symbol=preview.symbol,
                    exchange=preview.exchange,
                    event_dt=preview_created_at,
                    event_type=ShadowEventType.REJECTED_VALIDATION,
                    state_before=ShadowOrderState.CREATED,
                    state_after=ShadowOrderState.REJECTED_VALIDATION,
                    quantity=abs(preview.delta_quantity),
                    remaining_quantity=abs(preview.delta_quantity),
                    reference_price=_required_decimal(preview.reference_price),
                    limit_price=_required_decimal(paper_order.limit_price),
                    reason=reason,
                    created_at=created_at,
                    source_prediction_run_id=source_prediction_run_id,
                    source_qlib_export_run_id=source_qlib_export_run_id,
                    source_standard_build_run_id=source_standard_build_run_id,
                )
            )
            continue
        expiry_dt = resolve_session_end_dt(
            trade_date=trade_date,
            instrument=instrument,
            rules_repo=rules_repo,
        )
        working_order = ShadowWorkingOrder(
            order_id=order_id,
            instrument=instrument,
            side=OrderSide(preview.side),
            quantity=abs(preview.delta_quantity),
            remaining_quantity=abs(preview.delta_quantity),
            filled_quantity=0,
            reference_price=_required_decimal(preview.reference_price),
            limit_price=_required_decimal(paper_order.limit_price),
            previous_close=previous_close,
            estimated_cost=preview.estimated_cost,
            cumulative_notional=Decimal("0"),
            activation_dt=activation_dt,
            expiry_dt=expiry_dt,
            state=ShadowOrderState.WORKING,
            status_reason=None,
            source_order_intent_hash=paper_order.source_order_intent_hash,
            created_at=preview_created_at,
            creation_seq=index,
            last_fill_dt=None,
            source_prediction_run_id=source_prediction_run_id,
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
        )
        working_orders.append(working_order)
        paper_order_map[order_id] = paper_order
        order_events.append(
            _make_order_event(
                shadow_run_id=shadow_run_id,
                paper_run_id=paper_run_id,
                execution_task_id=execution_task_id,
                strategy_run_id=strategy_run_id,
                order_id=order_id,
                instrument_key=preview.instrument_key,
                symbol=preview.symbol,
                exchange=preview.exchange,
                event_dt=activation_dt,
                event_type=ShadowEventType.WORKING,
                state_before=ShadowOrderState.CREATED,
                state_after=ShadowOrderState.WORKING,
                quantity=working_order.quantity,
                remaining_quantity=working_order.remaining_quantity,
                reference_price=working_order.reference_price,
                limit_price=working_order.limit_price,
                reason=None,
                created_at=created_at,
                source_prediction_run_id=source_prediction_run_id,
                source_qlib_export_run_id=source_qlib_export_run_id,
                source_standard_build_run_id=source_standard_build_run_id,
            )
        )

    bars_cache: dict[str, Any] = {}
    bar_lookup: dict[str, dict[datetime, dict[str, object]]] = {}
    tick_cache = tick_frames_by_instrument or {}
    if config.market_replay_mode == "ticks_l1":
        for replay_event in collect_tick_replay_events(tick_cache):
            replay_orders = [
                item
                for item in working_orders
                if _is_order_active(item)
                and replay_event.instrument_key == item.instrument.instrument_key
                and item.activation_dt <= replay_event.event_dt <= item.expiry_dt
                and rules_repo.is_match_phase(replay_event.event_dt, item.instrument)
            ]
            if not replay_orders:
                continue
            if config.tick_fill_model == "crossing_full_fill_v1":
                for order in sorted(replay_orders, key=shadow_order_sort_key):
                    fill_decision = simulate_limit_fill_on_tick(
                        side=order.side.value,
                        limit_price=order.limit_price,
                        tick_row=replay_event.row,
                        tick_price_fallback=config.tick_price_fallback,
                    )
                    if (
                        fill_decision is None
                        or fill_decision.fill_price is None
                        or fill_decision.fill_bar_dt is None
                    ):
                        continue
                    filled = _apply_shadow_fill(
                        shadow_run_id=shadow_run_id,
                        paper_run_id=paper_run_id,
                        execution_task_id=execution_task_id,
                        strategy_run_id=strategy_run_id,
                        order=order,
                        fill_dt=fill_decision.fill_bar_dt,
                        fill_price=fill_decision.fill_price,
                        fill_quantity=order.remaining_quantity,
                        ledger=ledger,
                        paper_order_map=paper_order_map,
                        fill_events=fill_events,
                        order_events=order_events,
                        paper_trade_records=paper_trade_records,
                        created_at=created_at,
                        source_prediction_run_id=source_prediction_run_id,
                        source_qlib_export_run_id=source_qlib_export_run_id,
                        source_standard_build_run_id=source_standard_build_run_id,
                    )
                    if (
                        config.time_in_force == "IOC"
                        and fill_decision.fill_bar_dt is not None
                        and order.remaining_quantity > 0
                    ):
                        _expire_shadow_order(
                            shadow_run_id=shadow_run_id,
                            paper_run_id=paper_run_id,
                            execution_task_id=execution_task_id,
                            strategy_run_id=strategy_run_id,
                            order=order,
                            event_dt=fill_decision.fill_bar_dt,
                            paper_order_map=paper_order_map,
                            order_events=order_events,
                            created_at=created_at,
                            reason="expired_ioc_remaining",
                            state_after=ShadowOrderState.EXPIRED_IOC_REMAINING,
                            source_prediction_run_id=source_prediction_run_id,
                            source_qlib_export_run_id=source_qlib_export_run_id,
                            source_standard_build_run_id=source_standard_build_run_id,
                        )
                    if not filled:
                        continue
            else:
                _process_tick_partial_fill_event(
                    shadow_run_id=shadow_run_id,
                    paper_run_id=paper_run_id,
                    execution_task_id=execution_task_id,
                    strategy_run_id=strategy_run_id,
                    replay_event=replay_event,
                    working_orders=replay_orders,
                    config=config,
                    ledger=ledger,
                    paper_order_map=paper_order_map,
                    fill_events=fill_events,
                    order_events=order_events,
                    paper_trade_records=paper_trade_records,
                    created_at=created_at,
                    source_prediction_run_id=source_prediction_run_id,
                    source_qlib_export_run_id=source_qlib_export_run_id,
                    source_standard_build_run_id=source_standard_build_run_id,
                )
    else:
        for order in working_orders:
            if order.instrument.instrument_key in bars_cache:
                continue
            frame = load_bars_for_order(
                project_root=project_root,
                trade_date=trade_date,
                exchange=order.instrument.exchange.value,
                symbol=order.instrument.symbol,
                source_standard_build_run_id=source_standard_build_run_id,
            )
            bars_cache[order.instrument.instrument_key] = frame
            bar_lookup[order.instrument.instrument_key] = {
                ensure_cn_aware(
                    datetime.fromisoformat(str(row["bar_dt"]))
                    if not isinstance(row["bar_dt"], datetime)
                    else row["bar_dt"]
                ): row
                for row in frame.to_dict(orient="records")
            }

        for replay_dt in collect_replay_datetimes(bars_cache):
            for order in sorted(
                [item for item in working_orders if _is_order_active(item)],
                key=shadow_order_sort_key,
            ):
                if replay_dt < order.activation_dt or replay_dt > order.expiry_dt:
                    continue
                if not rules_repo.is_match_phase(replay_dt, order.instrument):
                    continue
                row = bar_lookup.get(order.instrument.instrument_key, {}).get(replay_dt)
                if row is None:
                    continue
                fill_decision = simulate_limit_fill_on_bar(
                    side=order.side.value,
                    limit_price=order.limit_price,
                    bar_row=row,
                )
                if fill_decision is None or fill_decision.fill_price is None or fill_decision.fill_bar_dt is None:
                    continue
                _apply_shadow_fill(
                    shadow_run_id=shadow_run_id,
                    paper_run_id=paper_run_id,
                    execution_task_id=execution_task_id,
                    strategy_run_id=strategy_run_id,
                    order=order,
                    fill_dt=fill_decision.fill_bar_dt,
                    fill_price=fill_decision.fill_price,
                    fill_quantity=order.remaining_quantity,
                    ledger=ledger,
                    paper_order_map=paper_order_map,
                    fill_events=fill_events,
                    order_events=order_events,
                    paper_trade_records=paper_trade_records,
                    created_at=created_at,
                    source_prediction_run_id=source_prediction_run_id,
                    source_qlib_export_run_id=source_qlib_export_run_id,
                    source_standard_build_run_id=source_standard_build_run_id,
                )

    for order in sorted([item for item in working_orders if _is_order_active(item)], key=shadow_order_sort_key):
        _expire_shadow_order(
            shadow_run_id=shadow_run_id,
            paper_run_id=paper_run_id,
            execution_task_id=execution_task_id,
            strategy_run_id=strategy_run_id,
            order=order,
            event_dt=order.expiry_dt,
            paper_order_map=paper_order_map,
            order_events=order_events,
            created_at=created_at,
            reason="expired_end_of_session",
            state_after=ShadowOrderState.EXPIRED_END_OF_SESSION,
            source_prediction_run_id=source_prediction_run_id,
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
        )

    market_prices = _final_market_prices(
        bars_cache=bars_cache,
        tick_cache=tick_cache,
        positions=ledger.positions,
        market_snapshots=market_snapshots,
    )
    account_record, position_records, report = _finalize_paper_outputs(
        paper_run_id=paper_run_id,
        strategy_run_id=strategy_run_id,
        execution_task_id=execution_task_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        created_at=created_at,
        ledger=ledger,
        paper_orders=list(paper_order_map.values()),
        paper_trades=paper_trade_records,
        instruments=instruments,
        market_prices=market_prices,
        source_prediction_run_id=source_prediction_run_id,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )
    return ShadowEngineOutcome(
        order_events=order_events,
        fill_events=fill_events,
        paper_orders=list(paper_order_map.values()),
        paper_trades=paper_trade_records,
        account_snapshot=account_record,
        position_snapshots=position_records,
        paper_reconcile_report=report,
    )


def _initialize_shadow_order(
    *,
    shadow_run_id: str,
    paper_run_id: str,
    execution_task_id: str,
    strategy_run_id: str,
    account_id: str,
    basket_id: str,
    preview: OrderIntentPreviewRecord,
    catalog: InstrumentCatalog,
    rules_repo: RulesRepository,
    ledger: PaperLedger,
    created_at: datetime,
    order_index: int,
    config: ShadowSessionConfig,
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
    instruments: dict[str, Instrument],
    resolved_previous_close: Decimal | None,
) -> tuple[str, PaperOrderRecord]:
    try:
        instrument = catalog.resolve(instrument_key=preview.instrument_key).instrument
    except KeyError:
        instrument = None
    else:
        if instrument is not None:
            instruments.setdefault(preview.instrument_key, instrument)
    del rules_repo, ledger
    effective_params = _resolve_shadow_order_parameters(
        execution_task_id=execution_task_id,
        preview=preview,
        config=config,
        resolved_previous_close=resolved_previous_close,
    )
    order_id = "sorder_" + stable_hash(
        {
            "shadow_run_id": shadow_run_id,
            "order_index": order_index,
            "source_order_intent_hash": effective_params.source_order_intent_hash,
        }
    )[:12]
    exchange = preview.exchange if instrument is None else instrument.exchange.value
    symbol = preview.symbol if instrument is None else instrument.symbol
    paper_order = PaperOrderRecord(
        order_id=order_id,
        paper_run_id=paper_run_id,
        execution_task_id=execution_task_id,
        strategy_run_id=strategy_run_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=preview.trade_date,
        instrument_key=preview.instrument_key,
        symbol=symbol,
        exchange=exchange,
        side=preview.side,
        order_type="LIMIT",
        quantity=abs(preview.delta_quantity),
        limit_price=effective_params.limit_price,
        reference_price=effective_params.reference_price,
        previous_close=resolved_previous_close,
        source_order_intent_hash=effective_params.source_order_intent_hash,
        status=PaperOrderStatus.CREATED,
        created_at=created_at,
        estimated_cost=preview.estimated_cost,
        source_prediction_run_id=source_prediction_run_id,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )
    return order_id, paper_order


def _apply_shadow_fill(
    *,
    shadow_run_id: str,
    paper_run_id: str,
    execution_task_id: str,
    strategy_run_id: str,
    order: ShadowWorkingOrder,
    fill_dt: datetime,
    fill_price: Decimal,
    fill_quantity: int,
    ledger: PaperLedger,
    paper_order_map: dict[str, PaperOrderRecord],
    fill_events: list[ShadowFillEventRecord],
    order_events: list[ShadowOrderStateEventRecord],
    paper_trade_records: list[PaperTradeRecord],
    created_at: datetime,
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> bool:
    if fill_quantity <= 0:
        return False
    ledger_result = ledger.apply_fill(
        instrument=order.instrument,
        side=order.side,
        quantity=fill_quantity,
        price=fill_price,
        previous_close=order.previous_close,
    )
    if not ledger_result.accepted or ledger_result.cost is None:
        return False
    state_before = order.state
    order.filled_quantity += fill_quantity
    order.remaining_quantity -= fill_quantity
    order.cumulative_notional += ledger_result.cost.notional
    resolved_fill_dt = ensure_cn_aware(fill_dt)
    order.last_fill_dt = resolved_fill_dt
    is_complete = order.remaining_quantity == 0
    order.state = ShadowOrderState.FILLED if is_complete else ShadowOrderState.PARTIALLY_FILLED
    paper_order_map[order.order_id] = paper_order_map[order.order_id].model_copy(
        update={
            "status": (
                PaperOrderStatus.FILLED
                if is_complete
                else PaperOrderStatus.PARTIALLY_FILLED
            ),
            "status_reason": None,
        }
    )
    trade_id = "strade_" + stable_hash(
        {
            "shadow_run_id": shadow_run_id,
            "order_id": order.order_id,
            "fill_dt": resolved_fill_dt.isoformat(),
            "filled_quantity_before": order.filled_quantity - fill_quantity,
            "fill_quantity": fill_quantity,
        }
    )[:12]
    fill_events.append(
        ShadowFillEventRecord(
            shadow_run_id=shadow_run_id,
            paper_run_id=paper_run_id,
            execution_task_id=execution_task_id,
            strategy_run_id=strategy_run_id,
            order_id=order.order_id,
            trade_id=trade_id,
            instrument_key=order.instrument.instrument_key,
            symbol=order.instrument.symbol,
            exchange=order.instrument.exchange.value,
            side=order.side.value,
            fill_dt=resolved_fill_dt,
            price=fill_price,
            quantity=fill_quantity,
            notional=ledger_result.cost.notional,
            cost_breakdown_json=ledger_result.cost.model_dump(mode="json"),
            created_at=created_at,
            source_prediction_run_id=source_prediction_run_id,
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
        )
    )
    order_events.append(
        _make_order_event(
            shadow_run_id=shadow_run_id,
            paper_run_id=paper_run_id,
            execution_task_id=execution_task_id,
            strategy_run_id=strategy_run_id,
            order_id=order.order_id,
            instrument_key=order.instrument.instrument_key,
            symbol=order.instrument.symbol,
            exchange=order.instrument.exchange.value,
            event_dt=resolved_fill_dt,
            event_type=(
                ShadowEventType.FILLED
                if is_complete
                else ShadowEventType.PARTIALLY_FILLED
            ),
            state_before=state_before,
            state_after=order.state,
            quantity=order.quantity,
            remaining_quantity=order.remaining_quantity,
            reference_price=order.reference_price,
            limit_price=order.limit_price,
            reason=None,
            created_at=created_at,
            source_prediction_run_id=source_prediction_run_id,
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
        )
    )
    paper_trade_records.append(
        PaperTradeRecord(
            paper_run_id=paper_run_id,
            order_id=order.order_id,
            trade_id=trade_id,
            execution_task_id=execution_task_id,
            strategy_run_id=strategy_run_id,
            instrument_key=order.instrument.instrument_key,
            symbol=order.instrument.symbol,
            exchange=order.instrument.exchange.value,
            side=order.side.value,
            quantity=fill_quantity,
            price=fill_price,
            notional=ledger_result.cost.notional,
            cost_breakdown_json=ledger_result.cost.model_dump(mode="json"),
            fill_bar_dt=resolved_fill_dt,
            created_at=created_at,
            source_prediction_run_id=source_prediction_run_id,
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
        )
    )
    return True


def _process_tick_partial_fill_event(
    *,
    shadow_run_id: str,
    paper_run_id: str,
    execution_task_id: str,
    strategy_run_id: str,
    replay_event: Any,
    working_orders: list[ShadowWorkingOrder],
    config: ShadowSessionConfig,
    ledger: PaperLedger,
    paper_order_map: dict[str, PaperOrderRecord],
    fill_events: list[ShadowFillEventRecord],
    order_events: list[ShadowOrderStateEventRecord],
    paper_trade_records: list[PaperTradeRecord],
    created_at: datetime,
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> None:
    for side in _execution_sides(config.execution_order):
        side_orders = sorted(
            [item for item in working_orders if _is_order_active(item) and item.side.value == side],
            key=shadow_order_fifo_key,
        )
        if not side_orders:
            continue
        liquidity = resolve_tick_liquidity(
            side=side,
            tick_row=replay_event.row,
            tick_price_fallback=config.tick_price_fallback,
        )
        if liquidity is None:
            continue
        available_quantity = liquidity.available_quantity
        for order in side_orders:
            if not _tick_price_crosses(
                side=order.side.value,
                limit_price=order.limit_price,
                liquidity_price=liquidity.fill_price,
            ):
                continue
            fill_quantity = min(order.remaining_quantity, available_quantity)
            if fill_quantity > 0:
                filled = _apply_shadow_fill(
                    shadow_run_id=shadow_run_id,
                    paper_run_id=paper_run_id,
                    execution_task_id=execution_task_id,
                    strategy_run_id=strategy_run_id,
                    order=order,
                    fill_dt=liquidity.fill_dt,
                    fill_price=liquidity.fill_price,
                    fill_quantity=fill_quantity,
                    ledger=ledger,
                    paper_order_map=paper_order_map,
                    fill_events=fill_events,
                    order_events=order_events,
                    paper_trade_records=paper_trade_records,
                    created_at=created_at,
                    source_prediction_run_id=source_prediction_run_id,
                    source_qlib_export_run_id=source_qlib_export_run_id,
                    source_standard_build_run_id=source_standard_build_run_id,
                )
                if filled:
                    available_quantity -= fill_quantity
            if config.time_in_force == "IOC" and order.remaining_quantity > 0:
                _expire_shadow_order(
                    shadow_run_id=shadow_run_id,
                    paper_run_id=paper_run_id,
                    execution_task_id=execution_task_id,
                    strategy_run_id=strategy_run_id,
                    order=order,
                    event_dt=liquidity.fill_dt,
                    paper_order_map=paper_order_map,
                    order_events=order_events,
                    created_at=created_at,
                    reason="expired_ioc_remaining",
                    state_after=ShadowOrderState.EXPIRED_IOC_REMAINING,
                    source_prediction_run_id=source_prediction_run_id,
                    source_qlib_export_run_id=source_qlib_export_run_id,
                    source_standard_build_run_id=source_standard_build_run_id,
                )
            if available_quantity <= 0 and config.time_in_force != "IOC":
                break


def _expire_shadow_order(
    *,
    shadow_run_id: str,
    paper_run_id: str,
    execution_task_id: str,
    strategy_run_id: str,
    order: ShadowWorkingOrder,
    event_dt: datetime,
    paper_order_map: dict[str, PaperOrderRecord],
    order_events: list[ShadowOrderStateEventRecord],
    created_at: datetime,
    reason: str,
    state_after: ShadowOrderState,
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> None:
    state_before = order.state
    if not _is_order_active(order):
        return
    order.state = state_after
    order.status_reason = reason
    paper_order_map[order.order_id] = paper_order_map[order.order_id].model_copy(
        update={
            "status": (
                PaperOrderStatus.PARTIALLY_FILLED
                if order.filled_quantity > 0
                else PaperOrderStatus.UNFILLED
            ),
            "status_reason": reason,
        }
    )
    order_events.append(
        _make_order_event(
            shadow_run_id=shadow_run_id,
            paper_run_id=paper_run_id,
            execution_task_id=execution_task_id,
            strategy_run_id=strategy_run_id,
            order_id=order.order_id,
            instrument_key=order.instrument.instrument_key,
            symbol=order.instrument.symbol,
            exchange=order.instrument.exchange.value,
            event_dt=event_dt,
            event_type=(
                ShadowEventType.EXPIRED_IOC_REMAINING
                if state_after == ShadowOrderState.EXPIRED_IOC_REMAINING
                else ShadowEventType.EXPIRED_END_OF_SESSION
            ),
            state_before=state_before,
            state_after=state_after,
            quantity=order.quantity,
            remaining_quantity=order.remaining_quantity,
            reference_price=order.reference_price,
            limit_price=order.limit_price,
            reason=reason,
            created_at=created_at,
            source_prediction_run_id=source_prediction_run_id,
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
        )
    )


def _make_order_event(
    *,
    shadow_run_id: str,
    paper_run_id: str,
    execution_task_id: str,
    strategy_run_id: str,
    order_id: str,
    instrument_key: str,
    symbol: str,
    exchange: str,
    event_dt: datetime,
    event_type: ShadowEventType,
    state_before: ShadowOrderState | None,
    state_after: ShadowOrderState,
    quantity: int,
    remaining_quantity: int,
    reference_price: Decimal,
    limit_price: Decimal,
    reason: str | None,
    created_at: datetime,
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> ShadowOrderStateEventRecord:
    return ShadowOrderStateEventRecord(
        shadow_run_id=shadow_run_id,
        paper_run_id=paper_run_id,
        execution_task_id=execution_task_id,
        strategy_run_id=strategy_run_id,
        order_id=order_id,
        instrument_key=instrument_key,
        symbol=symbol,
        exchange=exchange,
        event_dt=ensure_cn_aware(event_dt),
        event_type=event_type,
        state_before=state_before,
        state_after=state_after,
        quantity=quantity,
        remaining_quantity=remaining_quantity,
        reference_price=reference_price,
        limit_price=limit_price,
        reason=reason,
        created_at=created_at,
        source_prediction_run_id=source_prediction_run_id,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )


def _final_market_prices(
    *,
    bars_cache: dict[str, Any],
    tick_cache: dict[str, Any],
    positions: dict[str, Any],
    market_snapshots: dict[str, MarketSnapshot],
) -> dict[str, Decimal]:
    market_prices: dict[str, Decimal] = {}
    for instrument_key, position in positions.items():
        frame = bars_cache.get(instrument_key)
        if frame is not None and not frame.empty:
            last_row = frame.sort_values("bar_dt").iloc[-1]
            market_prices[instrument_key] = Decimal(str(last_row["close"]))
            continue
        tick_frame = tick_cache.get(instrument_key)
        tick_price = last_tick_price(tick_frame)
        if tick_price is not None:
            market_prices[instrument_key] = tick_price
            continue
        snapshot = market_snapshots.get(instrument_key)
        if snapshot is not None:
            market_prices[instrument_key] = snapshot.last_price
            continue
        market_prices[instrument_key] = position.avg_price
    return market_prices


def _finalize_paper_outputs(
    *,
    paper_run_id: str,
    strategy_run_id: str,
    execution_task_id: str,
    account_id: str,
    basket_id: str,
    trade_date: date,
    created_at: datetime,
    ledger: PaperLedger,
    paper_orders: list[PaperOrderRecord],
    paper_trades: list[PaperTradeRecord],
    instruments: dict[str, Instrument],
    market_prices: dict[str, Decimal],
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> tuple[PaperAccountSnapshotRecord, list[PaperPositionSnapshotRecord], PaperReconcileReportRecord]:
    market_value_end = sum(
        (
            Decimal(position.quantity) * market_prices.get(instrument_key, position.avg_price)
            for instrument_key, position in ledger.positions.items()
            if position.quantity > 0
        ),
        Decimal("0"),
    )
    account_record = build_account_snapshot(
        paper_run_id=paper_run_id,
        strategy_run_id=strategy_run_id,
        execution_task_id=execution_task_id,
        account_id=account_id,
        trade_date=trade_date,
        created_at=created_at,
        ledger=ledger,
        market_value_end=market_value_end,
        source_prediction_run_id=source_prediction_run_id,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )
    position_records = build_position_snapshots(
        paper_run_id=paper_run_id,
        strategy_run_id=strategy_run_id,
        execution_task_id=execution_task_id,
        account_id=account_id,
        trade_date=trade_date,
        created_at=created_at,
        positions=ledger.positions,
        instruments=instruments,
        market_prices=market_prices,
        source_prediction_run_id=source_prediction_run_id,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )
    report = build_reconcile_report(
        paper_run_id=paper_run_id,
        strategy_run_id=strategy_run_id,
        execution_task_id=execution_task_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        created_at=created_at,
        orders=paper_orders,
        trades=paper_trades,
        source_prediction_run_id=source_prediction_run_id,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )
    return account_record, position_records, report


def _required_decimal(value: Decimal | None) -> Decimal:
    if value is None:
        raise ValueError("expected decimal value for shadow session")
    return value


def _is_order_active(order: ShadowWorkingOrder) -> bool:
    return order.state in {ShadowOrderState.WORKING, ShadowOrderState.PARTIALLY_FILLED}


def _execution_sides(execution_order: str) -> tuple[str, str]:
    if execution_order == "sell_then_buy":
        return ("SELL", "BUY")
    return ("BUY", "SELL")


def _tick_price_crosses(*, side: str, limit_price: Decimal, liquidity_price: Decimal) -> bool:
    if side == "BUY":
        return liquidity_price <= limit_price
    return liquidity_price >= limit_price


def _resolve_previous_close(
    *,
    preview: OrderIntentPreviewRecord,
    market_snapshots: dict[str, MarketSnapshot],
) -> Decimal | None:
    market_snapshot = market_snapshots.get(preview.instrument_key)
    if market_snapshot is not None and market_snapshot.previous_close is not None:
        return market_snapshot.previous_close
    return preview.previous_close


def _resolve_shadow_order_parameters(
    *,
    execution_task_id: str,
    preview: OrderIntentPreviewRecord,
    config: ShadowSessionConfig,
    resolved_previous_close: Decimal | None,
) -> ResolvedShadowOrderParameters:
    reference_price = _required_decimal(preview.reference_price)
    hash_previous_close: Decimal | None
    if config.limit_price_source == "previous_close" and resolved_previous_close is not None:
        limit_price = resolved_previous_close
        hash_previous_close = resolved_previous_close
    else:
        limit_price = reference_price
        hash_previous_close = preview.previous_close
    source_order_intent_hash = stable_hash(
        {
            "execution_task_id": execution_task_id,
            "instrument_key": preview.instrument_key,
            "side": preview.side,
            "quantity": abs(preview.delta_quantity),
            "reference_price": str(reference_price),
            "previous_close": str(hash_previous_close) if hash_previous_close is not None else None,
            "source_target_weight_hash": preview.source_target_weight_hash,
        }
    )
    return ResolvedShadowOrderParameters(
        reference_price=reference_price,
        limit_price=limit_price,
        source_order_intent_hash=source_order_intent_hash,
    )


def _normalize_preview_created_at(
    *,
    trade_date: date,
    created_at: datetime,
    instrument: Instrument,
    rules_repo: RulesRepository,
) -> datetime:
    normalized = ensure_cn_aware(created_at)
    session_starts = [
        session.start
        for session in rules_repo.get_sessions(trade_date, instrument)
        if session.order_accepting
    ]
    if not session_starts:
        return normalized
    first_actionable = datetime.combine(trade_date, min(session_starts), tzinfo=normalized.tzinfo)
    if normalized.date() != trade_date:
        return first_actionable
    if rules_repo.is_order_accepting(normalized, instrument):
        return normalized
    next_actionable = rules_repo.next_actionable_time(normalized, instrument)
    if next_actionable is None or next_actionable.date() != trade_date:
        return first_actionable
    return next_actionable
