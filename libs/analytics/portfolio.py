"""Single-run portfolio / risk analytics for M12."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.analytics.portfolio_config import default_portfolio_analytics_config
from libs.analytics.portfolio_loaders import LoadedPortfolioSource, select_portfolio_source
from libs.analytics.portfolio_normalize import (
    compute_hhi_concentration,
    compute_realized_turnover,
    compute_top_concentration,
    compute_tracking_error_proxy,
    quantize_weight,
    safe_float_ratio,
    safe_weight_ratio,
)
from libs.analytics.portfolio_schemas import (
    PortfolioAnalyticsRunRecord,
    PortfolioAnalyticsStatus,
    PortfolioGroupRowRecord,
    PortfolioPositionRowRecord,
    PortfolioSourceType,
    PortfolioSummaryRecord,
)
from libs.analytics.schemas import ExecutionTcaRowRecord
from libs.common.time import ensure_cn_aware
from libs.execution.schemas import PaperPositionSnapshotRecord
from libs.marketdata.raw_store import stable_hash
from libs.planning.schemas import ApprovedTargetWeightRecord, OrderIntentPreviewRecord

_ZERO = Decimal("0")


@dataclass(frozen=True)
class _AggregatedTcaRow:
    instrument_key: str
    symbol: str
    exchange: str
    requested_quantity: int
    filled_quantity: int
    planned_notional: Decimal
    filled_notional: Decimal
    fill_rate: float
    partial_fill_count: int
    realized_cost_total: Decimal
    avg_fill_price: Decimal | None
    reference_price: Decimal
    previous_close: Decimal | None
    session_end_status: str | None
    replay_mode: str | None
    fill_model_name: str | None
    time_in_force: str | None


def run_portfolio_analytics(
    *,
    project_root: Path,
    paper_run_id: str | None = None,
    shadow_run_id: str | None = None,
    trade_date: date | None = None,
    account_id: str | None = None,
    basket_id: str | None = None,
    latest: bool = False,
    force: bool = False,
) -> dict[str, object]:
    config = default_portfolio_analytics_config()
    config_hash = stable_hash(config.model_dump(mode="json"))
    loaded = select_portfolio_source(
        project_root=project_root,
        paper_run_id=paper_run_id,
        shadow_run_id=shadow_run_id,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        latest=latest,
    )
    portfolio_analytics_run_id = build_portfolio_analytics_run_id(
        source_run_ids=loaded.source_run_ids,
        source_type=loaded.source_type,
        analytics_config_hash=config_hash,
    )
    store = PortfolioAnalyticsArtifactStore(project_root)
    if store.has_portfolio_run(
        trade_date=loaded.trade_date,
        account_id=loaded.account_id,
        basket_id=loaded.basket_id,
        portfolio_analytics_run_id=portfolio_analytics_run_id,
    ):
        existing = store.load_portfolio_run(
            trade_date=loaded.trade_date,
            account_id=loaded.account_id,
            basket_id=loaded.basket_id,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
        )
        if existing.status == PortfolioAnalyticsStatus.SUCCESS and not force:
            manifest = store.load_portfolio_manifest(
                trade_date=loaded.trade_date,
                account_id=loaded.account_id,
                basket_id=loaded.basket_id,
                portfolio_analytics_run_id=portfolio_analytics_run_id,
            )
            summary = store.load_portfolio_summary(
                trade_date=loaded.trade_date,
                account_id=loaded.account_id,
                basket_id=loaded.basket_id,
                portfolio_analytics_run_id=portfolio_analytics_run_id,
            )
            return {
                "portfolio_analytics_run_id": portfolio_analytics_run_id,
                "source_type": loaded.source_type.value,
                "source_run_ids": loaded.source_run_ids,
                "position_row_count": manifest.position_row_count,
                "summary_path": manifest.summary_file_path,
                "status": existing.status.value,
                "fill_rate_gross": summary.fill_rate_gross,
                "reused": True,
            }
        store.clear_portfolio_run(
            trade_date=loaded.trade_date,
            account_id=loaded.account_id,
            basket_id=loaded.basket_id,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
        )
    created_at = ensure_cn_aware(datetime.now())
    run = PortfolioAnalyticsRunRecord(
        portfolio_analytics_run_id=portfolio_analytics_run_id,
        trade_date=loaded.trade_date,
        account_id=loaded.account_id,
        basket_id=loaded.basket_id,
        source_type=loaded.source_type,
        source_run_ids=loaded.source_run_ids,
        source_execution_task_id=loaded.source_execution_task_id,
        source_strategy_run_id=loaded.source_strategy_run_id,
        source_prediction_run_id=loaded.source_prediction_run_id,
        analytics_config_hash=config_hash,
        status=PortfolioAnalyticsStatus.CREATED,
        created_at=created_at,
        source_qlib_export_run_id=loaded.source_qlib_export_run_id,
        source_standard_build_run_id=loaded.source_standard_build_run_id,
    )
    store.save_portfolio_run(run)
    try:
        positions, groups, summary = build_portfolio_analytics(
            loaded=loaded,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": PortfolioAnalyticsStatus.SUCCESS})
        manifest = store.save_portfolio_success(
            run=success_run,
            positions=positions,
            groups=groups,
            summary=summary,
        )
    except Exception as exc:
        failed_run = run.model_copy(update={"status": PortfolioAnalyticsStatus.FAILED})
        store.save_failed_portfolio_run(failed_run, error_message=str(exc))
        raise
    return {
        "portfolio_analytics_run_id": portfolio_analytics_run_id,
        "source_type": loaded.source_type.value,
        "source_run_ids": loaded.source_run_ids,
        "position_row_count": len(positions),
        "summary_path": manifest.summary_file_path,
        "status": success_run.status.value,
        "fill_rate_gross": summary.fill_rate_gross,
        "reused": False,
    }


def build_portfolio_analytics_run_id(
    *,
    source_run_ids: list[str],
    source_type: PortfolioSourceType,
    analytics_config_hash: str,
) -> str:
    return "portfolio_" + stable_hash(
        {
            "source_run_ids": source_run_ids,
            "source_type": source_type.value,
            "analytics_config_hash": analytics_config_hash,
        }
    )[:12]


def build_portfolio_analytics(
    *,
    loaded: LoadedPortfolioSource,
    portfolio_analytics_run_id: str,
    created_at: datetime,
) -> tuple[list[PortfolioPositionRowRecord], list[PortfolioGroupRowRecord], PortfolioSummaryRecord]:
    target_by_instrument = {item.instrument_key: item for item in loaded.target_weights}
    intent_by_instrument = {item.instrument_key: item for item in loaded.order_intents}
    position_by_instrument = {item.instrument_key: item for item in loaded.position_snapshots}
    tca_by_instrument = _aggregate_tca_rows(loaded.tca_rows)

    net_liquidation_end = loaded.account_snapshot.net_liquidation_end
    net_liquidation_start = _estimate_net_liquidation_start(
        order_intents=loaded.order_intents,
        cash_start=loaded.account_snapshot.cash_start,
    )
    has_planning_previews = bool(loaded.order_intents)

    rows: list[PortfolioPositionRowRecord] = []
    planned_notional_total = Decimal("0")
    filled_notional_total = Decimal("0")
    total_weight_drift_l1 = Decimal("0")
    executed_weights: list[Decimal] = []
    group_totals: dict[tuple[str, str], dict[str, Decimal]] = defaultdict(
        lambda: {
            "target_weight_sum": Decimal("0"),
            "executed_weight_sum": Decimal("0"),
            "weight_drift_sum": Decimal("0"),
            "market_value_end": Decimal("0"),
        }
    )

    all_instruments = sorted(
        set(target_by_instrument) | set(intent_by_instrument) | set(position_by_instrument) | set(tca_by_instrument)
    )
    for instrument_key in all_instruments:
        target = target_by_instrument.get(instrument_key)
        intent = intent_by_instrument.get(instrument_key)
        position = position_by_instrument.get(instrument_key)
        tca = tca_by_instrument.get(instrument_key)
        market_value_end = position.market_value if position is not None else _ZERO
        executed_weight = safe_weight_ratio(market_value_end, net_liquidation_end)
        target_weight = target.target_weight if target is not None else _ZERO
        weight_drift = quantize_weight(abs(target_weight - executed_weight))
        planned_notional = _resolve_planned_notional(
            tca=tca,
            intent=intent,
            target=target,
            net_liquidation_start=net_liquidation_start,
            has_planning_previews=has_planning_previews,
        )
        filled_notional = tca.filled_notional if tca is not None else Decimal("0.00")
        fill_rate = tca.fill_rate if tca is not None else 0.0
        row = PortfolioPositionRowRecord(
            portfolio_analytics_run_id=portfolio_analytics_run_id,
            instrument_key=instrument_key,
            symbol=_resolve_symbol(
                instrument_key=instrument_key,
                position=position,
                intent=intent,
                tca=tca,
                loaded=loaded,
            ),
            exchange=_resolve_exchange(
                instrument_key=instrument_key,
                position=position,
                intent=intent,
                tca=tca,
                loaded=loaded,
            ),
            target_weight=quantize_weight(target_weight),
            executed_weight=executed_weight,
            weight_drift=weight_drift,
            target_rank=target.rank if target is not None else None,
            target_score=target.score if target is not None else None,
            quantity_end=position.quantity if position is not None else 0,
            sellable_quantity_end=position.sellable_quantity if position is not None else 0,
            avg_price_end=position.avg_price if position is not None else _ZERO,
            market_value_end=market_value_end.quantize(Decimal("0.01")),
            executed_price_reference=_resolve_executed_price_reference(
                position=position,
                tca=tca,
                intent=intent,
            ),
            realized_pnl=_resolve_position_realized_pnl(
                position=position,
                tca=tca,
                intent=intent,
            ),
            unrealized_pnl=(position.unrealized_pnl if position is not None else _ZERO).quantize(
                Decimal("0.01")
            ),
            planned_notional=planned_notional.quantize(Decimal("0.01")),
            filled_notional=filled_notional.quantize(Decimal("0.01")),
            fill_rate=fill_rate,
            session_end_status=tca.session_end_status if tca is not None else None,
            replay_mode=tca.replay_mode if tca is not None else loaded.replay_mode,
            fill_model_name=tca.fill_model_name if tca is not None else loaded.fill_model_name,
            time_in_force=tca.time_in_force if tca is not None else loaded.time_in_force,
            created_at=created_at,
        )
        rows.append(row)
        planned_notional_total += row.planned_notional
        filled_notional_total += row.filled_notional
        total_weight_drift_l1 += row.weight_drift
        if row.executed_weight > _ZERO:
            executed_weights.append(row.executed_weight)
        _accumulate_group_totals(group_totals=group_totals, row=row, loaded=loaded)

    groups = _build_group_rows(
        portfolio_analytics_run_id=portfolio_analytics_run_id,
        group_totals=group_totals,
        created_at=created_at,
    )
    target_weight_sum = sum((item.target_weight for item in loaded.target_weights), _ZERO)
    target_cash_weight = quantize_weight(max(_ZERO, Decimal("1") - target_weight_sum))
    executed_cash_weight = safe_weight_ratio(loaded.account_snapshot.cash_end, net_liquidation_end)
    gross_exposure_end = quantize_weight(sum((abs(weight) for weight in executed_weights), _ZERO))
    net_exposure_end = quantize_weight(sum(executed_weights, _ZERO))
    total_unrealized_pnl = sum((item.unrealized_pnl for item in loaded.position_snapshots), _ZERO)
    realized_turnover = compute_realized_turnover(
        filled_notional_total=filled_notional_total.quantize(Decimal("0.01")),
        net_liquidation_start=net_liquidation_start,
    )
    top1 = compute_top_concentration(executed_weights, 1)
    top3 = compute_top_concentration(executed_weights, 3)
    top5 = compute_top_concentration(executed_weights, 5)
    hhi = compute_hhi_concentration(executed_weights)
    total_weight_drift_l1 = quantize_weight(total_weight_drift_l1)
    tracking_error_proxy = compute_tracking_error_proxy(total_weight_drift_l1)
    summary = PortfolioSummaryRecord(
        portfolio_analytics_run_id=portfolio_analytics_run_id,
        holdings_count_target=len([item for item in loaded.target_weights if item.target_weight > _ZERO]),
        holdings_count_end=len([item for item in loaded.position_snapshots if item.quantity > 0]),
        target_cash_weight=target_cash_weight,
        executed_cash_weight=executed_cash_weight,
        gross_exposure_end=gross_exposure_end,
        net_exposure_end=net_exposure_end,
        realized_turnover=realized_turnover,
        filled_notional_total=filled_notional_total.quantize(Decimal("0.01")),
        planned_notional_total=planned_notional_total.quantize(Decimal("0.01")),
        fill_rate_gross=safe_float_ratio(filled_notional_total, planned_notional_total),
        top1_concentration=top1,
        top3_concentration=top3,
        top5_concentration=top5,
        hhi_concentration=hhi,
        total_realized_pnl=loaded.account_snapshot.realized_pnl.quantize(Decimal("0.01")),
        total_unrealized_pnl=total_unrealized_pnl.quantize(Decimal("0.01")),
        total_weight_drift_l1=total_weight_drift_l1,
        tracking_error_proxy=tracking_error_proxy,
        net_liquidation_start=net_liquidation_start.quantize(Decimal("0.01")),
        net_liquidation_end=net_liquidation_end.quantize(Decimal("0.01")),
        summary_json={
            "tca_source": loaded.tca_source,
            "planned_notional_source": (
                "planning_artifacts"
                if has_planning_previews
                else "target_weight_times_net_liquidation_start"
            ),
            "source_run_ids": list(loaded.source_run_ids),
            "replay_mode": loaded.replay_mode,
            "fill_model_name": loaded.fill_model_name,
            "time_in_force": loaded.time_in_force,
            "tracking_error_proxy_mode": "0.5_times_total_weight_drift_l1",
            "net_liquidation_start_mode": "cash_start_plus_preview_current_quantity_times_previous_close_or_reference_price",
        },
        created_at=created_at,
    )
    return rows, groups, summary


def _estimate_net_liquidation_start(
    *, order_intents: list[OrderIntentPreviewRecord], cash_start: Decimal
) -> Decimal:
    current_value = Decimal("0")
    seen: set[str] = set()
    for intent in order_intents:
        if intent.instrument_key in seen:
            continue
        seen.add(intent.instrument_key)
        start_price = intent.previous_close or intent.reference_price or Decimal("0")
        current_value += Decimal(intent.current_quantity) * start_price
    return (cash_start + current_value).quantize(Decimal("0.01"))


def _resolve_planned_notional(
    *,
    tca: _AggregatedTcaRow | None,
    intent: OrderIntentPreviewRecord | None,
    target: ApprovedTargetWeightRecord | None,
    net_liquidation_start: Decimal,
    has_planning_previews: bool,
) -> Decimal:
    if tca is not None and tca.planned_notional > _ZERO:
        return tca.planned_notional
    if intent is not None:
        if intent.estimated_notional > _ZERO:
            return intent.estimated_notional
        if intent.reference_price is not None:
            return (abs(Decimal(intent.delta_quantity)) * intent.reference_price).quantize(
                Decimal("0.01")
            )
    if not has_planning_previews and target is not None:
        return (target.target_weight * net_liquidation_start).quantize(Decimal("0.01"))
    return Decimal("0.00")


def _resolve_executed_price_reference(
    *,
    position: PaperPositionSnapshotRecord | None,
    tca: _AggregatedTcaRow | None,
    intent: OrderIntentPreviewRecord | None,
) -> Decimal | None:
    if position is not None and position.quantity > 0:
        return (position.market_value / Decimal(position.quantity)).quantize(Decimal("0.0001"))
    if tca is not None and tca.avg_fill_price is not None:
        return tca.avg_fill_price
    if intent is not None and intent.reference_price is not None:
        return intent.reference_price.quantize(Decimal("0.0001"))
    return None


def _resolve_position_realized_pnl(
    *,
    position: PaperPositionSnapshotRecord | None,
    tca: _AggregatedTcaRow | None,
    intent: OrderIntentPreviewRecord | None,
) -> Decimal:
    if (
        tca is None
        or intent is None
        or intent.side != "SELL"
        or tca.avg_fill_price is None
        or position is None
        or tca.filled_quantity <= 0
    ):
        return Decimal("0.00")
    return (
        (tca.avg_fill_price - position.avg_price) * Decimal(tca.filled_quantity)
        - tca.realized_cost_total
    ).quantize(Decimal("0.01"))


def _resolve_symbol(
    *,
    instrument_key: str,
    position: PaperPositionSnapshotRecord | None,
    intent: OrderIntentPreviewRecord | None,
    tca: _AggregatedTcaRow | None,
    loaded: LoadedPortfolioSource,
) -> str:
    if position is not None:
        return position.symbol
    if intent is not None:
        return intent.symbol
    if tca is not None:
        return tca.symbol
    instrument = loaded.instruments.get(instrument_key)
    return instrument.symbol if instrument is not None else "UNKNOWN"


def _resolve_exchange(
    *,
    instrument_key: str,
    position: PaperPositionSnapshotRecord | None,
    intent: OrderIntentPreviewRecord | None,
    tca: _AggregatedTcaRow | None,
    loaded: LoadedPortfolioSource,
) -> str:
    if position is not None:
        return position.exchange
    if intent is not None:
        return intent.exchange
    if tca is not None:
        return tca.exchange
    instrument = loaded.instruments.get(instrument_key)
    return instrument.exchange.value if instrument is not None else "UNKNOWN"


def _aggregate_tca_rows(rows: list[ExecutionTcaRowRecord]) -> dict[str, _AggregatedTcaRow]:
    grouped: dict[str, list[ExecutionTcaRowRecord]] = defaultdict(list)
    for row in rows:
        grouped[row.instrument_key].append(row)
    results: dict[str, _AggregatedTcaRow] = {}
    for instrument_key, grouped_rows in grouped.items():
        grouped_rows.sort(
            key=lambda item: (
                item.first_fill_dt or item.created_at,
                item.created_at,
                item.symbol,
            )
        )
        requested_quantity = sum(item.requested_quantity for item in grouped_rows)
        filled_quantity = sum(item.filled_quantity for item in grouped_rows)
        filled_notional = sum((item.filled_notional for item in grouped_rows), _ZERO)
        avg_fill_price = None
        if filled_quantity > 0:
            avg_fill_price = (filled_notional / Decimal(filled_quantity)).quantize(Decimal("0.0001"))
        representative = grouped_rows[0]
        results[instrument_key] = _AggregatedTcaRow(
            instrument_key=instrument_key,
            symbol=representative.symbol,
            exchange=representative.exchange,
            requested_quantity=requested_quantity,
            filled_quantity=filled_quantity,
            planned_notional=sum((item.planned_notional for item in grouped_rows), _ZERO).quantize(
                Decimal("0.01")
            ),
            filled_notional=filled_notional.quantize(Decimal("0.01")),
            fill_rate=round(filled_quantity / requested_quantity, 4) if requested_quantity > 0 else 0.0,
            partial_fill_count=sum(item.partial_fill_count for item in grouped_rows),
            realized_cost_total=sum(
                (item.realized_cost_total for item in grouped_rows), _ZERO
            ).quantize(Decimal("0.01")),
            avg_fill_price=avg_fill_price,
            reference_price=representative.reference_price,
            previous_close=representative.previous_close,
            session_end_status=_aggregate_session_end_status(grouped_rows),
            replay_mode=representative.replay_mode,
            fill_model_name=representative.fill_model_name,
            time_in_force=representative.time_in_force,
        )
    return results


def _aggregate_session_end_status(rows: list[ExecutionTcaRowRecord]) -> str | None:
    values = {item.session_end_status.value for item in rows}
    for candidate in (
        "partially_filled_then_expired",
        "expired_ioc_remaining",
        "expired_end_of_session",
        "rejected_validation",
        "filled",
        "unfilled",
    ):
        if candidate in values:
            return candidate
    return next(iter(values), None)


def _accumulate_group_totals(
    *,
    group_totals: dict[tuple[str, str], dict[str, Decimal]],
    row: PortfolioPositionRowRecord,
    loaded: LoadedPortfolioSource,
) -> None:
    instrument = loaded.instruments.get(row.instrument_key)
    groups = {
        ("instrument_type", instrument.instrument_type.value if instrument is not None else "unknown"),
        ("exchange", row.exchange),
        ("board", instrument.board.value if instrument is not None else "unknown"),
    }
    for group_key in groups:
        bucket = group_totals[group_key]
        bucket["target_weight_sum"] += row.target_weight
        bucket["executed_weight_sum"] += row.executed_weight
        bucket["weight_drift_sum"] += row.weight_drift
        bucket["market_value_end"] += row.market_value_end


def _build_group_rows(
    *,
    portfolio_analytics_run_id: str,
    group_totals: dict[tuple[str, str], dict[str, Decimal]],
    created_at: datetime,
) -> list[PortfolioGroupRowRecord]:
    rows: list[PortfolioGroupRowRecord] = []
    for (group_type, group_key), totals in sorted(group_totals.items()):
        rows.append(
            PortfolioGroupRowRecord(
                portfolio_analytics_run_id=portfolio_analytics_run_id,
                group_type=group_type,
                group_key=group_key,
                target_weight_sum=quantize_weight(totals["target_weight_sum"]),
                executed_weight_sum=quantize_weight(totals["executed_weight_sum"]),
                weight_drift_sum=quantize_weight(totals["weight_drift_sum"]),
                market_value_end=totals["market_value_end"].quantize(Decimal("0.01")),
                created_at=created_at,
            )
        )
    return rows
