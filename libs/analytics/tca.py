"""Single-run execution analytics and TCA for M11."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from libs.analytics.artifacts import ExecutionAnalyticsArtifactStore
from libs.analytics.config import default_execution_analytics_config
from libs.analytics.loaders import LoadedExecutionSource, select_execution_source
from libs.analytics.normalize import (
    compute_avg_fill_price,
    compute_fill_rate,
    compute_implementation_shortfall,
    quantize_money,
)
from libs.analytics.schemas import (
    ExecutionAnalyticsRunRecord,
    ExecutionAnalyticsStatus,
    ExecutionTcaRowRecord,
    ExecutionTcaSummaryRecord,
    JsonScalar,
    SessionEndStatus,
)
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash


def run_execution_tca(
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
    config = default_execution_analytics_config()
    config_hash = stable_hash(config.model_dump(mode="json"))
    loaded = select_execution_source(
        project_root=project_root,
        paper_run_id=paper_run_id,
        shadow_run_id=shadow_run_id,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        latest=latest,
    )
    source = loaded.source
    analytics_run_id = "analytics_" + stable_hash(
        {
            "source_run_ids": loaded.source_run_ids,
            "source_type": source.source_type,
            "analytics_config_hash": config_hash,
        }
    )[:12]
    store = ExecutionAnalyticsArtifactStore(project_root)
    if store.has_analytics_run(
        trade_date=source.trade_date,
        account_id=source.account_id,
        basket_id=source.basket_id,
        analytics_run_id=analytics_run_id,
    ):
        existing = store.load_analytics_run(
            trade_date=source.trade_date,
            account_id=source.account_id,
            basket_id=source.basket_id,
            analytics_run_id=analytics_run_id,
        )
        if existing.status == ExecutionAnalyticsStatus.SUCCESS and not force:
            manifest = store.load_analytics_manifest(
                trade_date=source.trade_date,
                account_id=source.account_id,
                basket_id=source.basket_id,
                analytics_run_id=analytics_run_id,
            )
            summary = store.load_analytics_summary(
                trade_date=source.trade_date,
                account_id=source.account_id,
                basket_id=source.basket_id,
                analytics_run_id=analytics_run_id,
            )
            return {
                "analytics_run_id": analytics_run_id,
                "source_type": source.source_type,
                "source_run_ids": loaded.source_run_ids,
                "row_count": manifest.row_count,
                "summary_path": manifest.summary_file_path,
                "status": existing.status.value,
                "gross_fill_rate": summary.gross_fill_rate,
                "reused": True,
            }
        store.clear_analytics_run(
            trade_date=source.trade_date,
            account_id=source.account_id,
            basket_id=source.basket_id,
            analytics_run_id=analytics_run_id,
        )
    created_at = ensure_cn_aware(datetime.now())
    run = ExecutionAnalyticsRunRecord(
        analytics_run_id=analytics_run_id,
        trade_date=source.trade_date,
        account_id=source.account_id,
        basket_id=source.basket_id,
        source_type=source.source_type,
        source_run_ids=loaded.source_run_ids,
        source_execution_task_id=source.execution_task_id,
        source_strategy_run_id=source.strategy_run_id,
        source_prediction_run_id=source.prediction_run_id,
        analytics_config_hash=config_hash,
        status=ExecutionAnalyticsStatus.CREATED,
        created_at=created_at,
        source_qlib_export_run_id=source.source_qlib_export_run_id,
        source_standard_build_run_id=source.source_standard_build_run_id,
    )
    store.save_analytics_run(run)
    try:
        rows, summary = build_execution_tca_rows(
            loaded=loaded,
            analytics_run_id=analytics_run_id,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": ExecutionAnalyticsStatus.SUCCESS})
        manifest = store.save_analytics_success(run=success_run, rows=rows, summary=summary)
    except Exception as exc:
        failed_run = run.model_copy(update={"status": ExecutionAnalyticsStatus.FAILED})
        store.save_failed_analytics_run(failed_run, error_message=str(exc))
        raise
    return {
        "analytics_run_id": analytics_run_id,
        "source_type": source.source_type,
        "source_run_ids": loaded.source_run_ids,
        "row_count": len(rows),
        "summary_path": manifest.summary_file_path,
        "status": success_run.status.value,
        "gross_fill_rate": summary.gross_fill_rate,
        "reused": False,
    }


def build_execution_tca_rows(
    *,
    loaded: LoadedExecutionSource,
    analytics_run_id: str,
    created_at: datetime,
) -> tuple[list[ExecutionTcaRowRecord], ExecutionTcaSummaryRecord]:
    rows: list[ExecutionTcaRowRecord] = []
    total_requested_notional = Decimal("0")
    total_filled_notional = Decimal("0")
    total_estimated_cost = Decimal("0")
    total_realized_cost = Decimal("0")
    total_implementation_shortfall = Decimal("0")
    filled_order_count = 0
    partially_filled_order_count = 0
    expired_order_count = 0
    rejected_order_count = 0
    fill_rates: list[float] = []
    for order in loaded.source.orders:
        filled_quantity = sum(item.quantity for item in order.fills)
        remaining_quantity = max(order.requested_quantity - filled_quantity, 0)
        filled_notional = quantize_money(sum((item.notional for item in order.fills), Decimal("0")))
        realized_cost_total = quantize_money(
            sum((item.realized_cost_total for item in order.fills), Decimal("0"))
        )
        avg_fill_price = compute_avg_fill_price(
            filled_notional=filled_notional,
            filled_quantity=filled_quantity,
        )
        implementation_shortfall = compute_implementation_shortfall(
            side=order.side,
            reference_price=order.reference_price,
            avg_fill_price=avg_fill_price,
            filled_quantity=filled_quantity,
            realized_cost_total=realized_cost_total,
        )
        fill_rate = compute_fill_rate(
            requested_quantity=order.requested_quantity,
            filled_quantity=filled_quantity,
        )
        planned_notional = quantize_money(order.reference_price * Decimal(order.requested_quantity))
        first_fill_dt = min((item.fill_dt for item in order.fills), default=None)
        last_fill_dt = max((item.fill_dt for item in order.fills), default=None)
        row = ExecutionTcaRowRecord(
            analytics_run_id=analytics_run_id,
            instrument_key=order.instrument_key,
            symbol=order.symbol,
            exchange=order.exchange,
            side=order.side,
            requested_quantity=order.requested_quantity,
            filled_quantity=filled_quantity,
            remaining_quantity=remaining_quantity,
            fill_rate=fill_rate,
            partial_fill_count=order.partial_fill_count,
            avg_fill_price=avg_fill_price,
            reference_price=order.reference_price,
            previous_close=order.previous_close,
            planned_notional=planned_notional,
            filled_notional=filled_notional,
            estimated_cost_total=quantize_money(order.estimated_cost_total),
            realized_cost_total=realized_cost_total,
            implementation_shortfall=implementation_shortfall,
            first_fill_dt=first_fill_dt,
            last_fill_dt=last_fill_dt,
            session_end_status=order.session_end_status,
            replay_mode=order.replay_mode,
            fill_model_name=order.fill_model_name,
            time_in_force=order.time_in_force,
            created_at=created_at,
        )
        rows.append(row)
        total_requested_notional += planned_notional
        total_filled_notional += filled_notional
        total_estimated_cost += quantize_money(order.estimated_cost_total)
        total_realized_cost += realized_cost_total
        total_implementation_shortfall += implementation_shortfall
        fill_rates.append(fill_rate)
        if order.session_end_status == SessionEndStatus.FILLED:
            filled_order_count += 1
        if filled_quantity > 0 and filled_quantity < order.requested_quantity:
            partially_filled_order_count += 1
        if order.session_end_status in {
            SessionEndStatus.EXPIRED_END_OF_SESSION,
            SessionEndStatus.EXPIRED_IOC_REMAINING,
            SessionEndStatus.PARTIALLY_FILLED_THEN_EXPIRED,
        }:
            expired_order_count += 1
        if order.session_end_status == SessionEndStatus.REJECTED_VALIDATION:
            rejected_order_count += 1
    order_count = len(rows)
    gross_fill_rate = 0.0
    if total_requested_notional > Decimal("0"):
        gross_fill_rate = round(float(total_filled_notional / total_requested_notional), 4)
    avg_fill_rate = round(sum(fill_rates) / len(fill_rates), 4) if fill_rates else 0.0
    summary = ExecutionTcaSummaryRecord(
        analytics_run_id=analytics_run_id,
        order_count=order_count,
        filled_order_count=filled_order_count,
        partially_filled_order_count=partially_filled_order_count,
        expired_order_count=expired_order_count,
        rejected_order_count=rejected_order_count,
        total_requested_notional=quantize_money(total_requested_notional),
        total_filled_notional=quantize_money(total_filled_notional),
        gross_fill_rate=gross_fill_rate,
        avg_fill_rate=avg_fill_rate,
        total_estimated_cost=quantize_money(total_estimated_cost),
        total_realized_cost=quantize_money(total_realized_cost),
        total_implementation_shortfall=quantize_money(total_implementation_shortfall),
        summary_json={
            "source_run_ids": cast_json_list(loaded.source_run_ids),
            "source_type": loaded.source.source_type,
            "replay_mode": loaded.source.replay_mode,
            "fill_model_name": loaded.source.fill_model_name,
            "time_in_force": loaded.source.time_in_force,
            "implementation_shortfall_mode": "executed_notional_plus_cost",
            "no_fill_shortfall_behavior": "zero",
        },
        created_at=created_at,
    )
    return rows, summary


def cast_json_list(values: list[str]) -> list[JsonScalar]:
    return list(values)
