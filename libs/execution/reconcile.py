"""End-of-run reconciliation helpers for M7 paper execution."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from libs.execution.ledger import LedgerPosition, PaperLedger
from libs.execution.schemas import (
    PaperAccountSnapshotRecord,
    PaperOrderRecord,
    PaperOrderStatus,
    PaperPositionSnapshotRecord,
    PaperReconcileReportRecord,
    PaperTradeRecord,
)
from libs.schemas.master_data import Instrument


def build_account_snapshot(
    *,
    paper_run_id: str,
    strategy_run_id: str,
    execution_task_id: str,
    account_id: str,
    trade_date: date,
    created_at: datetime,
    ledger: PaperLedger,
    market_value_end: Decimal,
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> PaperAccountSnapshotRecord:
    return PaperAccountSnapshotRecord(
        paper_run_id=paper_run_id,
        strategy_run_id=strategy_run_id,
        execution_task_id=execution_task_id,
        account_id=account_id,
        trade_date=trade_date,
        cash_start=ledger.cash_start,
        cash_end=ledger.available_cash,
        fees_total=ledger.fees_total.quantize(Decimal("0.01")),
        realized_pnl=ledger.realized_pnl.quantize(Decimal("0.01")),
        market_value_end=market_value_end.quantize(Decimal("0.01")),
        net_liquidation_end=(ledger.available_cash + market_value_end).quantize(Decimal("0.01")),
        created_at=created_at,
        source_prediction_run_id=source_prediction_run_id,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )


def build_position_snapshots(
    *,
    paper_run_id: str,
    strategy_run_id: str,
    execution_task_id: str,
    account_id: str,
    trade_date: date,
    created_at: datetime,
    positions: dict[str, LedgerPosition],
    instruments: dict[str, Instrument],
    market_prices: dict[str, Decimal],
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> list[PaperPositionSnapshotRecord]:
    results: list[PaperPositionSnapshotRecord] = []
    for instrument_key in sorted(positions):
        position = positions[instrument_key]
        if position.quantity <= 0:
            continue
        instrument = instruments[instrument_key]
        market_price = market_prices.get(instrument_key, position.avg_price)
        market_value = market_price * Decimal(position.quantity)
        unrealized_pnl = (market_price - position.avg_price) * Decimal(position.quantity)
        results.append(
            PaperPositionSnapshotRecord(
                paper_run_id=paper_run_id,
                strategy_run_id=strategy_run_id,
                execution_task_id=execution_task_id,
                account_id=account_id,
                trade_date=trade_date,
                instrument_key=instrument_key,
                symbol=instrument.symbol,
                exchange=instrument.exchange.value,
                quantity=position.quantity,
                sellable_quantity=position.sellable_quantity,
                avg_price=position.avg_price.quantize(Decimal("0.0001")),
                market_value=market_value.quantize(Decimal("0.01")),
                unrealized_pnl=unrealized_pnl.quantize(Decimal("0.01")),
                created_at=created_at,
                source_prediction_run_id=source_prediction_run_id,
                source_qlib_export_run_id=source_qlib_export_run_id,
                source_standard_build_run_id=source_standard_build_run_id,
            )
        )
    return results


def build_reconcile_report(
    *,
    paper_run_id: str,
    strategy_run_id: str,
    execution_task_id: str,
    account_id: str,
    basket_id: str,
    trade_date: date,
    created_at: datetime,
    orders: list[PaperOrderRecord],
    trades: list[PaperTradeRecord],
    source_prediction_run_id: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
) -> PaperReconcileReportRecord:
    planned_notional = sum(
        (Decimal(order.quantity) * order.reference_price for order in orders),
        Decimal("0"),
    ).quantize(Decimal("0.01"))
    estimated_cost_total = sum((order.estimated_cost for order in orders), Decimal("0")).quantize(
        Decimal("0.01")
    )
    filled_notional = sum((trade.notional for trade in trades), Decimal("0")).quantize(
        Decimal("0.01")
    )
    realized_cost_total = sum(
        (Decimal(str(trade.cost_breakdown_json["total"])) for trade in trades),
        Decimal("0"),
    ).quantize(Decimal("0.01"))
    filled_order_ids = {trade.order_id for trade in trades}
    filled_order_id_list: list[str | int | float | bool | None] = []
    filled_order_id_list.extend(sorted(filled_order_ids))
    filled_order_count = len(filled_order_ids)
    rejected_order_count = len(
        [item for item in orders if item.status == PaperOrderStatus.REJECTED]
    )
    unfilled_order_count = len(
        [item for item in orders if item.status == PaperOrderStatus.UNFILLED]
    )
    return PaperReconcileReportRecord(
        paper_run_id=paper_run_id,
        strategy_run_id=strategy_run_id,
        execution_task_id=execution_task_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        planned_order_count=len(orders),
        filled_order_count=filled_order_count,
        rejected_order_count=rejected_order_count,
        unfilled_order_count=unfilled_order_count,
        planned_notional=planned_notional,
        filled_notional=filled_notional,
        estimated_cost_total=estimated_cost_total,
        realized_cost_total=realized_cost_total,
        summary_json={
            "filled_trade_count": len(trades),
            "filled_order_ids": filled_order_id_list,
            "filled_ratio": round(filled_order_count / len(orders), 4) if orders else 0.0,
        },
        created_at=created_at,
        source_prediction_run_id=source_prediction_run_id,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
    )
