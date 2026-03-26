"""Dry-run rebalance planner for M6."""

from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import ROUND_FLOOR, Decimal
from pathlib import Path
from typing import Any

from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.config import load_rebalance_planner_config
from libs.planning.pretrade import (
    build_order_request,
    evaluate_pretrade,
    planning_datetime,
    reference_price_from_snapshot,
    select_cost_profile,
)
from libs.planning.schemas import (
    ApprovedTargetWeightManifest,
    ExecutionTaskRecord,
    ExecutionTaskStatus,
    OrderIntentPreviewRecord,
    ValidationStatus,
)
from libs.rules_engine.calendar import load_calendars
from libs.rules_engine.market_rules import RulesRepository
from libs.schemas.master_data import Instrument
from libs.schemas.trading import AccountSnapshot, MarketSnapshot, OrderSide, PositionSnapshot
from scripts.load_master_data import BootstrapPayload, load_bootstrap

DEFAULT_REBALANCE_CONFIG = Path("configs/planning/rebalance_planner.yaml")


def plan_rebalance(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    config_path: Path = DEFAULT_REBALANCE_CONFIG,
    strategy_run_id: str | None = None,
    force: bool = False,
) -> dict[str, object]:
    config = load_rebalance_planner_config(project_root / config_path)
    payload = load_bootstrap(project_root / "data" / "master" / "bootstrap")
    catalog = InstrumentCatalog(payload)
    planning_store = PlanningArtifactStore(project_root)
    target_manifest = _resolve_target_weight_manifest(
        planning_store,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        strategy_run_id=strategy_run_id,
    )
    account_snapshot = load_account_snapshot(
        project_root / "data" / "bootstrap" / "execution_sample" / "account_demo.json"
    )
    positions = load_position_snapshots(
        project_root / "data" / "bootstrap" / "execution_sample" / "positions_demo.json"
    )
    market_snapshots = load_market_snapshots(
        project_root
        / "data"
        / "bootstrap"
        / "execution_sample"
        / f"market_snapshot_{trade_date.isoformat()}.json"
    )
    planner_config_hash = stable_hash(config.model_dump(mode="json"))
    execution_task_id = "task_" + stable_hash(
        {
            "target_weight_hash": target_manifest.file_hash,
            "planner_config_hash": planner_config_hash,
            "account_snapshot_hash": _snapshot_hash(account_snapshot),
            "positions_hash": _snapshot_hash(positions),
            "market_snapshot_hash": _snapshot_hash(market_snapshots),
        }
    )[:12]
    if planning_store.has_execution_task(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_id,
    ) and not force:
        manifest = planning_store.load_execution_task_manifest(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        )
        return {
            "execution_task_id": manifest.execution_task_id,
            "strategy_run_id": manifest.strategy_run_id,
            "row_count": manifest.preview_row_count,
            "file_path": manifest.file_path,
            "preview_file_path": manifest.preview_file_path,
            "reused": True,
        }
    if force:
        planning_store.clear_execution_task(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        )

    rules_repo = RulesRepository(
        payload.market_rules,
        load_calendars(project_root / "data" / "master" / "bootstrap" / "trading_calendar.json"),
    )
    target_frame = planning_store.load_target_weights(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        strategy_run_id=target_manifest.strategy_run_id,
    )
    created_at = ensure_cn_aware(datetime.now())
    previews = _build_order_intents(
        trade_date=trade_date,
        created_at=created_at,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_id,
        target_manifest=target_manifest,
        target_frame=target_frame,
        account_snapshot=account_snapshot,
        positions=positions,
        market_snapshots=market_snapshots,
        catalog=catalog,
        payload=payload,
        rules_repo=rules_repo,
        reference_price_field=config.reference_price_field,
        planner_time=config.planning_time,
        broker=config.broker,
    )
    task = ExecutionTaskRecord(
        execution_task_id=execution_task_id,
        strategy_run_id=target_manifest.strategy_run_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        exec_style=config.exec_style,
        status=ExecutionTaskStatus.PLANNED,
        created_at=created_at,
        source_target_weight_hash=target_manifest.file_hash,
        planner_config_hash=planner_config_hash,
        plan_only=config.plan_only,
        summary_json={
            "preview_count": len(previews),
            "accepted_count": len(
                [item for item in previews if item.validation_status == ValidationStatus.ACCEPTED]
            ),
            "rejected_count": len(
                [item for item in previews if item.validation_status == ValidationStatus.REJECTED]
            ),
            "reference_price_field": config.reference_price_field,
            "cash_policy": config.cash_policy,
        },
        source_qlib_export_run_id=target_manifest.source_qlib_export_run_id,
        source_standard_build_run_id=target_manifest.source_standard_build_run_id,
    )
    manifest = planning_store.save_execution_task(task=task, previews=previews)
    return {
        "execution_task_id": execution_task_id,
        "strategy_run_id": target_manifest.strategy_run_id,
        "row_count": len(previews),
        "file_path": manifest.file_path,
        "preview_file_path": manifest.preview_file_path,
        "reused": False,
    }


def load_account_snapshot(path: Path) -> AccountSnapshot:
    return AccountSnapshot.model_validate(json.loads(path.read_text(encoding="utf-8")))


def load_position_snapshots(path: Path) -> dict[str, PositionSnapshot]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(item["instrument_key"]): PositionSnapshot.model_validate(item)
        for item in payload["positions"]
    }


def load_market_snapshots(path: Path) -> dict[str, MarketSnapshot]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(item["instrument_key"]): MarketSnapshot.model_validate(item)
        for item in payload["market_snapshots"]
    }


def _resolve_target_weight_manifest(
    store: PlanningArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    strategy_run_id: str | None,
) -> ApprovedTargetWeightManifest:
    if strategy_run_id is not None:
        return store.load_target_weight_manifest(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            strategy_run_id=strategy_run_id,
        )
    for manifest in store.list_target_weight_manifests():
        if (
            manifest.trade_date == trade_date
            and manifest.account_id == account_id
            and manifest.basket_id == basket_id
        ):
            return manifest
    raise FileNotFoundError(
        f"no approved_target_weight artifact found for trade_date={trade_date.isoformat()} "
        f"account_id={account_id} basket_id={basket_id}"
    )


def _snapshot_hash(payload: object) -> str:
    if isinstance(payload, dict):
        return stable_hash(
            {
                key: value.model_dump(mode="json") if hasattr(value, "model_dump") else value
                for key, value in payload.items()
            }
        )
    if hasattr(payload, "model_dump"):
        return stable_hash(payload.model_dump(mode="json"))
    return stable_hash({"payload": payload})


def _build_order_intents(
    *,
    trade_date: date,
    created_at: datetime,
    account_id: str,
    basket_id: str,
    execution_task_id: str,
    target_manifest: ApprovedTargetWeightManifest,
    target_frame: Any,
    account_snapshot: AccountSnapshot,
    positions: dict[str, PositionSnapshot],
    market_snapshots: dict[str, MarketSnapshot],
    catalog: InstrumentCatalog,
    payload: BootstrapPayload,
    rules_repo: RulesRepository,
    reference_price_field: str,
    planner_time: time,
    broker: str,
) -> list[OrderIntentPreviewRecord]:
    order_ts = planning_datetime(trade_date, planner_time)
    target_weights = {
        str(row["instrument_key"]): Decimal(str(row["target_weight"]))
        for row in target_frame.to_dict(orient="records")
    }
    ranks = {
        str(row["instrument_key"]): int(row["rank"])
        for row in target_frame.to_dict(orient="records")
    }
    portfolio_value = _portfolio_value(
        account_snapshot,
        positions,
        market_snapshots,
        reference_price_field=reference_price_field,
    )
    available_cash = account_snapshot.available_cash
    previews: list[OrderIntentPreviewRecord] = []
    for instrument_key in sorted(set(target_weights).union(positions)):
        resolved = catalog.resolve(instrument_key=instrument_key)
        instrument = resolved.instrument
        position = positions.get(
            instrument_key,
            PositionSnapshot(
                instrument_key=instrument_key,
                total_quantity=0,
                sellable_quantity=0,
            ),
        )
        market = market_snapshots.get(instrument_key)
        target_weight = target_weights.get(instrument_key, Decimal("0"))
        session_tag = _session_tag(rules_repo, order_ts, instrument)
        if target_weight > 0 and (
            market is None
            or market.previous_close is None
            or reference_price_from_snapshot(market, reference_price_field) is None
        ):
            previews.append(
                _rejected_preview(
                    execution_task_id=execution_task_id,
                    strategy_run_id=target_manifest.strategy_run_id,
                    account_id=account_id,
                    basket_id=basket_id,
                    trade_date=trade_date,
                    instrument=instrument,
                    position=position,
                    target_quantity=0,
                    delta_quantity=0,
                    market=market,
                    created_at=created_at,
                    source_target_weight_hash=target_manifest.file_hash,
                    source_qlib_export_run_id=target_manifest.source_qlib_export_run_id,
                    source_standard_build_run_id=target_manifest.source_standard_build_run_id,
                    reason=(
                        "market_snapshot_missing"
                        if market is None
                        else "previous_close_missing"
                    ),
                    session_tag=session_tag,
                )
            )
            continue
        target_quantity = _target_quantity(
            portfolio_value=portfolio_value,
            target_weight=target_weight,
            instrument=instrument,
            market=market,
            reference_price_field=reference_price_field,
        )
        delta_quantity = target_quantity - position.total_quantity
        if delta_quantity == 0:
            continue
        if delta_quantity < 0:
            split_quantities = _split_sell_quantities(-delta_quantity, instrument, position.sellable_quantity)
            if split_quantities is None:
                previews.append(
                    _rejected_preview(
                        execution_task_id=execution_task_id,
                        strategy_run_id=target_manifest.strategy_run_id,
                        account_id=account_id,
                        basket_id=basket_id,
                        trade_date=trade_date,
                        instrument=instrument,
                        position=position,
                        target_quantity=target_quantity,
                        delta_quantity=delta_quantity,
                        market=market,
                        created_at=created_at,
                        source_target_weight_hash=target_manifest.file_hash,
                        source_qlib_export_run_id=target_manifest.source_qlib_export_run_id,
                        source_standard_build_run_id=target_manifest.source_standard_build_run_id,
                        reason="sell_quantity_exceeds_sellable",
                        session_tag=session_tag,
                    )
                )
                continue
            remaining_sellable = position.sellable_quantity
            for sell_quantity in split_quantities:
                preview, available_cash = _planned_preview(
                    execution_task_id=execution_task_id,
                    strategy_run_id=target_manifest.strategy_run_id,
                    account_id=account_id,
                    basket_id=basket_id,
                    trade_date=trade_date,
                    instrument=instrument,
                    position=position,
                    validation_position=PositionSnapshot(
                        instrument_key=instrument.instrument_key,
                        total_quantity=sell_quantity,
                        sellable_quantity=min(remaining_sellable, sell_quantity),
                    ),
                    market=market,
                    quantity=sell_quantity,
                    side=OrderSide.SELL,
                    created_at=created_at,
                    available_cash=available_cash,
                    source_target_weight_hash=target_manifest.file_hash,
                    source_qlib_export_run_id=target_manifest.source_qlib_export_run_id,
                    source_standard_build_run_id=target_manifest.source_standard_build_run_id,
                    payload=payload,
                    rules_repo=rules_repo,
                    order_ts=order_ts,
                    session_tag=session_tag,
                    target_quantity=target_quantity,
                    broker=broker,
                    reference_price_field=reference_price_field,
                )
                previews.append(preview)
                if preview.validation_status == ValidationStatus.ACCEPTED:
                    remaining_sellable = max(0, remaining_sellable - sell_quantity)
        else:
            preview, available_cash = _planned_preview(
                execution_task_id=execution_task_id,
                strategy_run_id=target_manifest.strategy_run_id,
                account_id=account_id,
                basket_id=basket_id,
                trade_date=trade_date,
                instrument=instrument,
                position=position,
                validation_position=position,
                market=market,
                quantity=delta_quantity,
                side=OrderSide.BUY,
                created_at=created_at,
                available_cash=available_cash,
                source_target_weight_hash=target_manifest.file_hash,
                source_qlib_export_run_id=target_manifest.source_qlib_export_run_id,
                source_standard_build_run_id=target_manifest.source_standard_build_run_id,
                payload=payload,
                rules_repo=rules_repo,
                order_ts=order_ts,
                session_tag=session_tag,
                target_quantity=target_quantity,
                broker=broker,
                reference_price_field=reference_price_field,
            )
            previews.append(preview)
    previews.sort(
        key=lambda item: (
            item.validation_status != ValidationStatus.ACCEPTED,
            ranks.get(item.instrument_key, 999),
            item.instrument_key,
            abs(item.delta_quantity),
        )
    )
    return previews


def _portfolio_value(
    account_snapshot: AccountSnapshot,
    positions: dict[str, PositionSnapshot],
    market_snapshots: dict[str, MarketSnapshot],
    *,
    reference_price_field: str,
) -> Decimal:
    if account_snapshot.nav is not None:
        return account_snapshot.nav
    market_value = Decimal("0")
    for instrument_key, position in positions.items():
        market = market_snapshots.get(instrument_key)
        if market is None:
            continue
        reference_price = reference_price_from_snapshot(market, reference_price_field)
        if reference_price is None:
            continue
        market_value += Decimal(position.total_quantity) * reference_price
    return account_snapshot.available_cash + market_value


def _target_quantity(
    *,
    portfolio_value: Decimal,
    target_weight: Decimal,
    instrument: Instrument,
    market: MarketSnapshot | None,
    reference_price_field: str,
) -> int:
    if target_weight <= 0 or market is None:
        return 0
    reference_price = reference_price_from_snapshot(market, reference_price_field)
    if reference_price is None or reference_price <= 0:
        return 0
    raw_quantity = (
        portfolio_value * target_weight / reference_price
    ).quantize(Decimal("1"), rounding=ROUND_FLOOR)
    raw_quantity_int = int(raw_quantity)
    if raw_quantity_int <= 0:
        return 0
    return raw_quantity_int // instrument.min_buy_lot * instrument.min_buy_lot


def _split_sell_quantities(
    quantity: int,
    instrument: Instrument,
    sellable_quantity: int,
) -> list[int] | None:
    if quantity > sellable_quantity:
        return None
    if not instrument.odd_lot_sell_only:
        return [quantity]
    lot_part = quantity // instrument.min_buy_lot * instrument.min_buy_lot
    odd_part = quantity % instrument.min_buy_lot
    return [part for part in (lot_part, odd_part) if part > 0]


def _planned_preview(
    *,
    execution_task_id: str,
    strategy_run_id: str,
    account_id: str,
    basket_id: str,
    trade_date: date,
    instrument: Instrument,
    position: PositionSnapshot,
    validation_position: PositionSnapshot,
    market: MarketSnapshot | None,
    quantity: int,
    side: OrderSide,
    created_at: datetime,
    available_cash: Decimal,
    source_target_weight_hash: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
    payload: BootstrapPayload,
    rules_repo: RulesRepository,
    order_ts: datetime,
    session_tag: str,
    target_quantity: int,
    broker: str,
    reference_price_field: str,
) -> tuple[OrderIntentPreviewRecord, Decimal]:
    delta_quantity = quantity if side == OrderSide.BUY else -quantity
    if market is None:
        return (
            _rejected_preview(
                execution_task_id=execution_task_id,
                strategy_run_id=strategy_run_id,
                account_id=account_id,
                basket_id=basket_id,
                trade_date=trade_date,
                instrument=instrument,
                position=position,
                target_quantity=target_quantity,
                delta_quantity=delta_quantity,
                market=None,
                created_at=created_at,
                source_target_weight_hash=source_target_weight_hash,
                source_qlib_export_run_id=source_qlib_export_run_id,
                source_standard_build_run_id=source_standard_build_run_id,
                reason="market_snapshot_missing",
                session_tag=session_tag,
            ),
            available_cash,
        )
    if market.previous_close is None:
        return (
            _rejected_preview(
                execution_task_id=execution_task_id,
                strategy_run_id=strategy_run_id,
                account_id=account_id,
                basket_id=basket_id,
                trade_date=trade_date,
                instrument=instrument,
                position=position,
                target_quantity=target_quantity,
                delta_quantity=delta_quantity,
                market=market,
                created_at=created_at,
                source_target_weight_hash=source_target_weight_hash,
                source_qlib_export_run_id=source_qlib_export_run_id,
                source_standard_build_run_id=source_standard_build_run_id,
                reason="previous_close_missing",
                session_tag=session_tag,
            ),
            available_cash,
        )
    reference_price = reference_price_from_snapshot(market, reference_price_field)
    if reference_price is None:
        return (
            _rejected_preview(
                execution_task_id=execution_task_id,
                strategy_run_id=strategy_run_id,
                account_id=account_id,
                basket_id=basket_id,
                trade_date=trade_date,
                instrument=instrument,
                position=position,
                target_quantity=target_quantity,
                delta_quantity=delta_quantity,
                market=market,
                created_at=created_at,
                source_target_weight_hash=source_target_weight_hash,
                source_qlib_export_run_id=source_qlib_export_run_id,
                source_standard_build_run_id=source_standard_build_run_id,
                reason="reference_price_missing",
                session_tag=session_tag,
            ),
            available_cash,
        )
    order = build_order_request(
        account_id=account_id,
        strategy_run_id=strategy_run_id,
        instrument=instrument,
        side=side,
        quantity=quantity,
        price=reference_price,
        order_ts=order_ts,
        reference=f"{strategy_run_id}:{instrument.symbol}:{side.value.lower()}",
    )
    cost_profile = select_cost_profile(
        payload,
        trade_date=trade_date,
        instrument=instrument,
        broker=broker,
    )
    accepted, reasons, cost = evaluate_pretrade(
        order=order,
        account_snapshot=AccountSnapshot(account_id=account_id, available_cash=available_cash),
        position_snapshot=validation_position,
        market_snapshot=market,
        instrument=instrument,
        rules_repo=rules_repo,
        cost_profile=cost_profile,
    )
    next_cash = available_cash
    if accepted:
        if side == OrderSide.BUY:
            next_cash = available_cash - (cost.notional + cost.total)
        else:
            next_cash = available_cash + (cost.notional - cost.total)
    preview = OrderIntentPreviewRecord(
        execution_task_id=execution_task_id,
        strategy_run_id=strategy_run_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        instrument_key=instrument.instrument_key,
        symbol=instrument.symbol,
        exchange=instrument.exchange.value,
        side=side.value,
        current_quantity=position.total_quantity,
        sellable_quantity=position.sellable_quantity,
        target_quantity=target_quantity,
        delta_quantity=delta_quantity,
        reference_price=reference_price,
        previous_close=market.previous_close,
        estimated_notional=cost.notional,
        estimated_cost=cost.total,
        validation_status=ValidationStatus.ACCEPTED if accepted else ValidationStatus.REJECTED,
        validation_reason=";".join(reasons) if reasons else None,
        session_tag=session_tag,
        created_at=created_at,
        source_target_weight_hash=source_target_weight_hash,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
        estimated_cost_breakdown={
            "commission": float(cost.commission),
            "stamp_duty": float(cost.stamp_duty),
            "handling_fee": float(cost.handling_fee),
            "transfer_fee": float(cost.transfer_fee),
            "reg_fee": float(cost.reg_fee),
            "total": float(cost.total),
        },
    )
    return preview, next_cash


def _rejected_preview(
    *,
    execution_task_id: str,
    strategy_run_id: str,
    account_id: str,
    basket_id: str,
    trade_date: date,
    instrument: Instrument,
    position: PositionSnapshot,
    target_quantity: int,
    delta_quantity: int,
    market: MarketSnapshot | None,
    created_at: datetime,
    source_target_weight_hash: str,
    source_qlib_export_run_id: str | None,
    source_standard_build_run_id: str | None,
    reason: str,
    session_tag: str,
) -> OrderIntentPreviewRecord:
    return OrderIntentPreviewRecord(
        execution_task_id=execution_task_id,
        strategy_run_id=strategy_run_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        instrument_key=instrument.instrument_key,
        symbol=instrument.symbol,
        exchange=instrument.exchange.value,
        side=OrderSide.BUY.value if delta_quantity > 0 else OrderSide.SELL.value,
        current_quantity=position.total_quantity,
        sellable_quantity=position.sellable_quantity,
        target_quantity=target_quantity,
        delta_quantity=delta_quantity,
        reference_price=market.previous_close if market is not None else None,
        previous_close=market.previous_close if market is not None else None,
        estimated_notional=Decimal("0"),
        estimated_cost=Decimal("0"),
        validation_status=ValidationStatus.REJECTED,
        validation_reason=reason,
        session_tag=session_tag,
        created_at=created_at,
        source_target_weight_hash=source_target_weight_hash,
        source_qlib_export_run_id=source_qlib_export_run_id,
        source_standard_build_run_id=source_standard_build_run_id,
        estimated_cost_breakdown=None,
    )


def _session_tag(rules_repo: RulesRepository, order_ts: datetime, instrument: Instrument) -> str:
    phase = rules_repo.get_trading_phase(order_ts, instrument).value
    if phase in {"OPEN_CALL", "CLOSE_CALL"}:
        return "auction"
    if phase == "CONTINUOUS_AM":
        return "am"
    if phase == "CONTINUOUS_PM":
        return "pm"
    if phase == "AFTER_HOURS_FIXED":
        return "after_hours"
    return "closed"
