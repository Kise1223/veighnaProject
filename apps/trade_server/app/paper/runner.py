"""Paper-only execution runner for M7."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from apps.trade_server.app.paper.bootstrap import PaperExecutionBootstrap
from apps.trade_server.app.paper.contracts import PaperExecutionResult
from apps.trade_server.app.planning.ingest import ingest_execution_task_dry_run
from libs.common.time import ensure_cn_aware
from libs.execution import (
    ExecutionArtifactStore,
    PaperExecutionRunRecord,
    PaperFillModelConfig,
    PaperOrderRecord,
    PaperOrderStatus,
    PaperReconcileReportRecord,
    PaperRunStatus,
    PaperTradeRecord,
    load_bars_for_order,
    load_fill_model_config,
    simulate_limit_fill,
)
from libs.execution.ledger import PaperLedger
from libs.execution.reconcile import (
    build_account_snapshot,
    build_position_snapshots,
    build_reconcile_report,
)
from libs.execution.validation import (
    validate_cash_available,
    validate_sellable_quantity,
    validate_static_order_inputs,
)
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.raw_store import file_sha256, list_partition_files, relative_path, stable_hash
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.rebalance import (
    load_account_snapshot,
    load_market_snapshots,
    load_position_snapshots,
)
from libs.planning.schemas import ExecutionTaskManifest, ExecutionTaskRecord
from libs.rules_engine.calendar import load_calendars
from libs.rules_engine.market_rules import RulesRepository
from libs.schemas.master_data import Instrument
from libs.schemas.trading import AccountSnapshot, MarketSnapshot, OrderSide, PositionSnapshot
from scripts.load_master_data import BootstrapPayload, load_bootstrap

DEFAULT_FILL_MODEL_CONFIG = Path("configs/execution/paper_fill_model.yaml")


def run_paper_execution(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str | None = None,
    config_path: Path = DEFAULT_FILL_MODEL_CONFIG,
    account_snapshot_path: Path | None = None,
    positions_path: Path | None = None,
    market_snapshot_path: Path | None = None,
    position_cost_basis_path: Path | None = None,
    force: bool = False,
) -> PaperExecutionResult:
    context = PaperExecutionBootstrap(project_root).bootstrap()
    planning_store = context.planning_store
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
    fill_config = load_fill_model_config(context.project_root / config_path)
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
    accepted_intents = _load_accepted_order_intents(
        planning_store,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task.execution_task_id,
    )
    fill_model_config_hash = stable_hash(fill_config.model_dump(mode="json"))
    market_data_hash = _build_market_data_hash(
        project_root=context.project_root,
        trade_date=trade_date,
        intents=accepted_intents.to_dict(orient="records"),
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
    paper_run_id = "paper_" + stable_hash(
        {
            "execution_task_id": execution_task.execution_task_id,
            "fill_model_config_hash": fill_model_config_hash,
            "market_data_hash": market_data_hash,
            "account_state_hash": account_state_hash,
        }
    )[:12]

    if execution_store.has_run(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        paper_run_id=paper_run_id,
    ):
        existing_run = execution_store.load_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        )
        if existing_run.status == PaperRunStatus.SUCCESS and not force:
            manifest = execution_store.load_manifest(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                paper_run_id=paper_run_id,
            )
            return PaperExecutionResult(
                paper_run_id=paper_run_id,
                execution_task_id=execution_task.execution_task_id,
                strategy_run_id=execution_task.strategy_run_id,
                account_id=account_id,
                basket_id=basket_id,
                trade_date=trade_date,
                status=PaperRunStatus.SUCCESS,
                order_count=manifest.orders_count,
                trade_count=manifest.trades_count,
                report_path=manifest.report_file_path,
                send_order_called=False,
                reused=True,
            )
        execution_store.clear_run(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        )

    created_at = ensure_cn_aware(datetime.now())
    base_run = PaperExecutionRunRecord(
        paper_run_id=paper_run_id,
        strategy_run_id=execution_task.strategy_run_id,
        execution_task_id=execution_task.execution_task_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        fill_model_name=fill_config.fill_model_name,
        fill_model_config_hash=fill_model_config_hash,
        market_data_hash=market_data_hash,
        account_state_hash=account_state_hash,
        status=PaperRunStatus.CREATED,
        created_at=created_at,
        source_prediction_run_id=target_manifest.prediction_run_id,
        source_qlib_export_run_id=execution_task.source_qlib_export_run_id,
        source_standard_build_run_id=execution_task.source_standard_build_run_id,
    )
    execution_store.save_run(base_run)

    try:
        result = _execute_run(
            project_root=context.project_root,
            execution_store=execution_store,
            paper_run=base_run,
            execution_task=execution_task,
            account_snapshot=account_snapshot,
            positions=positions,
            market_snapshots=market_snapshots,
            avg_price_by_instrument=avg_price_by_instrument,
            accepted_intents=accepted_intents.to_dict(orient="records"),
            catalog=catalog,
            payload=payload,
            rules_repo=rules_repo,
            fill_config=fill_config,
            send_order_called=context.send_order_called or ingest_result.send_order_called,
        )
    except Exception as exc:
        failed_run = base_run.model_copy(update={"status": PaperRunStatus.FAILED})
        execution_store.save_failed_run(failed_run, error_message=str(exc))
        raise
    return result


def load_reconcile_report(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    paper_run_id: str | None = None,
    execution_task_id: str | None = None,
    latest: bool = False,
) -> PaperReconcileReportRecord:
    store = ExecutionArtifactStore(project_root)
    resolved_run = _select_reconcile_run(
        store=store,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        paper_run_id=paper_run_id,
        execution_task_id=execution_task_id,
        latest=latest,
    )
    return store.load_reconcile_report(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        paper_run_id=resolved_run.paper_run_id,
    )


def _execute_run(
    *,
    project_root: Path,
    execution_store: ExecutionArtifactStore,
    paper_run: PaperExecutionRunRecord,
    execution_task: ExecutionTaskRecord,
    account_snapshot: AccountSnapshot,
    positions: dict[str, PositionSnapshot],
    market_snapshots: dict[str, MarketSnapshot],
    avg_price_by_instrument: dict[str, Decimal],
    accepted_intents: list[dict[str, object]],
    catalog: InstrumentCatalog,
    payload: BootstrapPayload,
    rules_repo: RulesRepository,
    fill_config: PaperFillModelConfig,
    send_order_called: bool,
) -> PaperExecutionResult:
    created_at = paper_run.created_at
    instruments: dict[str, Instrument] = {}
    for instrument_key in sorted(positions):
        instruments[instrument_key] = catalog.resolve(instrument_key=instrument_key).instrument
    ledger = PaperLedger.from_snapshots(
        trade_date=paper_run.trade_date,
        account_snapshot=account_snapshot,
        positions=positions,
        avg_price_by_instrument=avg_price_by_instrument,
        instruments=instruments,
        rules_repo=rules_repo,
        payload=payload,
        broker=fill_config.broker,
    )
    bars_cache: dict[str, Any] = {}
    market_prices: dict[str, Decimal] = {}
    orders: list[PaperOrderRecord] = []
    trades: list[PaperTradeRecord] = []

    sorted_intents = sorted(accepted_intents, key=_intent_sort_key)
    for index, row in enumerate(sorted_intents, start=1):
        instrument_key = str(row["instrument_key"])
        instrument = instruments.get(instrument_key)
        market_snapshot = market_snapshots.get(instrument_key)
        if instrument is None:
            try:
                instrument = catalog.resolve(instrument_key=instrument_key).instrument
            except KeyError:
                instrument = None
            else:
                instruments[instrument_key] = instrument
        previous_close = (
            market_snapshot.previous_close
            if market_snapshot is not None and market_snapshot.previous_close is not None
            else _as_decimal(row.get("previous_close"))
        )
        reason = validate_static_order_inputs(instrument=instrument, previous_close=previous_close)
        if instrument is None:
            bars = None
        else:
            bars = bars_cache.get(instrument_key)
            if bars is None:
                bars = load_bars_for_order(
                    project_root=project_root,
                    trade_date=paper_run.trade_date,
                    exchange=str(row["exchange"]),
                    symbol=str(row["symbol"]),
                    source_standard_build_run_id=paper_run.source_standard_build_run_id,
                )
                bars_cache[instrument_key] = bars
        side = OrderSide.BUY if _as_int(row["delta_quantity"]) > 0 else OrderSide.SELL
        quantity = abs(_as_int(row["delta_quantity"]))
        reference_price = _required_decimal(row["reference_price"])
        limit_price = (
            reference_price
            if fill_config.limit_price_source == "reference_price" or previous_close is None
            else previous_close
        )
        source_order_intent_hash = stable_hash(
            {
                "execution_task_id": paper_run.execution_task_id,
                "instrument_key": instrument_key,
                "side": side.value,
                "quantity": quantity,
                "reference_price": str(reference_price),
                "previous_close": str(previous_close) if previous_close is not None else None,
                "source_target_weight_hash": str(row.get("source_target_weight_hash")),
            }
        )
        order_id = "porder_" + stable_hash(
            {
                "paper_run_id": paper_run.paper_run_id,
                "order_index": index,
                "source_order_intent_hash": source_order_intent_hash,
            }
        )[:12]
        estimated_cost = _as_decimal(row.get("estimated_cost", "0")) or Decimal("0")
        paper_order = PaperOrderRecord(
            order_id=order_id,
            paper_run_id=paper_run.paper_run_id,
            execution_task_id=paper_run.execution_task_id,
            strategy_run_id=paper_run.strategy_run_id,
            account_id=paper_run.account_id,
            basket_id=paper_run.basket_id,
            trade_date=paper_run.trade_date,
            instrument_key=instrument_key,
            symbol=str(row["symbol"]),
            exchange=str(row["exchange"]),
            side=side.value,
            order_type=fill_config.order_type,
            quantity=quantity,
            limit_price=limit_price,
            reference_price=reference_price,
            previous_close=previous_close,
            source_order_intent_hash=source_order_intent_hash,
            status=PaperOrderStatus.CREATED,
            created_at=created_at,
            estimated_cost=estimated_cost,
            source_prediction_run_id=paper_run.source_prediction_run_id,
            source_qlib_export_run_id=paper_run.source_qlib_export_run_id,
            source_standard_build_run_id=paper_run.source_standard_build_run_id,
        )
        if reason is None and side == OrderSide.SELL and instrument is not None:
            sell_reason = validate_sellable_quantity(
                requested=quantity,
                sellable_quantity=ledger.get_position(instrument, previous_close or Decimal("0")).sellable_quantity,
            )
            reason = sell_reason
        if reason is None:
            fill_decision = simulate_limit_fill(
                side=side.value,
                limit_price=limit_price,
                bars=bars,
                config=fill_config,
            )
            if fill_decision.status == PaperOrderStatus.FILLED and instrument is not None and previous_close is not None:
                fill_price = fill_decision.fill_price or limit_price
                if side == OrderSide.BUY:
                    required_cash = (fill_price * Decimal(quantity)) + estimated_cost
                    reason = validate_cash_available(
                        available_cash=ledger.available_cash,
                        required_cash=required_cash,
                        policy=fill_config.insufficient_cash_behavior,
                    )
                if reason is None:
                    ledger_result = ledger.apply_fill(
                        instrument=instrument,
                        side=side,
                        quantity=quantity,
                        price=fill_price,
                        previous_close=previous_close,
                    )
                    if not ledger_result.accepted or ledger_result.cost is None:
                        reason = ledger_result.reason
                    else:
                        paper_order = paper_order.model_copy(update={"status": PaperOrderStatus.FILLED})
                        trade_id = "ptrade_" + stable_hash(
                            {
                                "paper_run_id": paper_run.paper_run_id,
                                "order_id": order_id,
                                "fill_bar_dt": fill_decision.fill_bar_dt.isoformat()
                                if fill_decision.fill_bar_dt
                                else None,
                            }
                        )[:12]
                        trades.append(
                            PaperTradeRecord(
                                paper_run_id=paper_run.paper_run_id,
                                order_id=order_id,
                                trade_id=trade_id,
                                execution_task_id=paper_run.execution_task_id,
                                strategy_run_id=paper_run.strategy_run_id,
                                instrument_key=instrument_key,
                                symbol=paper_order.symbol,
                                exchange=paper_order.exchange,
                                side=paper_order.side,
                                quantity=quantity,
                                price=fill_price,
                                notional=ledger_result.cost.notional,
                                cost_breakdown_json=ledger_result.cost.model_dump(mode="json"),
                                fill_bar_dt=fill_decision.fill_bar_dt or created_at,
                                created_at=created_at,
                                source_prediction_run_id=paper_run.source_prediction_run_id,
                                source_qlib_export_run_id=paper_run.source_qlib_export_run_id,
                                source_standard_build_run_id=paper_run.source_standard_build_run_id,
                            )
                        )
            else:
                reason = fill_decision.reason
                if fill_decision.status == PaperOrderStatus.UNFILLED:
                    paper_order = paper_order.model_copy(
                        update={"status": PaperOrderStatus.UNFILLED, "status_reason": reason}
                    )
        if reason is not None and paper_order.status != PaperOrderStatus.FILLED:
            paper_order = paper_order.model_copy(
                update={
                    "status": PaperOrderStatus.UNFILLED
                    if reason in {"missing_bars", "limit_not_crossed"}
                    else PaperOrderStatus.REJECTED,
                    "status_reason": reason,
                }
            )
        orders.append(paper_order)
        if instrument is not None:
            market_prices[instrument_key] = _resolve_market_price(
                bars=bars,
                fallback=market_snapshot.last_price if market_snapshot is not None else previous_close,
            )

    for instrument_key, position in ledger.positions.items():
        if instrument_key not in market_prices:
            fallback = market_snapshots.get(instrument_key)
            market_prices[instrument_key] = (
                fallback.last_price if fallback is not None else position.avg_price
            )

    market_value_end = sum(
        (
            Decimal(position.quantity) * market_prices.get(instrument_key, position.avg_price)
            for instrument_key, position in ledger.positions.items()
            if position.quantity > 0
        ),
        Decimal("0"),
    )
    account_record = build_account_snapshot(
        paper_run_id=paper_run.paper_run_id,
        strategy_run_id=paper_run.strategy_run_id,
        execution_task_id=paper_run.execution_task_id,
        account_id=paper_run.account_id,
        trade_date=paper_run.trade_date,
        created_at=created_at,
        ledger=ledger,
        market_value_end=market_value_end,
        source_prediction_run_id=paper_run.source_prediction_run_id,
        source_qlib_export_run_id=paper_run.source_qlib_export_run_id,
        source_standard_build_run_id=paper_run.source_standard_build_run_id,
    )
    position_records = build_position_snapshots(
        paper_run_id=paper_run.paper_run_id,
        strategy_run_id=paper_run.strategy_run_id,
        execution_task_id=paper_run.execution_task_id,
        account_id=paper_run.account_id,
        trade_date=paper_run.trade_date,
        created_at=created_at,
        positions=ledger.positions,
        instruments=instruments,
        market_prices=market_prices,
        source_prediction_run_id=paper_run.source_prediction_run_id,
        source_qlib_export_run_id=paper_run.source_qlib_export_run_id,
        source_standard_build_run_id=paper_run.source_standard_build_run_id,
    )
    report = build_reconcile_report(
        paper_run_id=paper_run.paper_run_id,
        strategy_run_id=paper_run.strategy_run_id,
        execution_task_id=paper_run.execution_task_id,
        account_id=paper_run.account_id,
        basket_id=paper_run.basket_id,
        trade_date=paper_run.trade_date,
        created_at=created_at,
        orders=orders,
        trades=trades,
        source_prediction_run_id=paper_run.source_prediction_run_id,
        source_qlib_export_run_id=paper_run.source_qlib_export_run_id,
        source_standard_build_run_id=paper_run.source_standard_build_run_id,
    )
    success_run = paper_run.model_copy(update={"status": PaperRunStatus.SUCCESS})
    manifest = execution_store.save_success(
        run=success_run,
        orders=orders,
        trades=trades,
        account_snapshot=account_record,
        positions=position_records,
        report=report,
    )
    return PaperExecutionResult(
        paper_run_id=success_run.paper_run_id,
        execution_task_id=success_run.execution_task_id,
        strategy_run_id=success_run.strategy_run_id,
        account_id=success_run.account_id,
        basket_id=success_run.basket_id,
        trade_date=success_run.trade_date,
        status=success_run.status,
        order_count=len(orders),
        trade_count=len(trades),
        report_path=manifest.report_file_path,
        send_order_called=send_order_called,
        reused=False,
    )


def _resolve_execution_task_manifest(
    store: PlanningArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str | None,
) -> ExecutionTaskManifest:
    if execution_task_id is not None:
        return store.load_execution_task_manifest(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        )
    for manifest in store.list_execution_task_manifests():
        if (
            manifest.trade_date == trade_date
            and manifest.account_id == account_id
            and manifest.basket_id == basket_id
        ):
            return manifest
    raise FileNotFoundError(
        f"no execution_task artifact found for trade_date={trade_date.isoformat()} "
        f"account_id={account_id} basket_id={basket_id}"
    )


def _select_reconcile_run(
    store: ExecutionArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    paper_run_id: str | None,
    execution_task_id: str | None,
    latest: bool,
) -> PaperExecutionRunRecord:
    runs = [
        run
        for run in store.list_runs()
        if run.trade_date == trade_date
        and run.account_id == account_id
        and run.basket_id == basket_id
    ]
    if not runs:
        raise FileNotFoundError(
            f"no paper run found for trade_date={trade_date.isoformat()} "
            f"account_id={account_id} basket_id={basket_id}"
        )
    if paper_run_id is not None:
        for run in runs:
            if run.paper_run_id == paper_run_id:
                return run
        raise FileNotFoundError(
            f"no paper run found for paper_run_id={paper_run_id} "
            f"trade_date={trade_date.isoformat()} account_id={account_id} basket_id={basket_id}"
        )
    filtered_runs = runs
    if execution_task_id is not None:
        filtered_runs = [run for run in runs if run.execution_task_id == execution_task_id]
        if not filtered_runs:
            raise FileNotFoundError(
                f"no paper run found for execution_task_id={execution_task_id} "
                f"trade_date={trade_date.isoformat()} account_id={account_id} basket_id={basket_id}"
            )
    if len(filtered_runs) == 1:
        return filtered_runs[0]
    if latest:
        return filtered_runs[0]
    qualifier = (
        f"execution_task_id={execution_task_id}"
        if execution_task_id is not None
        else f"trade_date={trade_date.isoformat()} account_id={account_id} basket_id={basket_id}"
    )
    raise ValueError(
        f"multiple paper runs match {qualifier}; pass --paper-run-id or --latest"
    )


def _load_avg_price_seed(
    *,
    seed_path: Path | None,
    positions: dict[str, PositionSnapshot],
    market_snapshots: dict[str, MarketSnapshot],
) -> dict[str, Decimal]:
    seed_payload: dict[str, str] = {}
    if seed_path is not None and seed_path.exists():
        raw_payload = json.loads(seed_path.read_text(encoding="utf-8"))
        seed_payload = {str(item["instrument_key"]): str(item["avg_price"]) for item in raw_payload["positions"]}
    results: dict[str, Decimal] = {}
    for instrument_key in positions:
        if instrument_key in seed_payload:
            results[instrument_key] = Decimal(seed_payload[instrument_key])
            continue
        market = market_snapshots.get(instrument_key)
        if market is not None and market.previous_close is not None:
            results[instrument_key] = market.previous_close
        else:
            results[instrument_key] = Decimal("0")
    return results


def _load_accepted_order_intents(
    store: PlanningArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str,
) -> Any:
    frame = store.load_order_intents(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_id,
    )
    return frame[
        (frame["validation_status"] == "accepted") & (frame["delta_quantity"].astype(int).abs() > 0)
    ].copy()


def _build_market_data_hash(
    *,
    project_root: Path,
    trade_date: date,
    intents: list[dict[str, object]],
    source_standard_build_run_id: str | None,
    market_snapshots: dict[str, MarketSnapshot],
) -> str:
    manifest_store = ManifestStore(project_root / "data" / "manifests")
    manifests = [
        manifest
        for manifest in manifest_store.list_standard_file_manifests(layer="bars_1m")
        if manifest.trade_date == trade_date
        and (
            source_standard_build_run_id is None
            or manifest.build_run_id == source_standard_build_run_id
        )
    ]
    wanted = {(str(item["instrument_key"]), str(item["symbol"]), str(item["exchange"])) for item in intents}
    relevant = [
        manifest
        for manifest in manifests
        if (str(manifest.instrument_key), str(manifest.symbol), str(manifest.exchange)) in wanted
    ]
    market_snapshot_hash = stable_hash(
        {
            key: value.model_dump(mode="json")
            for key, value in sorted(market_snapshots.items())
        }
    )
    if relevant:
        return stable_hash(
            {
                "bars": {
                    f"{item.instrument_key}:{item.symbol}:{item.exchange}": item.file_hash
                    for item in relevant
                },
                "market_snapshots": market_snapshot_hash,
            }
        )
    files = []
    for item in intents:
        files.extend(
            list_partition_files(
                project_root / "data" / "standard" / "bars_1m",
                trade_date=trade_date,
                exchange=str(item["exchange"]),
                symbol=str(item["symbol"]),
            )
        )
    unique_files = sorted(set(files))
    return stable_hash(
        {
            "bars": [
                {
                    "path": relative_path(project_root, path),
                    "file_hash": file_sha256(path),
                }
                for path in unique_files
            ],
            "market_snapshots": market_snapshot_hash,
        }
    )


def _resolve_execution_input_paths(
    *,
    project_root: Path,
    trade_date: date,
    account_snapshot_path: Path | None,
    positions_path: Path | None,
    market_snapshot_path: Path | None,
) -> tuple[Path, Path, Path]:
    sample_root = project_root / "data" / "bootstrap" / "execution_sample"
    return (
        account_snapshot_path or sample_root / "account_demo.json",
        positions_path or sample_root / "positions_demo.json",
        market_snapshot_path or sample_root / f"market_snapshot_{trade_date.isoformat()}.json",
    )


def _resolve_avg_price_seed_path(
    *,
    project_root: Path,
    positions_path: Path | None,
    position_cost_basis_path: Path | None,
) -> Path | None:
    if position_cost_basis_path is not None:
        if not position_cost_basis_path.exists():
            raise FileNotFoundError(f"position cost basis file not found: {position_cost_basis_path}")
        return position_cost_basis_path
    sample_root = project_root / "data" / "bootstrap" / "execution_sample"
    if positions_path is None:
        return sample_root / "position_cost_basis_demo.json"
    for candidate in (
        positions_path.parent / "position_cost_basis.json",
        positions_path.parent / "position_cost_basis_demo.json",
    ):
        if candidate.exists():
            return candidate
    return None


def _intent_sort_key(row: dict[str, object]) -> tuple[int, str, str]:
    side_rank = 0 if _as_int(row["delta_quantity"]) < 0 else 1
    return (side_rank, str(row["symbol"]), str(row["instrument_key"]))


def _resolve_market_price(*, bars: Any, fallback: Decimal | None) -> Decimal:
    if bars is not None and not bars.empty:
        last_row = bars.sort_values("bar_dt").iloc[-1]
        return Decimal(str(last_row["close"]))
    return fallback or Decimal("0")


def _as_decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _required_decimal(value: object | None) -> Decimal:
    decimal_value = _as_decimal(value)
    if decimal_value is None:
        raise ValueError("expected decimal value for paper execution")
    return decimal_value


def _as_int(value: object) -> int:
    return int(str(value))
