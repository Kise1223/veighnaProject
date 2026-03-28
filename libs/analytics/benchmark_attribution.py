"""Benchmark reference and attribution analytics for M13."""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore
from libs.analytics.attribution_config import default_benchmark_analytics_config
from libs.analytics.attribution_schemas import (
    BenchmarkAnalyticsRunRecord,
    BenchmarkGroupRowRecord,
    BenchmarkPositionRowRecord,
    BenchmarkSummaryRowRecord,
)
from libs.analytics.benchmark_artifacts import BenchmarkReferenceArtifactStore
from libs.analytics.benchmark_config import default_benchmark_reference_config
from libs.analytics.benchmark_loaders import (
    LoadedBenchmarkReference,
    LoadedPortfolioAnalyticsSource,
    load_benchmark_reference,
    select_portfolio_analytics_source,
)
from libs.analytics.benchmark_normalize import (
    compute_active_share,
    compute_hhi_concentration,
    compute_top_concentration,
    quantize_return,
    quantize_weight,
    safe_decimal_ratio,
)
from libs.analytics.benchmark_schemas import (
    BenchmarkReferenceRunRecord,
    BenchmarkRunStatus,
    BenchmarkSourceType,
    BenchmarkSummaryRecord,
    BenchmarkWeightRowRecord,
)
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import file_sha256, stable_hash
from libs.schemas.master_data import Instrument

_ZERO = Decimal("0")


def build_benchmark_reference(
    *,
    project_root: Path,
    portfolio_analytics_run_id: str | None = None,
    paper_run_id: str | None = None,
    shadow_run_id: str | None = None,
    trade_date: date | None = None,
    account_id: str | None = None,
    basket_id: str | None = None,
    latest: bool = False,
    source_type: str,
    benchmark_path: Path | None = None,
    benchmark_name: str | None = None,
    force: bool = False,
) -> dict[str, object]:
    loaded = select_portfolio_analytics_source(
        project_root=project_root,
        portfolio_analytics_run_id=portfolio_analytics_run_id,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        paper_run_id=paper_run_id,
        shadow_run_id=shadow_run_id,
        latest=latest,
    )
    benchmark_source_type = BenchmarkSourceType(source_type)
    if benchmark_source_type == BenchmarkSourceType.CUSTOM_WEIGHTS and benchmark_path is None:
        raise ValueError("--benchmark-path is required when --source-type=custom_weights")
    config = default_benchmark_reference_config()
    benchmark_source_hash = file_sha256(benchmark_path) if benchmark_path is not None else None
    benchmark_label = benchmark_name or _default_benchmark_name(
        source_type=benchmark_source_type,
        source_portfolio_analytics_run_id=loaded.manifest.portfolio_analytics_run_id,
    )
    config_hash = stable_hash(
        {
            "config": config.model_dump(mode="json"),
            "source_type": benchmark_source_type.value,
            "benchmark_name": benchmark_label,
            "benchmark_source_hash": benchmark_source_hash,
        }
    )
    benchmark_run_id = build_benchmark_run_id(
        source_portfolio_analytics_run_id=loaded.manifest.portfolio_analytics_run_id,
        benchmark_config_hash=config_hash,
    )
    store = BenchmarkReferenceArtifactStore(project_root)
    if store.has_benchmark_run(trade_date=loaded.manifest.trade_date, benchmark_run_id=benchmark_run_id):
        existing = store.load_benchmark_run(
            trade_date=loaded.manifest.trade_date,
            benchmark_run_id=benchmark_run_id,
        )
        if existing.status == BenchmarkRunStatus.SUCCESS and not force:
            manifest = store.load_benchmark_manifest(
                trade_date=loaded.manifest.trade_date,
                benchmark_run_id=benchmark_run_id,
            )
            summary = store.load_benchmark_summary(
                trade_date=loaded.manifest.trade_date,
                benchmark_run_id=benchmark_run_id,
            )
            return {
                "benchmark_run_id": benchmark_run_id,
                "benchmark_name": benchmark_label,
                "benchmark_source_type": benchmark_source_type.value,
                "weight_row_count": manifest.weight_row_count,
                "summary_path": manifest.summary_file_path,
                "benchmark_cash_weight": summary.benchmark_cash_weight,
                "status": existing.status.value,
                "reused": True,
            }
        store.clear_benchmark_run(trade_date=loaded.manifest.trade_date, benchmark_run_id=benchmark_run_id)
    created_at = ensure_cn_aware(datetime.now())
    run = BenchmarkReferenceRunRecord(
        benchmark_run_id=benchmark_run_id,
        trade_date=loaded.manifest.trade_date,
        benchmark_name=benchmark_label,
        benchmark_source_type=benchmark_source_type,
        source_portfolio_analytics_run_id=loaded.manifest.portfolio_analytics_run_id,
        source_strategy_run_id=loaded.manifest.source_strategy_run_id,
        source_prediction_run_id=loaded.manifest.source_prediction_run_id,
        benchmark_config_hash=config_hash,
        status=BenchmarkRunStatus.CREATED,
        created_at=created_at,
        source_qlib_export_run_id=loaded.manifest.source_qlib_export_run_id,
        source_standard_build_run_id=loaded.manifest.source_standard_build_run_id,
    )
    store.save_benchmark_run(run)
    try:
        weights, summary = _build_benchmark_reference_rows(
            loaded=loaded,
            benchmark_run_id=benchmark_run_id,
            benchmark_source_type=benchmark_source_type,
            benchmark_path=benchmark_path,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": BenchmarkRunStatus.SUCCESS})
        manifest = store.save_benchmark_success(run=success_run, weights=weights, summary=summary)
    except Exception as exc:
        failed_run = run.model_copy(update={"status": BenchmarkRunStatus.FAILED})
        store.save_failed_benchmark_run(failed_run, error_message=str(exc))
        raise
    return {
        "benchmark_run_id": benchmark_run_id,
        "benchmark_name": benchmark_label,
        "benchmark_source_type": benchmark_source_type.value,
        "weight_row_count": len(weights),
        "summary_path": manifest.summary_file_path,
        "benchmark_cash_weight": summary.benchmark_cash_weight,
        "status": success_run.status.value,
        "reused": False,
    }


def build_benchmark_run_id(*, source_portfolio_analytics_run_id: str, benchmark_config_hash: str) -> str:
    return "benchmark_" + stable_hash(
        {
            "source_portfolio_analytics_run_id": source_portfolio_analytics_run_id,
            "benchmark_config_hash": benchmark_config_hash,
        }
    )[:12]


def run_benchmark_analytics(
    *,
    project_root: Path,
    portfolio_analytics_run_id: str | None = None,
    paper_run_id: str | None = None,
    shadow_run_id: str | None = None,
    trade_date: date | None = None,
    account_id: str | None = None,
    basket_id: str | None = None,
    latest: bool = False,
    benchmark_run_id: str | None = None,
    benchmark_source_type: str | None = None,
    benchmark_path: Path | None = None,
    benchmark_name: str | None = None,
    force: bool = False,
) -> dict[str, object]:
    loaded = select_portfolio_analytics_source(
        project_root=project_root,
        portfolio_analytics_run_id=portfolio_analytics_run_id,
        paper_run_id=paper_run_id,
        shadow_run_id=shadow_run_id,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        latest=latest,
    )
    if benchmark_run_id is None:
        if benchmark_source_type is None:
            raise ValueError("--benchmark-run-id or --benchmark-source-type is required")
        benchmark_result = build_benchmark_reference(
            project_root=project_root,
            portfolio_analytics_run_id=loaded.manifest.portfolio_analytics_run_id,
            source_type=benchmark_source_type,
            benchmark_path=benchmark_path,
            benchmark_name=benchmark_name,
        )
        benchmark_run_id = str(benchmark_result["benchmark_run_id"])
    benchmark = load_benchmark_reference(project_root=project_root, benchmark_run_id=benchmark_run_id)
    config_hash = stable_hash(default_benchmark_analytics_config().model_dump(mode="json"))
    source_run_id = loaded.portfolio_source.source_run_id
    benchmark_analytics_run_id = build_benchmark_analytics_run_id(
        source_portfolio_analytics_run_id=loaded.manifest.portfolio_analytics_run_id,
        source_run_id=source_run_id,
        benchmark_run_id=benchmark.run.benchmark_run_id,
        analytics_config_hash=config_hash,
    )
    store = BenchmarkAttributionArtifactStore(project_root)
    if store.has_benchmark_analytics_run(
        trade_date=loaded.manifest.trade_date,
        account_id=loaded.manifest.account_id,
        basket_id=loaded.manifest.basket_id,
        benchmark_analytics_run_id=benchmark_analytics_run_id,
    ):
        existing = store.load_benchmark_analytics_run(
            trade_date=loaded.manifest.trade_date,
            account_id=loaded.manifest.account_id,
            basket_id=loaded.manifest.basket_id,
            benchmark_analytics_run_id=benchmark_analytics_run_id,
        )
        if existing.status == BenchmarkRunStatus.SUCCESS and not force:
            manifest = store.load_benchmark_analytics_manifest(
                trade_date=loaded.manifest.trade_date,
                account_id=loaded.manifest.account_id,
                basket_id=loaded.manifest.basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            )
            summary = store.load_benchmark_summary(
                trade_date=loaded.manifest.trade_date,
                account_id=loaded.manifest.account_id,
                basket_id=loaded.manifest.basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            )
            return {
                "benchmark_analytics_run_id": benchmark_analytics_run_id,
                "source_run_id": source_run_id,
                "benchmark_run_id": benchmark.run.benchmark_run_id,
                "position_row_count": manifest.position_row_count,
                "summary_path": manifest.summary_file_path,
                "executed_active_share": summary.executed_active_share,
                "status": existing.status.value,
                "reused": True,
            }
        store.clear_benchmark_analytics_run(
            trade_date=loaded.manifest.trade_date,
            account_id=loaded.manifest.account_id,
            basket_id=loaded.manifest.basket_id,
            benchmark_analytics_run_id=benchmark_analytics_run_id,
        )
    created_at = ensure_cn_aware(datetime.now())
    run = BenchmarkAnalyticsRunRecord(
        benchmark_analytics_run_id=benchmark_analytics_run_id,
        trade_date=loaded.manifest.trade_date,
        account_id=loaded.manifest.account_id,
        basket_id=loaded.manifest.basket_id,
        source_portfolio_analytics_run_id=loaded.manifest.portfolio_analytics_run_id,
        source_run_type=loaded.manifest.source_type.value,
        source_run_id=source_run_id,
        source_execution_task_id=loaded.manifest.source_execution_task_id,
        source_strategy_run_id=loaded.manifest.source_strategy_run_id,
        source_prediction_run_id=loaded.manifest.source_prediction_run_id,
        benchmark_run_id=benchmark.run.benchmark_run_id,
        analytics_config_hash=config_hash,
        status=BenchmarkRunStatus.CREATED,
        created_at=created_at,
        source_qlib_export_run_id=loaded.manifest.source_qlib_export_run_id,
        source_standard_build_run_id=loaded.manifest.source_standard_build_run_id,
    )
    store.save_benchmark_analytics_run(run)
    try:
        positions, groups, summary = build_benchmark_analytics(
            loaded=loaded,
            benchmark=benchmark,
            benchmark_analytics_run_id=benchmark_analytics_run_id,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": BenchmarkRunStatus.SUCCESS})
        manifest = store.save_benchmark_analytics_success(
            run=success_run,
            positions=positions,
            groups=groups,
            summary=summary,
        )
    except Exception as exc:
        failed_run = run.model_copy(update={"status": BenchmarkRunStatus.FAILED})
        store.save_failed_benchmark_analytics_run(failed_run, error_message=str(exc))
        raise
    return {
        "benchmark_analytics_run_id": benchmark_analytics_run_id,
        "source_run_id": source_run_id,
        "benchmark_run_id": benchmark.run.benchmark_run_id,
        "position_row_count": len(positions),
        "summary_path": manifest.summary_file_path,
        "executed_active_share": summary.executed_active_share,
        "status": success_run.status.value,
        "reused": False,
    }


def build_benchmark_analytics_run_id(
    *,
    source_portfolio_analytics_run_id: str,
    source_run_id: str,
    benchmark_run_id: str,
    analytics_config_hash: str,
) -> str:
    return "banalytics_" + stable_hash(
        {
            "source_portfolio_analytics_run_id": source_portfolio_analytics_run_id,
            "source_run_id": source_run_id,
            "benchmark_run_id": benchmark_run_id,
            "analytics_config_hash": analytics_config_hash,
        }
    )[:12]


def build_benchmark_analytics(
    *,
    loaded: LoadedPortfolioAnalyticsSource,
    benchmark: LoadedBenchmarkReference,
    benchmark_analytics_run_id: str,
    created_at: datetime,
) -> tuple[list[BenchmarkPositionRowRecord], list[BenchmarkGroupRowRecord], BenchmarkSummaryRowRecord]:
    portfolio_rows = {str(item["instrument_key"]): item for item in loaded.positions_frame.to_dict(orient="records")}
    benchmark_rows = {str(item["instrument_key"]): item for item in benchmark.weight_frame.to_dict(orient="records")}
    target_weights = {
        item.instrument_key: item.target_weight
        for item in loaded.portfolio_source.target_weights
        if item.target_weight > _ZERO
    }
    previous_close_by_instrument = {
        item.instrument_key: (item.previous_close or item.reference_price or _ZERO)
        for item in loaded.portfolio_source.order_intents
    }
    instruments = loaded.portfolio_source.instruments
    position_rows: list[BenchmarkPositionRowRecord] = []
    group_totals: dict[tuple[str, str], dict[str, Decimal]] = defaultdict(
        lambda: {
            "target_weight_sum": Decimal("0"),
            "executed_weight_sum": Decimal("0"),
            "benchmark_weight_sum": Decimal("0"),
            "active_weight_sum": Decimal("0"),
            "portfolio_contribution_sum": Decimal("0"),
            "benchmark_contribution_sum": Decimal("0"),
        }
    )
    total_portfolio_contribution_proxy = Decimal("0")
    total_benchmark_contribution_proxy = Decimal("0")
    total_active_contribution_proxy = Decimal("0")
    executed_weights: dict[str, Decimal] = {}
    benchmark_weights: dict[str, Decimal] = {
        instrument_key: Decimal(str(row["benchmark_weight"])) for instrument_key, row in benchmark_rows.items()
    }
    target_overlap_count = 0
    executed_overlap_count = 0
    all_instruments = sorted(set(portfolio_rows) | set(benchmark_rows) | set(target_weights))
    for instrument_key in all_instruments:
        row = portfolio_rows.get(instrument_key)
        benchmark_weight = benchmark_weights.get(instrument_key, _ZERO)
        target_weight = (
            Decimal(str(row["target_weight"])) if row is not None else target_weights.get(instrument_key, _ZERO)
        )
        executed_weight = Decimal(str(row["executed_weight"])) if row is not None else _ZERO
        active_weight_target = quantize_weight(target_weight - benchmark_weight)
        active_weight_executed = quantize_weight(executed_weight - benchmark_weight)
        if benchmark_weight > _ZERO and target_weight > _ZERO:
            target_overlap_count += 1
        if benchmark_weight > _ZERO and executed_weight > _ZERO:
            executed_overlap_count += 1
        market_value_end = Decimal(str(row["market_value_end"])) if row is not None else Decimal("0.00")
        quantity_end = int(row["quantity_end"]) if row is not None else 0
        executed_price_reference = (
            Decimal(str(row["executed_price_reference"]))
            if row is not None and row["executed_price_reference"] is not None
            else None
        )
        previous_close = previous_close_by_instrument.get(instrument_key)
        mark_price_end = _resolve_mark_price_end(
            quantity_end=quantity_end,
            market_value_end=market_value_end,
            executed_price_reference=executed_price_reference,
            previous_close=previous_close,
        )
        instrument_return_proxy = _resolve_instrument_return_proxy(
            mark_price_end=mark_price_end,
            previous_close=previous_close,
        )
        portfolio_contribution_proxy = quantize_return(executed_weight * instrument_return_proxy)
        benchmark_contribution_proxy = quantize_return(benchmark_weight * instrument_return_proxy)
        active_contribution_proxy = quantize_return(
            portfolio_contribution_proxy - benchmark_contribution_proxy
        )
        record = BenchmarkPositionRowRecord(
            benchmark_analytics_run_id=benchmark_analytics_run_id,
            instrument_key=instrument_key,
            symbol=_resolve_symbol(
                instrument_key=instrument_key,
                row=row,
                benchmark_row=benchmark_rows.get(instrument_key),
                instruments=instruments,
            ),
            exchange=_resolve_exchange(
                instrument_key=instrument_key,
                row=row,
                benchmark_row=benchmark_rows.get(instrument_key),
                instruments=instruments,
            ),
            target_weight=quantize_weight(target_weight),
            executed_weight=quantize_weight(executed_weight),
            benchmark_weight=quantize_weight(benchmark_weight),
            active_weight_target=active_weight_target,
            active_weight_executed=active_weight_executed,
            target_rank=_optional_int(row.get("target_rank")) if row is not None else None,
            target_score=_optional_float(row.get("target_score")) if row is not None else None,
            market_value_end=market_value_end.quantize(Decimal("0.01")),
            realized_pnl=Decimal(str(row["realized_pnl"])) if row is not None else Decimal("0.00"),
            unrealized_pnl=Decimal(str(row["unrealized_pnl"])) if row is not None else Decimal("0.00"),
            portfolio_contribution_proxy=portfolio_contribution_proxy,
            benchmark_contribution_proxy=benchmark_contribution_proxy,
            active_contribution_proxy=active_contribution_proxy,
            instrument_return_proxy=instrument_return_proxy,
            replay_mode=(
                str(row["replay_mode"])
                if row is not None and row.get("replay_mode") is not None
                else loaded.portfolio_source.replay_mode
            ),
            fill_model_name=(
                str(row["fill_model_name"])
                if row is not None and row.get("fill_model_name") is not None
                else loaded.portfolio_source.fill_model_name
            ),
            time_in_force=(
                str(row["time_in_force"])
                if row is not None and row.get("time_in_force") is not None
                else loaded.portfolio_source.time_in_force
            ),
            created_at=created_at,
        )
        position_rows.append(record)
        total_portfolio_contribution_proxy += record.portfolio_contribution_proxy
        total_benchmark_contribution_proxy += record.benchmark_contribution_proxy
        total_active_contribution_proxy += record.active_contribution_proxy
        executed_weights[instrument_key] = record.executed_weight
        _accumulate_group_totals(group_totals=group_totals, record=record, instruments=instruments)
    group_rows = _build_group_rows(
        benchmark_analytics_run_id=benchmark_analytics_run_id,
        group_totals=group_totals,
        created_at=created_at,
    )
    target_active_share = compute_active_share(left_weights=target_weights, right_weights=benchmark_weights)
    executed_active_share = compute_active_share(left_weights=executed_weights, right_weights=benchmark_weights)
    benchmark_positive_weights = [weight for weight in benchmark_weights.values() if weight > _ZERO]
    benchmark_top1 = compute_top_concentration(benchmark_positive_weights, 1)
    benchmark_top3 = compute_top_concentration(benchmark_positive_weights, 3)
    benchmark_top5 = compute_top_concentration(benchmark_positive_weights, 5)
    benchmark_hhi = compute_hhi_concentration(benchmark_positive_weights)
    summary = BenchmarkSummaryRowRecord(
        benchmark_analytics_run_id=benchmark_analytics_run_id,
        holdings_count_benchmark=benchmark.summary.holdings_count,
        holdings_overlap_count=executed_overlap_count,
        target_active_share=target_active_share,
        executed_active_share=executed_active_share,
        active_cash_weight=quantize_weight(
            loaded.summary.executed_cash_weight - benchmark.summary.benchmark_cash_weight
        ),
        benchmark_cash_weight=benchmark.summary.benchmark_cash_weight,
        delta_top1_concentration=quantize_weight(loaded.summary.top1_concentration - benchmark_top1),
        delta_top5_concentration=quantize_weight(loaded.summary.top5_concentration - benchmark_top5),
        delta_hhi_concentration=quantize_weight(loaded.summary.hhi_concentration - benchmark_hhi),
        total_portfolio_contribution_proxy=quantize_return(total_portfolio_contribution_proxy),
        total_benchmark_contribution_proxy=quantize_return(total_benchmark_contribution_proxy),
        total_active_contribution_proxy=quantize_return(total_active_contribution_proxy),
        summary_json={
            "benchmark_name": benchmark.run.benchmark_name,
            "benchmark_source_type": benchmark.run.benchmark_source_type.value,
            "benchmark_source_portfolio_analytics_run_id": benchmark.run.source_portfolio_analytics_run_id,
            "source_portfolio_analytics_run_id": loaded.manifest.portfolio_analytics_run_id,
            "target_overlap_count": target_overlap_count,
            "executed_overlap_count": executed_overlap_count,
            "benchmark_top3_concentration": str(benchmark_top3),
            "benchmark_hhi_concentration": str(benchmark_hhi),
            "portfolio_summary_source_json": json.dumps(
                loaded.summary.summary_json,
                ensure_ascii=False,
                sort_keys=True,
            ),
            "instrument_return_proxy_mode": "mark_price_end_or_executed_reference_vs_previous_close",
            "group_proxy_mode": "allocation_plus_selection_only",
            "industry_grouping": "skipped",
        },
        created_at=created_at,
    )
    return position_rows, group_rows, summary


def _build_benchmark_reference_rows(
    *,
    loaded: LoadedPortfolioAnalyticsSource,
    benchmark_run_id: str,
    benchmark_source_type: BenchmarkSourceType,
    benchmark_path: Path | None,
    created_at: datetime,
) -> tuple[list[BenchmarkWeightRowRecord], BenchmarkSummaryRecord]:
    raw_rows: list[tuple[str, str, str, Decimal]] = []
    if benchmark_source_type == BenchmarkSourceType.EQUAL_WEIGHT_TARGET_UNIVERSE:
        targets = sorted(
            [item for item in loaded.portfolio_source.target_weights if item.target_weight > _ZERO],
            key=lambda item: item.instrument_key,
        )
        if not targets:
            raise ValueError("equal_weight_target_universe requires at least one positive target weight")
        equal_weight = quantize_weight(Decimal("1") / Decimal(len(targets)))
        running = Decimal("1")
        for index, target in enumerate(targets):
            instrument = loaded.portfolio_source.instruments.get(target.instrument_key)
            if instrument is None:
                raise ValueError(f"missing instrument metadata for {target.instrument_key}")
            benchmark_weight = equal_weight if index < len(targets) - 1 else quantize_weight(running)
            running -= benchmark_weight
            raw_rows.append(
                (
                    target.instrument_key,
                    instrument.symbol,
                    instrument.exchange.value,
                    benchmark_weight,
                )
            )
    elif benchmark_source_type == BenchmarkSourceType.EQUAL_WEIGHT_UNION:
        union_keys = sorted(
            {
                item.instrument_key
                for item in loaded.portfolio_source.target_weights
                if item.target_weight > _ZERO
            }
            | {
                str(row["instrument_key"])
                for row in loaded.positions_frame.to_dict(orient="records")
                if Decimal(str(row["executed_weight"])) > _ZERO
            }
        )
        if not union_keys:
            raise ValueError("equal_weight_union requires at least one target or executed instrument")
        equal_weight = quantize_weight(Decimal("1") / Decimal(len(union_keys)))
        running = Decimal("1")
        for index, instrument_key in enumerate(union_keys):
            instrument = loaded.portfolio_source.instruments.get(instrument_key)
            if instrument is None:
                raise ValueError(f"missing instrument metadata for {instrument_key}")
            benchmark_weight = equal_weight if index < len(union_keys) - 1 else quantize_weight(running)
            running -= benchmark_weight
            raw_rows.append((instrument_key, instrument.symbol, instrument.exchange.value, benchmark_weight))
    else:
        if benchmark_path is None:
            raise ValueError("custom_weights benchmark requires a benchmark_path")
        raw_rows = _load_custom_benchmark_rows(
            benchmark_path=benchmark_path,
            instruments=loaded.portfolio_source.instruments,
            fallback_rows=loaded.positions_frame.to_dict(orient="records"),
        )
    weights = _normalize_benchmark_rows(
        benchmark_run_id=benchmark_run_id,
        raw_rows=raw_rows,
        created_at=created_at,
    )
    positive_weights = [item.benchmark_weight for item in weights if item.benchmark_weight > _ZERO]
    total_weight = sum((item.benchmark_weight for item in weights), _ZERO)
    summary = BenchmarkSummaryRecord(
        benchmark_run_id=benchmark_run_id,
        holdings_count=len(positive_weights),
        benchmark_cash_weight=quantize_weight(max(_ZERO, Decimal("1") - total_weight)),
        top1_concentration=compute_top_concentration(positive_weights, 1),
        top3_concentration=compute_top_concentration(positive_weights, 3),
        top5_concentration=compute_top_concentration(positive_weights, 5),
        hhi_concentration=compute_hhi_concentration(positive_weights),
        summary_json={
            "normalized_weight_sum": str(quantize_weight(total_weight)),
            "source_portfolio_analytics_run_id": loaded.manifest.portfolio_analytics_run_id,
            "source_execution_task_id": loaded.manifest.source_execution_task_id,
            "source_strategy_run_id": loaded.manifest.source_strategy_run_id,
            "benchmark_source_type": benchmark_source_type.value,
        },
        created_at=created_at,
    )
    return weights, summary


def _load_custom_benchmark_rows(
    *,
    benchmark_path: Path,
    instruments: dict[str, Instrument],
    fallback_rows: list[dict[str, object]],
) -> list[tuple[str, str, str, Decimal]]:
    payload = json.loads(benchmark_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        if isinstance(payload.get("weights"), list):
            raw_items = list(payload["weights"])
        else:
            raw_items = [{"instrument_key": key, "benchmark_weight": value} for key, value in payload.items()]
    elif isinstance(payload, list):
        raw_items = list(payload)
    else:
        raise ValueError("custom benchmark must be a JSON object or list")
    fallback_map = {str(item["instrument_key"]): item for item in fallback_rows}
    rows: dict[str, tuple[str, str, str, Decimal]] = {}
    for item in raw_items:
        instrument_key = str(item["instrument_key"])
        weight_field = item.get("benchmark_weight", item.get("weight"))
        if weight_field is None:
            raise ValueError(f"benchmark row for {instrument_key} is missing weight")
        weight = Decimal(str(weight_field))
        if weight < _ZERO:
            raise ValueError(f"benchmark row for {instrument_key} has negative weight")
        instrument = instruments.get(instrument_key)
        fallback = fallback_map.get(instrument_key)
        symbol = str(
            item.get("symbol")
            or (instrument.symbol if instrument is not None else None)
            or (fallback["symbol"] if fallback is not None else "UNKNOWN")
        )
        exchange = str(
            item.get("exchange")
            or (instrument.exchange.value if instrument is not None else None)
            or (fallback["exchange"] if fallback is not None else "UNKNOWN")
        )
        current = rows.get(instrument_key)
        if current is None:
            rows[instrument_key] = (instrument_key, symbol, exchange, weight)
        else:
            rows[instrument_key] = (instrument_key, current[1], current[2], current[3] + weight)
    return [rows[key] for key in sorted(rows)]


def _normalize_benchmark_rows(
    *,
    benchmark_run_id: str,
    raw_rows: list[tuple[str, str, str, Decimal]],
    created_at: datetime,
) -> list[BenchmarkWeightRowRecord]:
    total = sum((weight for _, _, _, weight in raw_rows), _ZERO)
    if total <= _ZERO:
        raise ValueError("benchmark weights must sum to a positive value")
    scale = total if total > Decimal("1") else Decimal("1")
    normalized = [
        (instrument_key, symbol, exchange, quantize_weight(weight / scale))
        for instrument_key, symbol, exchange, weight in raw_rows
        if weight > _ZERO
    ]
    normalized.sort(key=lambda item: (-item[3], item[0]))
    return [
        BenchmarkWeightRowRecord(
            benchmark_run_id=benchmark_run_id,
            instrument_key=instrument_key,
            symbol=symbol,
            exchange=exchange,
            benchmark_weight=weight,
            benchmark_rank=rank,
            group_key_optional=None,
            created_at=created_at,
        )
        for rank, (instrument_key, symbol, exchange, weight) in enumerate(normalized, start=1)
    ]


def _default_benchmark_name(*, source_type: BenchmarkSourceType, source_portfolio_analytics_run_id: str) -> str:
    if source_type == BenchmarkSourceType.CUSTOM_WEIGHTS:
        return f"custom_{source_portfolio_analytics_run_id}"
    if source_type == BenchmarkSourceType.EQUAL_WEIGHT_UNION:
        return f"equal_weight_union_{source_portfolio_analytics_run_id}"
    return f"equal_weight_target_universe_{source_portfolio_analytics_run_id}"


def _is_missing_scalar(value: object | None) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    return False


def _optional_int(value: object | None) -> int | None:
    if _is_missing_scalar(value):
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | str | Decimal):
        return int(value)
    if isinstance(value, float):
        return int(value)
    raise TypeError(f"unsupported integer-like benchmark field value: {value!r}")


def _optional_float(value: object | None) -> float | None:
    if _is_missing_scalar(value):
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int | float | str | Decimal):
        return float(value)
    raise TypeError(f"unsupported float-like benchmark field value: {value!r}")


def _resolve_mark_price_end(
    *,
    quantity_end: int,
    market_value_end: Decimal,
    executed_price_reference: Decimal | None,
    previous_close: Decimal | None,
) -> Decimal:
    if quantity_end > 0 and market_value_end > _ZERO:
        return quantize_return(market_value_end / Decimal(quantity_end))
    if executed_price_reference is not None and executed_price_reference > _ZERO:
        return quantize_return(executed_price_reference)
    if previous_close is not None and previous_close > _ZERO:
        return quantize_return(previous_close)
    return Decimal("0.000000")


def _resolve_instrument_return_proxy(
    *,
    mark_price_end: Decimal,
    previous_close: Decimal | None,
) -> Decimal:
    if previous_close is None or previous_close <= _ZERO:
        return Decimal("0.000000")
    return safe_decimal_ratio(mark_price_end - previous_close, previous_close)


def _resolve_symbol(
    *,
    instrument_key: str,
    row: dict[str, object] | None,
    benchmark_row: dict[str, object] | None,
    instruments: dict[str, Instrument],
) -> str:
    if row is not None and row.get("symbol") is not None:
        return str(row["symbol"])
    if benchmark_row is not None and benchmark_row.get("symbol") is not None:
        return str(benchmark_row["symbol"])
    instrument = instruments.get(instrument_key)
    return instrument.symbol if instrument is not None else "UNKNOWN"


def _resolve_exchange(
    *,
    instrument_key: str,
    row: dict[str, object] | None,
    benchmark_row: dict[str, object] | None,
    instruments: dict[str, Instrument],
) -> str:
    if row is not None and row.get("exchange") is not None:
        return str(row["exchange"])
    if benchmark_row is not None and benchmark_row.get("exchange") is not None:
        return str(benchmark_row["exchange"])
    instrument = instruments.get(instrument_key)
    return instrument.exchange.value if instrument is not None else "UNKNOWN"


def _accumulate_group_totals(
    *,
    group_totals: dict[tuple[str, str], dict[str, Decimal]],
    record: BenchmarkPositionRowRecord,
    instruments: dict[str, Instrument],
) -> None:
    instrument = instruments.get(record.instrument_key)
    groups = {
        ("exchange", record.exchange),
        ("board", instrument.board.value if instrument is not None else "unknown"),
        ("instrument_type", instrument.instrument_type.value if instrument is not None else "unknown"),
    }
    for group_key in groups:
        bucket = group_totals[group_key]
        bucket["target_weight_sum"] += record.target_weight
        bucket["executed_weight_sum"] += record.executed_weight
        bucket["benchmark_weight_sum"] += record.benchmark_weight
        bucket["active_weight_sum"] += record.active_weight_executed
        bucket["portfolio_contribution_sum"] += record.portfolio_contribution_proxy
        bucket["benchmark_contribution_sum"] += record.benchmark_contribution_proxy


def _build_group_rows(
    *,
    benchmark_analytics_run_id: str,
    group_totals: dict[tuple[str, str], dict[str, Decimal]],
    created_at: datetime,
) -> list[BenchmarkGroupRowRecord]:
    rows: list[BenchmarkGroupRowRecord] = []
    for (group_type, group_key), totals in sorted(group_totals.items()):
        executed_weight_sum = quantize_weight(totals["executed_weight_sum"])
        benchmark_weight_sum = quantize_weight(totals["benchmark_weight_sum"])
        portfolio_return_proxy = (
            safe_decimal_ratio(totals["portfolio_contribution_sum"], executed_weight_sum)
            if executed_weight_sum > _ZERO
            else Decimal("0.000000")
        )
        benchmark_return_proxy = (
            safe_decimal_ratio(totals["benchmark_contribution_sum"], benchmark_weight_sum)
            if benchmark_weight_sum > _ZERO
            else Decimal("0.000000")
        )
        rows.append(
            BenchmarkGroupRowRecord(
                benchmark_analytics_run_id=benchmark_analytics_run_id,
                group_type=group_type,
                group_key=group_key,
                target_weight_sum=quantize_weight(totals["target_weight_sum"]),
                executed_weight_sum=executed_weight_sum,
                benchmark_weight_sum=benchmark_weight_sum,
                active_weight_sum=quantize_weight(totals["active_weight_sum"]),
                portfolio_return_proxy=portfolio_return_proxy,
                benchmark_return_proxy=benchmark_return_proxy,
                allocation_proxy=quantize_return(
                    (executed_weight_sum - benchmark_weight_sum) * benchmark_return_proxy
                ),
                selection_proxy=quantize_return(
                    executed_weight_sum * (portfolio_return_proxy - benchmark_return_proxy)
                ),
                created_at=created_at,
            )
        )
    return rows
