"""Cross-run benchmark-relative comparison for M13 analytics."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore
from libs.analytics.attribution_config import default_benchmark_analytics_config
from libs.analytics.attribution_schemas import (
    BenchmarkAnalyticsManifest,
    BenchmarkCompareBasis,
    BenchmarkCompareRowRecord,
    BenchmarkCompareRunRecord,
    BenchmarkCompareSummaryRecord,
)
from libs.analytics.benchmark_normalize import quantize_return, quantize_weight
from libs.analytics.benchmark_schemas import BenchmarkRunStatus
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash


def compare_benchmark_analytics(
    *,
    project_root: Path,
    left_benchmark_analytics_run_id: str,
    right_benchmark_analytics_run_id: str,
    compare_basis: str,
    force: bool = False,
) -> dict[str, object]:
    store = BenchmarkAttributionArtifactStore(project_root)
    left_manifest = _find_manifest(store=store, benchmark_analytics_run_id=left_benchmark_analytics_run_id)
    right_manifest = _find_manifest(store=store, benchmark_analytics_run_id=right_benchmark_analytics_run_id)
    if left_manifest.benchmark_run_id != right_manifest.benchmark_run_id:
        raise ValueError("benchmark compare requires the same benchmark_run_id on both sides")
    config_hash = stable_hash(default_benchmark_analytics_config().model_dump(mode="json"))
    basis = BenchmarkCompareBasis(compare_basis)
    compare_run_id = "bcompare_" + stable_hash(
        {
            "left_benchmark_analytics_run_id": left_manifest.benchmark_analytics_run_id,
            "right_benchmark_analytics_run_id": right_manifest.benchmark_analytics_run_id,
            "compare_basis": basis.value,
            "analytics_config_hash": config_hash,
        }
    )[:12]
    if store.has_benchmark_compare_run(benchmark_compare_run_id=compare_run_id):
        existing = store.load_benchmark_compare_manifest(benchmark_compare_run_id=compare_run_id)
        if existing.status.value == "success" and not force:
            summary = store.load_benchmark_compare_summary(benchmark_compare_run_id=compare_run_id)
            return {
                "benchmark_compare_run_id": compare_run_id,
                "row_count": existing.row_count,
                "summary_path": existing.summary_file_path,
                "delta_executed_active_share": summary.delta_executed_active_share,
                "reused": True,
            }
        store.clear_benchmark_compare_run(benchmark_compare_run_id=compare_run_id)
    created_at = ensure_cn_aware(datetime.now())
    run = BenchmarkCompareRunRecord(
        benchmark_compare_run_id=compare_run_id,
        left_benchmark_analytics_run_id=left_manifest.benchmark_analytics_run_id,
        right_benchmark_analytics_run_id=right_manifest.benchmark_analytics_run_id,
        compare_basis=basis,
        analytics_config_hash=config_hash,
        status=left_manifest.status,
        created_at=created_at,
        source_execution_task_ids=_merge_ids(left_manifest.source_execution_task_id, right_manifest.source_execution_task_id),
        source_strategy_run_ids=_merge_ids(left_manifest.source_strategy_run_id, right_manifest.source_strategy_run_id),
        source_prediction_run_ids=_merge_ids(left_manifest.source_prediction_run_id, right_manifest.source_prediction_run_id),
        source_portfolio_analytics_run_ids=_merge_ids(
            left_manifest.source_portfolio_analytics_run_id,
            right_manifest.source_portfolio_analytics_run_id,
        ),
        benchmark_run_ids=[left_manifest.benchmark_run_id],
        source_qlib_export_run_ids=_merge_optional_ids(
            left_manifest.source_qlib_export_run_id,
            right_manifest.source_qlib_export_run_id,
        ),
        source_standard_build_run_ids=_merge_optional_ids(
            left_manifest.source_standard_build_run_id,
            right_manifest.source_standard_build_run_id,
        ),
    )
    run = run.model_copy(update={"status": left_manifest.status})
    store.save_benchmark_compare_run(run)
    try:
        rows, summary = _build_compare_rows_and_summary(
            store=store,
            left_manifest=left_manifest,
            right_manifest=right_manifest,
            compare_run_id=compare_run_id,
            compare_basis=basis,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": BenchmarkRunStatus.SUCCESS})
        manifest = store.save_benchmark_compare_success(run=success_run, rows=rows, summary=summary)
    except Exception as exc:
        failed_run = run.model_copy(update={"status": BenchmarkRunStatus.FAILED})
        store.save_failed_benchmark_compare_run(failed_run, error_message=str(exc))
        raise
    return {
        "benchmark_compare_run_id": compare_run_id,
        "row_count": len(rows),
        "summary_path": manifest.summary_file_path,
        "delta_executed_active_share": summary.delta_executed_active_share,
        "reused": False,
    }


def _build_compare_rows_and_summary(
    *,
    store: BenchmarkAttributionArtifactStore,
    left_manifest: BenchmarkAnalyticsManifest,
    right_manifest: BenchmarkAnalyticsManifest,
    compare_run_id: str,
    compare_basis: BenchmarkCompareBasis,
    created_at: datetime,
) -> tuple[list[BenchmarkCompareRowRecord], BenchmarkCompareSummaryRecord]:
    left_rows_frame = store.load_benchmark_position_rows(
        trade_date=left_manifest.trade_date,
        account_id=left_manifest.account_id,
        basket_id=left_manifest.basket_id,
        benchmark_analytics_run_id=left_manifest.benchmark_analytics_run_id,
    )
    right_rows_frame = store.load_benchmark_position_rows(
        trade_date=right_manifest.trade_date,
        account_id=right_manifest.account_id,
        basket_id=right_manifest.basket_id,
        benchmark_analytics_run_id=right_manifest.benchmark_analytics_run_id,
    )
    left_summary = store.load_benchmark_summary(
        trade_date=left_manifest.trade_date,
        account_id=left_manifest.account_id,
        basket_id=left_manifest.basket_id,
        benchmark_analytics_run_id=left_manifest.benchmark_analytics_run_id,
    )
    right_summary = store.load_benchmark_summary(
        trade_date=right_manifest.trade_date,
        account_id=right_manifest.account_id,
        basket_id=right_manifest.basket_id,
        benchmark_analytics_run_id=right_manifest.benchmark_analytics_run_id,
    )
    left_by_instrument = {
        str(item["instrument_key"]): item for item in left_rows_frame.to_dict(orient="records")
    }
    right_by_instrument = {
        str(item["instrument_key"]): item for item in right_rows_frame.to_dict(orient="records")
    }
    shared = sorted(set(left_by_instrument) & set(right_by_instrument))
    left_only = sorted(set(left_by_instrument) - set(right_by_instrument))
    right_only = sorted(set(right_by_instrument) - set(left_by_instrument))
    rows: list[BenchmarkCompareRowRecord] = []
    for instrument_key in shared:
        left_row = left_by_instrument[instrument_key]
        right_row = right_by_instrument[instrument_key]
        numeric_metrics = {
            "executed_weight": (
                left_row["executed_weight"],
                right_row["executed_weight"],
                quantize_weight(Decimal(str(right_row["executed_weight"])) - Decimal(str(left_row["executed_weight"]))),
            ),
            "active_weight_executed": (
                left_row["active_weight_executed"],
                right_row["active_weight_executed"],
                quantize_weight(
                    Decimal(str(right_row["active_weight_executed"])) - Decimal(str(left_row["active_weight_executed"]))
                ),
            ),
            "portfolio_contribution_proxy": (
                left_row["portfolio_contribution_proxy"],
                right_row["portfolio_contribution_proxy"],
                quantize_return(
                    Decimal(str(right_row["portfolio_contribution_proxy"]))
                    - Decimal(str(left_row["portfolio_contribution_proxy"]))
                ),
            ),
            "active_contribution_proxy": (
                left_row["active_contribution_proxy"],
                right_row["active_contribution_proxy"],
                quantize_return(
                    Decimal(str(right_row["active_contribution_proxy"]))
                    - Decimal(str(left_row["active_contribution_proxy"]))
                ),
            ),
        }
        for metric_name, (left_value, right_value, delta_value) in numeric_metrics.items():
            rows.append(
                BenchmarkCompareRowRecord(
                    benchmark_compare_run_id=compare_run_id,
                    left_benchmark_analytics_run_id=left_manifest.benchmark_analytics_run_id,
                    right_benchmark_analytics_run_id=right_manifest.benchmark_analytics_run_id,
                    instrument_key=instrument_key,
                    symbol=str(left_row["symbol"]),
                    metric_name=metric_name,
                    left_value=str(left_value),
                    right_value=str(right_value),
                    delta_value=str(delta_value),
                    created_at=created_at,
                )
            )
    summary = BenchmarkCompareSummaryRecord(
        benchmark_compare_run_id=compare_run_id,
        left_benchmark_analytics_run_id=left_manifest.benchmark_analytics_run_id,
        right_benchmark_analytics_run_id=right_manifest.benchmark_analytics_run_id,
        compare_basis=compare_basis,
        comparable_count=len(shared),
        delta_executed_active_share=quantize_weight(
            right_summary.executed_active_share - left_summary.executed_active_share
        ),
        delta_active_cash_weight=quantize_weight(
            right_summary.active_cash_weight - left_summary.active_cash_weight
        ),
        delta_total_active_contribution_proxy=quantize_return(
            right_summary.total_active_contribution_proxy - left_summary.total_active_contribution_proxy
        ),
        delta_delta_hhi_concentration=quantize_weight(
            right_summary.delta_hhi_concentration - left_summary.delta_hhi_concentration
        ),
        summary_json={
            "left_only_count": len(left_only),
            "right_only_count": len(right_only),
            "left_only_instruments": left_only,
            "right_only_instruments": right_only,
            "benchmark_run_id": left_manifest.benchmark_run_id,
            "left_source_run_id": left_manifest.source_run_id,
            "right_source_run_id": right_manifest.source_run_id,
            "left_replay_mode": left_rows_frame.iloc[0]["replay_mode"] if len(left_rows_frame.index) else None,
            "right_replay_mode": right_rows_frame.iloc[0]["replay_mode"] if len(right_rows_frame.index) else None,
            "left_fill_model_name": left_rows_frame.iloc[0]["fill_model_name"] if len(left_rows_frame.index) else None,
            "right_fill_model_name": right_rows_frame.iloc[0]["fill_model_name"] if len(right_rows_frame.index) else None,
            "left_time_in_force": left_rows_frame.iloc[0]["time_in_force"] if len(left_rows_frame.index) else None,
            "right_time_in_force": right_rows_frame.iloc[0]["time_in_force"] if len(right_rows_frame.index) else None,
        },
        created_at=created_at,
    )
    return rows, summary


def _find_manifest(
    *,
    store: BenchmarkAttributionArtifactStore,
    benchmark_analytics_run_id: str,
) -> BenchmarkAnalyticsManifest:
    for manifest in store.list_benchmark_analytics_manifests():
        if manifest.benchmark_analytics_run_id == benchmark_analytics_run_id:
            return manifest
    raise FileNotFoundError(f"no benchmark analytics manifest found for {benchmark_analytics_run_id}")


def _merge_ids(left_value: str, right_value: str) -> list[str]:
    if left_value == right_value:
        return [left_value]
    return [left_value, right_value]


def _merge_optional_ids(left_value: str | None, right_value: str | None) -> list[str]:
    values: list[str] = []
    for item in (left_value, right_value):
        if item is None:
            continue
        if item not in values:
            values.append(item)
    return values
