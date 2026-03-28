"""Cross-run execution comparison for M11 analytics."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from libs.analytics.artifacts import ExecutionAnalyticsArtifactStore
from libs.analytics.config import default_execution_analytics_config
from libs.analytics.loaders import LoadedExecutionSource, select_execution_source
from libs.analytics.normalize import quantize_money
from libs.analytics.schemas import (
    CompareBasis,
    ExecutionAnalyticsStatus,
    ExecutionCompareRowRecord,
    ExecutionCompareRunRecord,
    ExecutionCompareSummaryRecord,
    JsonScalar,
)
from libs.analytics.tca import build_execution_tca_rows
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash


def compare_execution_runs(
    *,
    project_root: Path,
    left_paper_run_id: str | None = None,
    left_shadow_run_id: str | None = None,
    right_paper_run_id: str | None = None,
    right_shadow_run_id: str | None = None,
    compare_basis: str,
    force: bool = False,
) -> dict[str, object]:
    config_hash = stable_hash(default_execution_analytics_config().model_dump(mode="json"))
    left = select_execution_source(
        project_root=project_root,
        paper_run_id=left_paper_run_id,
        shadow_run_id=left_shadow_run_id,
    )
    right = select_execution_source(
        project_root=project_root,
        paper_run_id=right_paper_run_id,
        shadow_run_id=right_shadow_run_id,
    )
    basis = CompareBasis(compare_basis)
    compare_run_id = "compare_" + stable_hash(
        {
            "left_run_id": left.source.source_run_id,
            "right_run_id": right.source.source_run_id,
            "compare_basis": basis.value,
            "analytics_config_hash": config_hash,
        }
    )[:12]
    store = ExecutionAnalyticsArtifactStore(project_root)
    if store.has_compare_run(compare_run_id=compare_run_id):
        existing = store.load_compare_manifest(compare_run_id=compare_run_id)
        if existing.status == ExecutionAnalyticsStatus.SUCCESS and not force:
            summary = store.load_compare_summary(compare_run_id=compare_run_id)
            return {
                "compare_run_id": compare_run_id,
                "left_run_id": left.source.source_run_id,
                "right_run_id": right.source.source_run_id,
                "compare_basis": basis.value,
                "row_count": existing.row_count,
                "summary_path": existing.summary_file_path,
                "delta_fill_rate": summary.delta_fill_rate,
                "reused": True,
            }
        store.clear_compare_run(compare_run_id=compare_run_id)
    created_at = ensure_cn_aware(datetime.now())
    run = ExecutionCompareRunRecord(
        compare_run_id=compare_run_id,
        left_run_id=left.source.source_run_id,
        right_run_id=right.source.source_run_id,
        compare_basis=basis,
        analytics_config_hash=config_hash,
        status=ExecutionAnalyticsStatus.CREATED,
        created_at=created_at,
        source_execution_task_ids=_merge_ids(
            left.source.execution_task_id,
            right.source.execution_task_id,
        ),
        source_strategy_run_ids=_merge_ids(
            left.source.strategy_run_id,
            right.source.strategy_run_id,
        ),
        source_prediction_run_ids=_merge_ids(
            left.source.prediction_run_id,
            right.source.prediction_run_id,
        ),
        source_qlib_export_run_ids=_merge_optional_ids(
            left.source.source_qlib_export_run_id,
            right.source.source_qlib_export_run_id,
        ),
        source_standard_build_run_ids=_merge_optional_ids(
            left.source.source_standard_build_run_id,
            right.source.source_standard_build_run_id,
        ),
    )
    store.save_compare_run(run)
    try:
        rows, summary = _build_compare_rows_and_summary(
            left=left,
            right=right,
            compare_run_id=compare_run_id,
            compare_basis=basis,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": ExecutionAnalyticsStatus.SUCCESS})
        manifest = store.save_compare_success(run=success_run, rows=rows, summary=summary)
    except Exception as exc:
        failed_run = run.model_copy(update={"status": ExecutionAnalyticsStatus.FAILED})
        store.save_failed_compare_run(failed_run, error_message=str(exc))
        raise
    return {
        "compare_run_id": compare_run_id,
        "left_run_id": left.source.source_run_id,
        "right_run_id": right.source.source_run_id,
        "compare_basis": basis.value,
        "row_count": len(rows),
        "summary_path": manifest.summary_file_path,
        "delta_fill_rate": summary.delta_fill_rate,
        "reused": False,
    }


def _build_compare_rows_and_summary(
    *,
    left: LoadedExecutionSource,
    right: LoadedExecutionSource,
    compare_run_id: str,
    compare_basis: CompareBasis,
    created_at: datetime,
) -> tuple[list[ExecutionCompareRowRecord], ExecutionCompareSummaryRecord]:
    left_rows, _ = build_execution_tca_rows(
        loaded=left,
        analytics_run_id=f"compare_left_{compare_run_id}",
        created_at=created_at,
    )
    right_rows, _ = build_execution_tca_rows(
        loaded=right,
        analytics_run_id=f"compare_right_{compare_run_id}",
        created_at=created_at,
    )
    left_by_instrument = {row.instrument_key: row for row in left_rows}
    right_by_instrument = {row.instrument_key: row for row in right_rows}
    shared = sorted(set(left_by_instrument) & set(right_by_instrument))
    left_only = sorted(set(left_by_instrument) - set(right_by_instrument))
    right_only = sorted(set(right_by_instrument) - set(left_by_instrument))
    rows: list[ExecutionCompareRowRecord] = []
    delta_filled_notional = Decimal("0")
    delta_fill_rate_sum = Decimal("0")
    delta_realized_cost = Decimal("0")
    delta_impl_shortfall = Decimal("0")
    for instrument_key in shared:
        left_row = left_by_instrument[instrument_key]
        right_row = right_by_instrument[instrument_key]
        numeric_metrics = {
            "filled_notional": (
                left_row.filled_notional,
                right_row.filled_notional,
                quantize_money(right_row.filled_notional - left_row.filled_notional),
            ),
            "fill_rate": (
                left_row.fill_rate,
                right_row.fill_rate,
                round(right_row.fill_rate - left_row.fill_rate, 4),
            ),
            "realized_cost_total": (
                left_row.realized_cost_total,
                right_row.realized_cost_total,
                quantize_money(right_row.realized_cost_total - left_row.realized_cost_total),
            ),
            "implementation_shortfall": (
                left_row.implementation_shortfall,
                right_row.implementation_shortfall,
                quantize_money(
                    right_row.implementation_shortfall - left_row.implementation_shortfall
                ),
            ),
        }
        for metric_name, (left_value, right_value, delta_value) in numeric_metrics.items():
            rows.append(
                ExecutionCompareRowRecord(
                    compare_run_id=compare_run_id,
                    left_run_id=left.source.source_run_id,
                    right_run_id=right.source.source_run_id,
                    instrument_key=instrument_key,
                    symbol=left_row.symbol,
                    metric_name=metric_name,
                    left_value=str(left_value),
                    right_value=str(right_value),
                    delta_value=str(delta_value),
                    created_at=created_at,
                )
            )
        rows.append(
            ExecutionCompareRowRecord(
                compare_run_id=compare_run_id,
                left_run_id=left.source.source_run_id,
                right_run_id=right.source.source_run_id,
                instrument_key=instrument_key,
                symbol=left_row.symbol,
                metric_name="session_end_status",
                left_value=left_row.session_end_status.value,
                right_value=right_row.session_end_status.value,
                delta_value=None,
                created_at=created_at,
            )
        )
        delta_filled_notional += right_row.filled_notional - left_row.filled_notional
        delta_fill_rate_sum += Decimal(str(round(right_row.fill_rate - left_row.fill_rate, 4)))
        delta_realized_cost += right_row.realized_cost_total - left_row.realized_cost_total
        delta_impl_shortfall += (
            right_row.implementation_shortfall - left_row.implementation_shortfall
        )
    summary = ExecutionCompareSummaryRecord(
        compare_run_id=compare_run_id,
        left_run_id=left.source.source_run_id,
        right_run_id=right.source.source_run_id,
        compare_basis=compare_basis,
        order_count=max(len(left_rows), len(right_rows)),
        comparable_order_count=len(shared),
        delta_filled_notional=quantize_money(delta_filled_notional),
        delta_fill_rate=round(float(delta_fill_rate_sum), 4) if shared else 0.0,
        delta_realized_cost=quantize_money(delta_realized_cost),
        delta_implementation_shortfall=quantize_money(delta_impl_shortfall),
        summary_json={
            "left_only_count": len(left_only),
            "right_only_count": len(right_only),
            "left_only_instruments": _json_list(left_only),
            "right_only_instruments": _json_list(right_only),
            "left_replay_mode": left.source.replay_mode,
            "right_replay_mode": right.source.replay_mode,
            "left_fill_model_name": left.source.fill_model_name,
            "right_fill_model_name": right.source.fill_model_name,
            "left_time_in_force": left.source.time_in_force,
            "right_time_in_force": right.source.time_in_force,
        },
        created_at=created_at,
    )
    return rows, summary


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


def _json_list(values: list[str]) -> list[JsonScalar]:
    return list(values)
