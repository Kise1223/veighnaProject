"""Cross-run portfolio analytics comparison for M12."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from libs.analytics.portfolio import run_portfolio_analytics
from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.analytics.portfolio_config import default_portfolio_analytics_config
from libs.analytics.portfolio_loaders import LoadedPortfolioSource, select_portfolio_source
from libs.analytics.portfolio_normalize import quantize_weight
from libs.analytics.portfolio_schemas import (
    PortfolioAnalyticsStatus,
    PortfolioCompareBasis,
    PortfolioCompareRowRecord,
    PortfolioCompareRunRecord,
    PortfolioCompareSummaryRecord,
)
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash


def compare_portfolios(
    *,
    project_root: Path,
    left_paper_run_id: str | None = None,
    left_shadow_run_id: str | None = None,
    right_paper_run_id: str | None = None,
    right_shadow_run_id: str | None = None,
    compare_basis: str,
    force: bool = False,
) -> dict[str, object]:
    config_hash = stable_hash(default_portfolio_analytics_config().model_dump(mode="json"))
    left_source = select_portfolio_source(
        project_root=project_root,
        paper_run_id=left_paper_run_id,
        shadow_run_id=left_shadow_run_id,
    )
    right_source = select_portfolio_source(
        project_root=project_root,
        paper_run_id=right_paper_run_id,
        shadow_run_id=right_shadow_run_id,
    )
    left_result = run_portfolio_analytics(
        project_root=project_root,
        paper_run_id=left_paper_run_id,
        shadow_run_id=left_shadow_run_id,
    )
    right_result = run_portfolio_analytics(
        project_root=project_root,
        paper_run_id=right_paper_run_id,
        shadow_run_id=right_shadow_run_id,
    )
    basis = PortfolioCompareBasis(compare_basis)
    compare_run_id = "pcompare_" + stable_hash(
        {
            "left_run_id": left_source.source_run_id,
            "right_run_id": right_source.source_run_id,
            "compare_basis": basis.value,
            "analytics_config_hash": config_hash,
        }
    )[:12]
    store = PortfolioAnalyticsArtifactStore(project_root)
    if store.has_compare_run(portfolio_compare_run_id=compare_run_id):
        existing = store.load_compare_manifest(portfolio_compare_run_id=compare_run_id)
        if existing.status == PortfolioAnalyticsStatus.SUCCESS and not force:
            summary = store.load_compare_summary(portfolio_compare_run_id=compare_run_id)
            return {
                "portfolio_compare_run_id": compare_run_id,
                "left_run_id": left_source.source_run_id,
                "right_run_id": right_source.source_run_id,
                "compare_basis": basis.value,
                "row_count": existing.row_count,
                "summary_path": existing.summary_file_path,
                "delta_fill_rate": summary.delta_fill_rate,
                "reused": True,
            }
        store.clear_compare_run(portfolio_compare_run_id=compare_run_id)
    created_at = ensure_cn_aware(datetime.now())
    run = PortfolioCompareRunRecord(
        portfolio_compare_run_id=compare_run_id,
        left_run_id=left_source.source_run_id,
        right_run_id=right_source.source_run_id,
        compare_basis=basis,
        analytics_config_hash=config_hash,
        status=PortfolioAnalyticsStatus.CREATED,
        created_at=created_at,
        source_execution_task_ids=_merge_ids(
            left_source.source_execution_task_id, right_source.source_execution_task_id
        ),
        source_strategy_run_ids=_merge_ids(
            left_source.source_strategy_run_id, right_source.source_strategy_run_id
        ),
        source_prediction_run_ids=_merge_ids(
            left_source.source_prediction_run_id, right_source.source_prediction_run_id
        ),
        source_qlib_export_run_ids=_merge_optional_ids(
            left_source.source_qlib_export_run_id, right_source.source_qlib_export_run_id
        ),
        source_standard_build_run_ids=_merge_optional_ids(
            left_source.source_standard_build_run_id, right_source.source_standard_build_run_id
        ),
    )
    store.save_compare_run(run)
    try:
        rows, summary = _build_compare_rows_and_summary(
            store=store,
            left_source=left_source,
            right_source=right_source,
            left_analytics_run_id=str(left_result["portfolio_analytics_run_id"]),
            right_analytics_run_id=str(right_result["portfolio_analytics_run_id"]),
            compare_run_id=compare_run_id,
            compare_basis=basis,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": PortfolioAnalyticsStatus.SUCCESS})
        manifest = store.save_compare_success(run=success_run, rows=rows, summary=summary)
    except Exception as exc:
        failed_run = run.model_copy(update={"status": PortfolioAnalyticsStatus.FAILED})
        store.save_failed_compare_run(failed_run, error_message=str(exc))
        raise
    return {
        "portfolio_compare_run_id": compare_run_id,
        "left_run_id": left_source.source_run_id,
        "right_run_id": right_source.source_run_id,
        "compare_basis": basis.value,
        "row_count": len(rows),
        "summary_path": manifest.summary_file_path,
        "delta_fill_rate": summary.delta_fill_rate,
        "reused": False,
    }


def _build_compare_rows_and_summary(
    *,
    store: PortfolioAnalyticsArtifactStore,
    left_source: LoadedPortfolioSource,
    right_source: LoadedPortfolioSource,
    left_analytics_run_id: str,
    right_analytics_run_id: str,
    compare_run_id: str,
    compare_basis: PortfolioCompareBasis,
    created_at: datetime,
) -> tuple[list[PortfolioCompareRowRecord], PortfolioCompareSummaryRecord]:
    left_rows_frame = store.load_position_rows(
        trade_date=left_source.trade_date,
        account_id=left_source.account_id,
        basket_id=left_source.basket_id,
        portfolio_analytics_run_id=left_analytics_run_id,
    )
    right_rows_frame = store.load_position_rows(
        trade_date=right_source.trade_date,
        account_id=right_source.account_id,
        basket_id=right_source.basket_id,
        portfolio_analytics_run_id=right_analytics_run_id,
    )
    left_summary = store.load_portfolio_summary(
        trade_date=left_source.trade_date,
        account_id=left_source.account_id,
        basket_id=left_source.basket_id,
        portfolio_analytics_run_id=left_analytics_run_id,
    )
    right_summary = store.load_portfolio_summary(
        trade_date=right_source.trade_date,
        account_id=right_source.account_id,
        basket_id=right_source.basket_id,
        portfolio_analytics_run_id=right_analytics_run_id,
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
    rows: list[PortfolioCompareRowRecord] = []
    for instrument_key in shared:
        left_row = left_by_instrument[instrument_key]
        right_row = right_by_instrument[instrument_key]
        numeric_metrics = {
            "executed_weight": (
                left_row["executed_weight"],
                right_row["executed_weight"],
                quantize_weight(right_row["executed_weight"] - left_row["executed_weight"]),
            ),
            "weight_drift": (
                left_row["weight_drift"],
                right_row["weight_drift"],
                quantize_weight(right_row["weight_drift"] - left_row["weight_drift"]),
            ),
            "filled_notional": (
                left_row["filled_notional"],
                right_row["filled_notional"],
                (right_row["filled_notional"] - left_row["filled_notional"]).quantize(Decimal("0.01")),
            ),
            "fill_rate": (
                left_row["fill_rate"],
                right_row["fill_rate"],
                round(float(right_row["fill_rate"] - left_row["fill_rate"]), 4),
            ),
            "realized_pnl": (
                left_row["realized_pnl"],
                right_row["realized_pnl"],
                (right_row["realized_pnl"] - left_row["realized_pnl"]).quantize(Decimal("0.01")),
            ),
            "unrealized_pnl": (
                left_row["unrealized_pnl"],
                right_row["unrealized_pnl"],
                (right_row["unrealized_pnl"] - left_row["unrealized_pnl"]).quantize(Decimal("0.01")),
            ),
        }
        for metric_name, (left_value, right_value, delta_value) in numeric_metrics.items():
            rows.append(
                PortfolioCompareRowRecord(
                    portfolio_compare_run_id=compare_run_id,
                    left_run_id=left_source.source_run_id,
                    right_run_id=right_source.source_run_id,
                    instrument_key=instrument_key,
                    symbol=str(left_row["symbol"]),
                    metric_name=metric_name,
                    left_value=str(left_value),
                    right_value=str(right_value),
                    delta_value=str(delta_value),
                    created_at=created_at,
                )
            )
        rows.append(
            PortfolioCompareRowRecord(
                portfolio_compare_run_id=compare_run_id,
                left_run_id=left_source.source_run_id,
                right_run_id=right_source.source_run_id,
                instrument_key=instrument_key,
                symbol=str(left_row["symbol"]),
                metric_name="session_end_status",
                left_value=left_row.get("session_end_status"),
                right_value=right_row.get("session_end_status"),
                delta_value=None,
                created_at=created_at,
            )
        )
    summary = PortfolioCompareSummaryRecord(
        portfolio_compare_run_id=compare_run_id,
        left_run_id=left_source.source_run_id,
        right_run_id=right_source.source_run_id,
        compare_basis=compare_basis,
        comparable_count=len(shared),
        delta_cash_weight=quantize_weight(
            right_summary.executed_cash_weight - left_summary.executed_cash_weight
        ),
        delta_fill_rate=round(right_summary.fill_rate_gross - left_summary.fill_rate_gross, 4),
        delta_weight_drift_l1=quantize_weight(
            right_summary.total_weight_drift_l1 - left_summary.total_weight_drift_l1
        ),
        delta_top1_concentration=quantize_weight(
            right_summary.top1_concentration - left_summary.top1_concentration
        ),
        delta_top5_concentration=quantize_weight(
            right_summary.top5_concentration - left_summary.top5_concentration
        ),
        delta_hhi_concentration=quantize_weight(
            right_summary.hhi_concentration - left_summary.hhi_concentration
        ),
        delta_realized_turnover=quantize_weight(
            right_summary.realized_turnover - left_summary.realized_turnover
        ),
        delta_net_liquidation_end=(
            right_summary.net_liquidation_end - left_summary.net_liquidation_end
        ).quantize(Decimal("0.01")),
        delta_total_realized_pnl=(
            right_summary.total_realized_pnl - left_summary.total_realized_pnl
        ).quantize(Decimal("0.01")),
        delta_total_unrealized_pnl=(
            right_summary.total_unrealized_pnl - left_summary.total_unrealized_pnl
        ).quantize(Decimal("0.01")),
        summary_json={
            "left_only_count": len(left_only),
            "right_only_count": len(right_only),
            "left_only_instruments": left_only,
            "right_only_instruments": right_only,
            "left_replay_mode": left_source.replay_mode,
            "right_replay_mode": right_source.replay_mode,
            "left_fill_model_name": left_source.fill_model_name,
            "right_fill_model_name": right_source.fill_model_name,
            "left_time_in_force": left_source.time_in_force,
            "right_time_in_force": right_source.time_in_force,
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
