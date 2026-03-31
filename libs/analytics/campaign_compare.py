"""Cross-campaign comparison helpers for M14 walk-forward analytics."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

from libs.analytics.campaign_artifacts import CampaignArtifactStore
from libs.analytics.campaign_config import default_campaign_compare_config
from libs.analytics.campaign_schemas import (
    CampaignCompareBasis,
    CampaignCompareDayRowRecord,
    CampaignCompareRunRecord,
    CampaignCompareSummaryRecord,
    CampaignManifest,
    CampaignStatus,
    JsonScalar,
)
from libs.analytics.portfolio_normalize import quantize_weight
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash

_ZERO = Decimal("0")


def compare_campaigns(
    *,
    project_root: Path,
    left_campaign_run_id: str,
    right_campaign_run_id: str,
    compare_basis: str,
    force: bool = False,
) -> dict[str, object]:
    store = CampaignArtifactStore(project_root)
    left_manifest = _find_campaign_manifest(store=store, campaign_run_id=left_campaign_run_id)
    right_manifest = _find_campaign_manifest(store=store, campaign_run_id=right_campaign_run_id)
    config_hash = stable_hash(default_campaign_compare_config().model_dump(mode="json"))
    basis = CampaignCompareBasis(compare_basis)
    campaign_compare_run_id = "ccompare_" + stable_hash(
        {
            "left_campaign_run_id": left_campaign_run_id,
            "right_campaign_run_id": right_campaign_run_id,
            "left_model_schedule_run_id": left_manifest.model_schedule_run_id,
            "right_model_schedule_run_id": right_manifest.model_schedule_run_id,
            "compare_basis": basis.value,
            "compare_config_hash": config_hash,
        }
    )[:12]
    if store.has_campaign_compare_run(campaign_compare_run_id=campaign_compare_run_id):
        existing = store.load_campaign_compare_manifest(
            campaign_compare_run_id=campaign_compare_run_id
        )
        if existing.status == CampaignStatus.SUCCESS and not force:
            summary = store.load_campaign_compare_summary(
                campaign_compare_run_id=campaign_compare_run_id
            )
            return {
                "campaign_compare_run_id": campaign_compare_run_id,
                "overlapping_day_count": summary.overlapping_day_count,
                "summary_path": existing.summary_file_path,
                "delta_average_fill_rate": summary.delta_average_fill_rate,
                "reused": True,
            }
        store.clear_campaign_compare_run(campaign_compare_run_id=campaign_compare_run_id)
    created_at = ensure_cn_aware(datetime.now())
    left_day_rows = store.load_day_rows(
        date_start=left_manifest.date_start,
        date_end=left_manifest.date_end,
        account_id=left_manifest.account_id,
        basket_id=left_manifest.basket_id,
        campaign_run_id=left_manifest.campaign_run_id,
    )
    right_day_rows = store.load_day_rows(
        date_start=right_manifest.date_start,
        date_end=right_manifest.date_end,
        account_id=right_manifest.account_id,
        basket_id=right_manifest.basket_id,
        campaign_run_id=right_manifest.campaign_run_id,
    )
    run = CampaignCompareRunRecord(
        campaign_compare_run_id=campaign_compare_run_id,
        left_campaign_run_id=left_manifest.campaign_run_id,
        right_campaign_run_id=right_manifest.campaign_run_id,
        compare_basis=basis,
        compare_config_hash=config_hash,
        status=CampaignStatus.CREATED,
        created_at=created_at,
        source_model_run_ids=_merge_unique(
            left_day_rows["model_run_id"].tolist(),
            right_day_rows["model_run_id"].tolist(),
        ),
        source_model_schedule_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "model_schedule_run_id"),
            _non_null_series(right_day_rows, "model_schedule_run_id"),
        ),
        source_prediction_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "prediction_run_id"),
            _non_null_series(right_day_rows, "prediction_run_id"),
        ),
        source_strategy_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "strategy_run_id"),
            _non_null_series(right_day_rows, "strategy_run_id"),
        ),
        source_execution_task_ids=_merge_unique(
            _non_null_series(left_day_rows, "execution_task_id"),
            _non_null_series(right_day_rows, "execution_task_id"),
        ),
        source_paper_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "paper_run_id"),
            _non_null_series(right_day_rows, "paper_run_id"),
        ),
        source_shadow_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "shadow_run_id"),
            _non_null_series(right_day_rows, "shadow_run_id"),
        ),
        source_execution_analytics_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "execution_analytics_run_id"),
            _non_null_series(right_day_rows, "execution_analytics_run_id"),
        ),
        source_portfolio_analytics_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "portfolio_analytics_run_id"),
            _non_null_series(right_day_rows, "portfolio_analytics_run_id"),
        ),
        source_benchmark_analytics_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "benchmark_analytics_run_id"),
            _non_null_series(right_day_rows, "benchmark_analytics_run_id"),
        ),
        source_qlib_export_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "source_qlib_export_run_id"),
            _non_null_series(right_day_rows, "source_qlib_export_run_id"),
        ),
        source_standard_build_run_ids=_merge_unique(
            _non_null_series(left_day_rows, "source_standard_build_run_id"),
            _non_null_series(right_day_rows, "source_standard_build_run_id"),
        ),
    )
    store.save_campaign_compare_run(run)
    try:
        rows, summary = _build_compare_rows_and_summary(
            store=store,
            left_manifest=left_manifest,
            right_manifest=right_manifest,
            campaign_compare_run_id=campaign_compare_run_id,
            compare_basis=basis,
            created_at=created_at,
        )
        success_run = run.model_copy(update={"status": CampaignStatus.SUCCESS})
        manifest = store.save_campaign_compare_success(
            run=success_run,
            rows=rows,
            summary=summary,
        )
    except Exception as exc:
        failed_run = run.model_copy(update={"status": CampaignStatus.FAILED})
        store.save_failed_campaign_compare_run(failed_run, error_message=str(exc))
        raise
    return {
        "campaign_compare_run_id": campaign_compare_run_id,
        "overlapping_day_count": summary.overlapping_day_count,
        "summary_path": manifest.summary_file_path,
        "delta_average_fill_rate": summary.delta_average_fill_rate,
        "reused": False,
    }


def _build_compare_rows_and_summary(
    *,
    store: CampaignArtifactStore,
    left_manifest: CampaignManifest,
    right_manifest: CampaignManifest,
    campaign_compare_run_id: str,
    compare_basis: CampaignCompareBasis,
    created_at: datetime,
) -> tuple[list[CampaignCompareDayRowRecord], CampaignCompareSummaryRecord]:
    left_rows = store.load_timeseries_rows(
        date_start=left_manifest.date_start,
        date_end=left_manifest.date_end,
        account_id=left_manifest.account_id,
        basket_id=left_manifest.basket_id,
        campaign_run_id=left_manifest.campaign_run_id,
    )
    right_rows = store.load_timeseries_rows(
        date_start=right_manifest.date_start,
        date_end=right_manifest.date_end,
        account_id=right_manifest.account_id,
        basket_id=right_manifest.basket_id,
        campaign_run_id=right_manifest.campaign_run_id,
    )
    left_summary = store.load_campaign_summary(
        date_start=left_manifest.date_start,
        date_end=left_manifest.date_end,
        account_id=left_manifest.account_id,
        basket_id=left_manifest.basket_id,
        campaign_run_id=left_manifest.campaign_run_id,
    )
    right_summary = store.load_campaign_summary(
        date_start=right_manifest.date_start,
        date_end=right_manifest.date_end,
        account_id=right_manifest.account_id,
        basket_id=right_manifest.basket_id,
        campaign_run_id=right_manifest.campaign_run_id,
    )
    left_by_date = {item["trade_date"]: item for item in left_rows.to_dict(orient="records")}
    right_by_date = {item["trade_date"]: item for item in right_rows.to_dict(orient="records")}
    overlapping_dates = sorted(set(left_by_date) & set(right_by_date))
    left_only = sorted(set(left_by_date) - set(right_by_date))
    right_only = sorted(set(right_by_date) - set(left_by_date))
    rows: list[CampaignCompareDayRowRecord] = []
    for trade_date in overlapping_dates:
        left_row = left_by_date[trade_date]
        right_row = right_by_date[trade_date]
        left_active_share = _optional_decimal(left_row.get("daily_active_share"))
        right_active_share = _optional_decimal(right_row.get("daily_active_share"))
        left_active_contribution = _optional_decimal(left_row.get("daily_active_contribution_proxy"))
        right_active_contribution = _optional_decimal(right_row.get("daily_active_contribution_proxy"))
        left_model_age_trade_days = _optional_int(left_row.get("daily_model_age_trade_days"))
        right_model_age_trade_days = _optional_int(right_row.get("daily_model_age_trade_days"))
        rows.append(
            CampaignCompareDayRowRecord(
                campaign_compare_run_id=campaign_compare_run_id,
                trade_date=trade_date,
                left_campaign_run_id=left_manifest.campaign_run_id,
                right_campaign_run_id=right_manifest.campaign_run_id,
                left_net_liquidation_end=Decimal(str(left_row["net_liquidation_end"])),
                right_net_liquidation_end=Decimal(str(right_row["net_liquidation_end"])),
                delta_net_liquidation_end=(
                    Decimal(str(right_row["net_liquidation_end"]))
                    - Decimal(str(left_row["net_liquidation_end"]))
                ).quantize(Decimal("0.01")),
                left_fill_rate=float(left_row["daily_fill_rate"]),
                right_fill_rate=float(right_row["daily_fill_rate"]),
                delta_fill_rate=round(
                    float(right_row["daily_fill_rate"]) - float(left_row["daily_fill_rate"]),
                    4,
                ),
                left_active_share=left_active_share,
                right_active_share=right_active_share,
                delta_active_share=(
                    quantize_weight(right_active_share - left_active_share)
                    if left_active_share is not None and right_active_share is not None
                    else None
                ),
                left_top5_concentration=Decimal(str(left_row["daily_top5_concentration"])),
                right_top5_concentration=Decimal(str(right_row["daily_top5_concentration"])),
                delta_top5_concentration=quantize_weight(
                    Decimal(str(right_row["daily_top5_concentration"]))
                    - Decimal(str(left_row["daily_top5_concentration"]))
                ),
                left_active_contribution_proxy=left_active_contribution,
                right_active_contribution_proxy=right_active_contribution,
                delta_active_contribution_proxy=(
                    (right_active_contribution - left_active_contribution).quantize(Decimal("0.000001"))
                    if left_active_contribution is not None and right_active_contribution is not None
                    else None
                ),
                left_model_age_trade_days=left_model_age_trade_days,
                right_model_age_trade_days=right_model_age_trade_days,
                delta_model_age_trade_days=(
                    right_model_age_trade_days - left_model_age_trade_days
                    if left_model_age_trade_days is not None and right_model_age_trade_days is not None
                    else None
                ),
                created_at=created_at,
            )
        )
    summary_json = cast(
        dict[str, JsonScalar | Sequence[JsonScalar]],
        {
            "left_only_trade_dates": [item.isoformat() for item in left_only],
            "right_only_trade_dates": [item.isoformat() for item in right_only],
            "left_only_count": len(left_only),
            "right_only_count": len(right_only),
            "left_market_replay_mode": left_manifest.market_replay_mode,
            "right_market_replay_mode": right_manifest.market_replay_mode,
            "left_tick_fill_model": left_manifest.tick_fill_model,
            "right_tick_fill_model": right_manifest.tick_fill_model,
            "left_time_in_force": left_manifest.time_in_force,
            "right_time_in_force": right_manifest.time_in_force,
            "left_benchmark_enabled": left_manifest.benchmark_enabled,
            "right_benchmark_enabled": right_manifest.benchmark_enabled,
            "left_model_schedule_run_id": left_manifest.model_schedule_run_id,
            "right_model_schedule_run_id": right_manifest.model_schedule_run_id,
        },
    )
    summary = CampaignCompareSummaryRecord(
        campaign_compare_run_id=campaign_compare_run_id,
        left_campaign_run_id=left_manifest.campaign_run_id,
        right_campaign_run_id=right_manifest.campaign_run_id,
        compare_basis=compare_basis,
        overlapping_day_count=len(overlapping_dates),
        delta_net_liquidation_end=(
            right_summary.net_liquidation_end - left_summary.net_liquidation_end
        ).quantize(Decimal("0.01")),
        delta_cumulative_realized_pnl=(
            right_summary.cumulative_realized_pnl - left_summary.cumulative_realized_pnl
        ).quantize(Decimal("0.01")),
        delta_cumulative_realized_cost=(
            right_summary.cumulative_realized_cost - left_summary.cumulative_realized_cost
        ).quantize(Decimal("0.01")),
        delta_average_fill_rate=round(
            right_summary.average_fill_rate - left_summary.average_fill_rate,
            4,
        ),
        delta_average_turnover=quantize_weight(
            right_summary.average_turnover - left_summary.average_turnover
        ),
        delta_final_active_share=(
            quantize_weight(right_summary.final_active_share - left_summary.final_active_share)
            if right_summary.final_active_share is not None and left_summary.final_active_share is not None
            else None
        ),
        delta_final_top5_concentration=quantize_weight(
            right_summary.final_top5_concentration - left_summary.final_top5_concentration
        ),
        delta_max_drawdown=quantize_weight(right_summary.max_drawdown - left_summary.max_drawdown),
        delta_cumulative_active_contribution_proxy=(
            (right_summary.cumulative_active_contribution_proxy - left_summary.cumulative_active_contribution_proxy).quantize(Decimal("0.000001"))
            if right_summary.cumulative_active_contribution_proxy is not None
            and left_summary.cumulative_active_contribution_proxy is not None
            else None
        ),
        delta_unique_model_count=right_summary.unique_model_count - left_summary.unique_model_count,
        delta_retrain_count=right_summary.retrain_count - left_summary.retrain_count,
        delta_average_model_age_trade_days=(
            quantize_weight(
                right_summary.average_model_age_trade_days - left_summary.average_model_age_trade_days
            )
            if right_summary.average_model_age_trade_days is not None
            and left_summary.average_model_age_trade_days is not None
            else None
        ),
        summary_json=summary_json,
        created_at=created_at,
    )
    return rows, summary


def _find_campaign_manifest(
    *, store: CampaignArtifactStore, campaign_run_id: str
) -> CampaignManifest:
    for manifest in store.list_campaign_manifests():
        if manifest.campaign_run_id == campaign_run_id:
            return manifest
    raise FileNotFoundError(f"no campaign manifest found for {campaign_run_id}")


def _merge_unique(left_values: list[str], right_values: list[str]) -> list[str]:
    results: list[str] = []
    for value in [*left_values, *right_values]:
        if value and value not in results:
            results.append(value)
    return results


def _non_null_series(frame: Any, column: str) -> list[str]:
    return [str(item) for item in frame[column].tolist() if item is not None]


def _optional_decimal(value: object | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


def _optional_int(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | str | Decimal):
        return int(value)
    if isinstance(value, float):
        return int(value)
    raise TypeError(f"unsupported integer-like compare field value: {value!r}")
