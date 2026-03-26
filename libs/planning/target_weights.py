"""Target weight builder for M6 dry-run bridge."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.config import load_target_weight_config
from libs.planning.schemas import (
    ApprovedTargetWeightManifest,
    ApprovedTargetWeightRecord,
    ApprovedTargetWeightStatus,
)
from libs.research.artifacts import PredictionManifest, ResearchArtifactStore
from libs.research.lineage import resolve_prediction_lineage

DEFAULT_TARGET_WEIGHT_CONFIG = Path("configs/planning/target_weight_baseline.yaml")


def build_target_weights(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    approved_by: str,
    prediction_run_id: str | None = None,
    config_path: Path = DEFAULT_TARGET_WEIGHT_CONFIG,
    force: bool = False,
) -> dict[str, object]:
    config = load_target_weight_config(project_root / config_path)
    research_store = ResearchArtifactStore(project_root, project_root / "data" / "research")
    planning_store = PlanningArtifactStore(project_root)
    prediction_manifest = _resolve_prediction_manifest(
        research_store,
        trade_date=trade_date,
        prediction_run_id=prediction_run_id,
    )
    resolved_prediction_run_id = prediction_manifest.run_id
    prediction_lineage = resolve_prediction_lineage(
        research_store,
        trade_date=trade_date,
        run_id=resolved_prediction_run_id,
    )
    run = research_store.load_run(resolved_prediction_run_id)
    prediction_frame = research_store.load_predictions(trade_date, resolved_prediction_run_id)
    config_hash = stable_hash(config.model_dump(mode="json"))
    strategy_run_id = (
        "strategy_"
        + stable_hash(
            {
                "trade_date": trade_date,
                "prediction_run_id": resolved_prediction_run_id,
                "account_id": account_id,
                "basket_id": basket_id,
                "config_hash": config_hash,
            }
        )[:12]
    )
    if planning_store.has_target_weight(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        strategy_run_id=strategy_run_id,
    ) and not force:
        manifest = planning_store.load_target_weight_manifest(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            strategy_run_id=strategy_run_id,
        )
        return {
            "strategy_run_id": manifest.strategy_run_id,
            "prediction_run_id": manifest.prediction_run_id,
            "row_count": manifest.row_count,
            "file_path": manifest.file_path,
            "reused": True,
        }
    if force:
        planning_store.clear_target_weights(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            strategy_run_id=strategy_run_id,
        )

    created_at = ensure_cn_aware(datetime.now())
    ranked = _rank_predictions(prediction_frame)
    selected = ranked.head(config.max_names)
    weights = _compute_weights(
        selected,
        max_weight_per_name=config.max_weight_per_name,
        cash_buffer=config.cash_buffer,
        weighting=config.weighting,
    )
    records: list[ApprovedTargetWeightRecord] = []
    for row in selected.to_dict(orient="records"):
        target_weight = weights.get(str(row["instrument_key"]), Decimal("0"))
        if target_weight < 0:
            continue
        records.append(
            ApprovedTargetWeightRecord(
                strategy_run_id=strategy_run_id,
                prediction_run_id=resolved_prediction_run_id,
                account_id=account_id,
                basket_id=basket_id,
                trade_date=trade_date,
                instrument_key=str(row["instrument_key"]),
                qlib_symbol=str(row["qlib_symbol"]),
                score=float(row["score"]),
                rank=int(row["rank"]),
                target_weight=target_weight,
                status=ApprovedTargetWeightStatus.APPROVED,
                approved_by=approved_by,
                approved_at=created_at,
                model_version=run.model_version,
                feature_set_version=run.feature_set_version,
                config_hash=config_hash,
                source_qlib_export_run_id=run.source_qlib_export_run_id,
                source_standard_build_run_id=run.source_standard_build_run_id,
                created_at=created_at,
            )
        )
    draft_manifest = ApprovedTargetWeightManifest(
        strategy_run_id=strategy_run_id,
        prediction_run_id=resolved_prediction_run_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        row_count=len(records),
        status=ApprovedTargetWeightStatus.APPROVED,
        approved_by=approved_by,
        approved_at=created_at,
        model_version=run.model_version,
        feature_set_version=run.feature_set_version,
        config_hash=config_hash,
        source_qlib_export_run_id=run.source_qlib_export_run_id,
        source_standard_build_run_id=run.source_standard_build_run_id,
        created_at=created_at,
        file_path="pending",
        file_hash="pending",
        prediction_path=prediction_lineage.prediction_path,
        prediction_file_hash=prediction_lineage.prediction_file_hash,
    )
    manifest = planning_store.save_target_weights(manifest=draft_manifest, records=records)
    return {
        "strategy_run_id": strategy_run_id,
        "prediction_run_id": resolved_prediction_run_id,
        "row_count": len(records),
        "file_path": manifest.file_path,
        "reused": False,
    }


def _resolve_prediction_manifest(
    store: ResearchArtifactStore,
    *,
    trade_date: date,
    prediction_run_id: str | None,
) -> PredictionManifest:
    if prediction_run_id is not None:
        return store.load_prediction_manifest(trade_date, prediction_run_id)
    for manifest in store.list_prediction_manifests():
        if manifest.trade_date == trade_date:
            return manifest
    raise FileNotFoundError(
        f"no prediction artifact found for trade_date={trade_date.isoformat()}"
    )


def _rank_predictions(frame: Any) -> Any:
    ordered = frame.sort_values(["score", "qlib_symbol"], ascending=[False, True]).reset_index(
        drop=True
    )
    ordered["rank"] = ordered.index + 1
    return ordered


def _compute_weights(
    frame: Any,
    *,
    max_weight_per_name: Decimal,
    cash_buffer: Decimal,
    weighting: str,
) -> dict[str, Decimal]:
    allocatable = max(Decimal("0"), Decimal("1") - cash_buffer)
    if frame.empty or allocatable == 0:
        return {}
    instrument_keys = [str(item) for item in frame["instrument_key"].tolist()]
    if weighting == "score":
        non_negative = [
            max(Decimal(str(value)), Decimal("0")) for value in frame["score"].tolist()
        ]
        total_score = sum(non_negative, Decimal("0"))
        if total_score > 0:
            return {
                key: min((score / total_score) * allocatable, max_weight_per_name).quantize(
                    Decimal("0.0001")
                )
                for key, score in zip(instrument_keys, non_negative, strict=True)
            }
    equal_weight = min(allocatable / Decimal(len(instrument_keys)), max_weight_per_name)
    return {key: equal_weight.quantize(Decimal("0.0001")) for key in instrument_keys}
