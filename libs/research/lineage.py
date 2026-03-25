"""Research lineage helpers."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from libs.common.time import ensure_cn_aware
from libs.research.artifacts import ResearchArtifactStore
from libs.research.schemas import PredictionLineage, QlibExportLineage


def load_qlib_export_lineage(provider_root: Path, *, freq: str = "day") -> QlibExportLineage:
    candidates = [
        provider_root / f"export_manifest_{freq}.json",
        provider_root / "export_manifest.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if payload.get("created_at"):
            payload["created_at"] = ensure_cn_aware(datetime.fromisoformat(payload["created_at"]))
        return QlibExportLineage.model_validate(payload)
    raise FileNotFoundError(f"qlib export lineage manifest not found under {provider_root}")


def resolve_prediction_lineage(
    store: ResearchArtifactStore,
    *,
    trade_date: date,
    run_id: str,
) -> PredictionLineage:
    run = store.load_run(run_id)
    prediction = store.load_prediction_manifest(trade_date, run_id)
    return PredictionLineage(
        trade_date=trade_date,
        run_id=run_id,
        prediction_path=prediction.file_path,
        prediction_file_hash=prediction.file_hash,
        model_name=run.model_name,
        model_version=run.model_version,
        feature_set_name=run.feature_set_name,
        feature_set_version=run.feature_set_version,
        source_qlib_export_run_id=run.source_qlib_export_run_id,
        source_standard_build_run_id=run.source_standard_build_run_id,
    )
