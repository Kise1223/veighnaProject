"""File-first artifact persistence for M5 research workflows."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from libs.marketdata.raw_store import (
    file_sha256,
    relative_path,
    require_parquet_support,
    stable_hash,
)
from libs.research.schemas import ArtifactFile, ModelRunRecord, PredictionManifest, PredictionRecord


class ResearchArtifactStore:
    """Persist research runs and predictions under a local file-first root."""

    def __init__(self, project_root: Path, root: Path) -> None:
        self.project_root = project_root
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "model_runs").mkdir(exist_ok=True)
        (self.root / "predictions").mkdir(exist_ok=True)

    def run_dir(self, run_id: str) -> Path:
        return self.root / "model_runs" / run_id

    def has_run(self, run_id: str) -> bool:
        return self.run_record_path(run_id).exists()

    def run_record_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "model_run.json"

    def save_run(self, run: ModelRunRecord) -> Path:
        target_dir = self.run_dir(run.run_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = self.run_record_path(run.run_id)
        target_path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return target_path

    def load_run(self, run_id: str) -> ModelRunRecord:
        return ModelRunRecord.model_validate_json(
            self.run_record_path(run_id).read_text(encoding="utf-8")
        )

    def list_runs(self) -> list[ModelRunRecord]:
        records: list[ModelRunRecord] = []
        for path in sorted((self.root / "model_runs").glob("*/model_run.json")):
            records.append(ModelRunRecord.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(records, key=lambda item: item.created_at, reverse=True)

    def write_json_artifact(self, run_id: str, name: str, payload: Any) -> ArtifactFile:
        target_path = self.run_dir(run_id) / name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return ArtifactFile(
            name=name,
            relative_path=relative_path(self.project_root, target_path),
            file_hash=file_sha256(target_path),
        )

    def write_parquet_artifact(
        self,
        run_id: str,
        name: str,
        rows: list[dict[str, object]],
    ) -> ArtifactFile:
        pd = require_parquet_support()
        target_path = self.run_dir(run_id) / name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(target_path, index=False)
        return ArtifactFile(
            name=name,
            relative_path=relative_path(self.project_root, target_path),
            file_hash=file_sha256(target_path),
        )

    def finalize_artifact_hash(self, files: list[ArtifactFile]) -> str:
        return stable_hash({item.name: item.file_hash for item in files})

    def prediction_dir(self, trade_date: date, run_id: str) -> Path:
        return self.root / "predictions" / f"trade_date={trade_date.isoformat()}" / f"run_id={run_id}"

    def prediction_manifest_path(self, trade_date: date, run_id: str) -> Path:
        return self.prediction_dir(trade_date, run_id) / "prediction_manifest.json"

    def has_prediction(self, trade_date: date, run_id: str) -> bool:
        return self.prediction_manifest_path(trade_date, run_id).exists()

    def save_predictions(
        self,
        *,
        trade_date: date,
        run_id: str,
        model_version: str,
        feature_set_version: str,
        records: list[PredictionRecord],
    ) -> PredictionManifest:
        if not records:
            raise ValueError("prediction records must not be empty")
        pd = require_parquet_support()
        target_dir = self.prediction_dir(trade_date, run_id)
        target_dir.mkdir(parents=True, exist_ok=True)
        frame_path = target_dir / "predictions.parquet"
        frame = pd.DataFrame([record.model_dump(mode="json") for record in records])
        frame.to_parquet(frame_path, index=False)
        manifest = PredictionManifest(
            trade_date=trade_date,
            run_id=run_id,
            model_version=model_version,
            feature_set_version=feature_set_version,
            row_count=len(records),
            file_path=relative_path(self.project_root, frame_path),
            file_hash=file_sha256(frame_path),
            created_at=records[0].created_at,
        )
        manifest_path = self.prediction_manifest_path(trade_date, run_id)
        manifest_path.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_prediction_manifest(self, trade_date: date, run_id: str) -> PredictionManifest:
        return PredictionManifest.model_validate_json(
            self.prediction_manifest_path(trade_date, run_id).read_text(encoding="utf-8")
        )

    def load_predictions(self, trade_date: date, run_id: str) -> Any:
        pd = require_parquet_support()
        frame_path = self.prediction_dir(trade_date, run_id) / "predictions.parquet"
        return pd.read_parquet(frame_path)

    def list_prediction_manifests(self) -> list[PredictionManifest]:
        manifests: list[PredictionManifest] = []
        for path in sorted((self.root / "predictions").glob("trade_date=*/run_id=*/prediction_manifest.json")):
            manifests.append(PredictionManifest.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(manifests, key=lambda item: (item.trade_date, item.run_id), reverse=True)
