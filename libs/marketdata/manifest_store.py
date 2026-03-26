"""Local manifest persistence for M4 parquet-first workflows."""

from __future__ import annotations

from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

from libs.marketdata.schemas import (
    AdjustmentFactorRecord,
    CorporateActionRecord,
    DQReport,
    RawFileManifest,
    RecordingRun,
    StandardFileManifest,
)

ModelT = TypeVar("ModelT", bound=BaseModel)


class ManifestStore:
    """Persist manifest metadata as one JSON document per logical record."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def upsert_recording_run(self, run: RecordingRun) -> Path:
        return self._write("recording_runs", run.run_id, run)

    def upsert_raw_file_manifest(self, manifest: RawFileManifest) -> Path:
        return self._write("raw_file_manifest", manifest.file_id, manifest)

    def upsert_standard_file_manifest(self, manifest: StandardFileManifest) -> Path:
        return self._write("standard_file_manifest", manifest.file_id, manifest)

    def upsert_dq_report(self, report: DQReport) -> Path:
        return self._write("dq_reports", report.report_id, report)

    def upsert_corporate_action(self, action: CorporateActionRecord) -> Path:
        return self._write("corporate_actions", action.action_id, action)

    def upsert_adjustment_factor(self, factor: AdjustmentFactorRecord) -> Path:
        key = f"{factor.instrument_key}_{factor.trade_date.isoformat()}_{factor.adj_mode}"
        return self._write("adjustment_factors", key, factor)

    def list_recording_runs(self) -> list[RecordingRun]:
        return self._read_all("recording_runs", RecordingRun)

    def list_raw_file_manifests(self) -> list[RawFileManifest]:
        return self._read_all("raw_file_manifest", RawFileManifest)

    def list_standard_file_manifests(self, *, layer: str | None = None) -> list[StandardFileManifest]:
        records = self._read_all("standard_file_manifest", StandardFileManifest)
        if layer is None:
            return records
        return [record for record in records if record.layer == layer]

    def delete_standard_file_manifests(
        self,
        *,
        layer: str,
        trade_date: object | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
        instrument_key: str | None = None,
    ) -> None:
        target_dir = self.root / "standard_file_manifest"
        if not target_dir.exists():
            return
        for path in sorted(target_dir.glob("*.json")):
            record = StandardFileManifest.model_validate_json(path.read_text(encoding="utf-8"))
            if record.layer != layer:
                continue
            if trade_date is not None and record.trade_date != trade_date:
                continue
            if symbol is not None and record.symbol != symbol:
                continue
            if exchange is not None and record.exchange != exchange:
                continue
            if instrument_key is not None and record.instrument_key != instrument_key:
                continue
            path.unlink()

    def list_dq_reports(self) -> list[DQReport]:
        return self._read_all("dq_reports", DQReport)

    def list_corporate_actions(self) -> list[CorporateActionRecord]:
        return self._read_all("corporate_actions", CorporateActionRecord)

    def list_adjustment_factors(self) -> list[AdjustmentFactorRecord]:
        return self._read_all("adjustment_factors", AdjustmentFactorRecord)

    def _write(self, kind: str, identifier: str, model: BaseModel) -> Path:
        target_dir = self.root / kind
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{identifier}.json"
        target_path.write_text(model.model_dump_json(indent=2), encoding="utf-8")
        return target_path

    def _read_all(self, kind: str, model_type: type[ModelT]) -> list[ModelT]:
        target_dir = self.root / kind
        if not target_dir.exists():
            return []
        results: list[ModelT] = []
        for path in sorted(target_dir.glob("*.json")):
            results.append(model_type.model_validate_json(path.read_text(encoding="utf-8")))
        return results
