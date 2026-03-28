"""File-first persistence for M13 benchmark reference artifacts."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.analytics.benchmark_schemas import (
    BenchmarkReferenceManifest,
    BenchmarkReferenceRunRecord,
    BenchmarkSummaryRecord,
    BenchmarkWeightRowRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class BenchmarkReferenceArtifactStore:
    """Persist M13 benchmark references under local file-first roots."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "analytics" / "benchmark_runs"
        self.weight_root = project_root / "data" / "analytics" / "benchmark_weights"
        self.summary_root = project_root / "data" / "analytics" / "benchmark_reference_summaries"
        for target in (self.run_root, self.weight_root, self.summary_root):
            target.mkdir(parents=True, exist_ok=True)

    def _run_dir(self, *, trade_date: date, benchmark_run_id: str) -> Path:
        return self.run_root / f"trade_date={trade_date.isoformat()}" / f"benchmark_run_id={benchmark_run_id}"

    def _weight_dir(self, *, trade_date: date, benchmark_run_id: str) -> Path:
        return self.weight_root / f"trade_date={trade_date.isoformat()}" / f"benchmark_run_id={benchmark_run_id}"

    def _summary_dir(self, *, trade_date: date, benchmark_run_id: str) -> Path:
        return self.summary_root / f"trade_date={trade_date.isoformat()}" / f"benchmark_run_id={benchmark_run_id}"

    def _run_path(self, *, trade_date: date, benchmark_run_id: str) -> Path:
        return self._run_dir(trade_date=trade_date, benchmark_run_id=benchmark_run_id) / "benchmark_reference_run.json"

    def _manifest_path(self, *, trade_date: date, benchmark_run_id: str) -> Path:
        return self._run_dir(trade_date=trade_date, benchmark_run_id=benchmark_run_id) / "benchmark_reference_manifest.json"

    def has_benchmark_run(self, *, trade_date: date, benchmark_run_id: str) -> bool:
        return self._run_path(trade_date=trade_date, benchmark_run_id=benchmark_run_id).exists()

    def clear_benchmark_run(self, *, trade_date: date, benchmark_run_id: str) -> None:
        for target in (
            self._run_dir(trade_date=trade_date, benchmark_run_id=benchmark_run_id),
            self._weight_dir(trade_date=trade_date, benchmark_run_id=benchmark_run_id),
            self._summary_dir(trade_date=trade_date, benchmark_run_id=benchmark_run_id),
        ):
            if target.exists():
                shutil.rmtree(target)

    def save_benchmark_run(self, run: BenchmarkReferenceRunRecord) -> Path:
        path = self._run_path(trade_date=run.trade_date, benchmark_run_id=run.benchmark_run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_benchmark_run(
        self,
        run: BenchmarkReferenceRunRecord,
        *,
        error_message: str,
    ) -> BenchmarkReferenceManifest:
        run_path = self.save_benchmark_run(run)
        manifest = BenchmarkReferenceManifest(
            benchmark_run_id=run.benchmark_run_id,
            trade_date=run.trade_date,
            benchmark_name=run.benchmark_name,
            benchmark_source_type=run.benchmark_source_type,
            source_portfolio_analytics_run_id=run.source_portfolio_analytics_run_id,
            source_strategy_run_id=run.source_strategy_run_id,
            source_prediction_run_id=run.source_prediction_run_id,
            benchmark_config_hash=run.benchmark_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            error_message=error_message,
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self._manifest_path(trade_date=run.trade_date, benchmark_run_id=run.benchmark_run_id).write_text(
            manifest.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return manifest

    def save_benchmark_success(
        self,
        *,
        run: BenchmarkReferenceRunRecord,
        weights: list[BenchmarkWeightRowRecord],
        summary: BenchmarkSummaryRecord,
    ) -> BenchmarkReferenceManifest:
        pd = require_parquet_support()
        run_path = self.save_benchmark_run(run)
        weight_dir = self._weight_dir(trade_date=run.trade_date, benchmark_run_id=run.benchmark_run_id)
        summary_dir = self._summary_dir(trade_date=run.trade_date, benchmark_run_id=run.benchmark_run_id)
        weight_dir.mkdir(parents=True, exist_ok=True)
        summary_dir.mkdir(parents=True, exist_ok=True)
        weights_path = weight_dir / "benchmark_weight_rows.parquet"
        summary_path = summary_dir / "benchmark_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in weights]).to_parquet(weights_path, index=False)
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = BenchmarkReferenceManifest(
            benchmark_run_id=run.benchmark_run_id,
            trade_date=run.trade_date,
            benchmark_name=run.benchmark_name,
            benchmark_source_type=run.benchmark_source_type,
            source_portfolio_analytics_run_id=run.source_portfolio_analytics_run_id,
            source_strategy_run_id=run.source_strategy_run_id,
            source_prediction_run_id=run.source_prediction_run_id,
            benchmark_config_hash=run.benchmark_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            weights_file_path=relative_path(self.project_root, weights_path),
            weights_file_hash=file_sha256(weights_path),
            weight_row_count=len(weights),
            summary_file_path=relative_path(self.project_root, summary_path),
            summary_file_hash=file_sha256(summary_path),
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self._manifest_path(trade_date=run.trade_date, benchmark_run_id=run.benchmark_run_id).write_text(
            manifest.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return manifest

    def load_benchmark_run(self, *, trade_date: date, benchmark_run_id: str) -> BenchmarkReferenceRunRecord:
        return BenchmarkReferenceRunRecord.model_validate_json(
            self._run_path(trade_date=trade_date, benchmark_run_id=benchmark_run_id).read_text(encoding="utf-8")
        )

    def load_benchmark_manifest(self, *, trade_date: date, benchmark_run_id: str) -> BenchmarkReferenceManifest:
        return BenchmarkReferenceManifest.model_validate_json(
            self._manifest_path(trade_date=trade_date, benchmark_run_id=benchmark_run_id).read_text(encoding="utf-8")
        )

    def load_weight_rows(self, *, trade_date: date, benchmark_run_id: str) -> Any:
        pd = require_parquet_support()
        path = self._weight_dir(trade_date=trade_date, benchmark_run_id=benchmark_run_id) / "benchmark_weight_rows.parquet"
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [BenchmarkWeightRowRecord.model_validate(item) for item in frame.to_dict(orient="records")]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_benchmark_summary(self, *, trade_date: date, benchmark_run_id: str) -> BenchmarkSummaryRecord:
        path = self._summary_dir(trade_date=trade_date, benchmark_run_id=benchmark_run_id) / "benchmark_summary.json"
        return BenchmarkSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_benchmark_manifests(self) -> list[BenchmarkReferenceManifest]:
        manifests: list[BenchmarkReferenceManifest] = []
        for path in sorted(self.run_root.glob("trade_date=*/benchmark_run_id=*/benchmark_reference_manifest.json")):
            manifests.append(BenchmarkReferenceManifest.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)
