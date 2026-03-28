"""File-first artifact persistence for M11 execution analytics and TCA."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.analytics.schemas import (
    ExecutionAnalyticsManifest,
    ExecutionAnalyticsRunRecord,
    ExecutionCompareManifest,
    ExecutionCompareRowRecord,
    ExecutionCompareRunRecord,
    ExecutionCompareSummaryRecord,
    ExecutionTcaRowRecord,
    ExecutionTcaSummaryRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class ExecutionAnalyticsArtifactStore:
    """Persist M11 analytics artifacts under local file-first roots."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "analytics" / "execution_runs"
        self.row_root = project_root / "data" / "analytics" / "execution_tca_rows"
        self.summary_root = project_root / "data" / "analytics" / "execution_summaries"
        self.compare_root = project_root / "data" / "analytics" / "execution_compares"
        for target in (self.run_root, self.row_root, self.summary_root, self.compare_root):
            target.mkdir(parents=True, exist_ok=True)

    def _analytics_run_dir(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> Path:
        return (
            self.run_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"analytics_run_id={analytics_run_id}"
        )

    def _analytics_row_dir(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> Path:
        return (
            self.row_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"analytics_run_id={analytics_run_id}"
        )

    def _analytics_summary_dir(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> Path:
        return (
            self.summary_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"analytics_run_id={analytics_run_id}"
        )

    def _compare_dir(self, *, compare_run_id: str) -> Path:
        return self.compare_root / f"compare_run_id={compare_run_id}"

    def _analytics_run_path(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> Path:
        return self._analytics_run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            analytics_run_id=analytics_run_id,
        ) / "execution_analytics_run.json"

    def _analytics_manifest_path(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> Path:
        return self._analytics_run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            analytics_run_id=analytics_run_id,
        ) / "execution_analytics_manifest.json"

    def _compare_run_path(self, *, compare_run_id: str) -> Path:
        return self._compare_dir(compare_run_id=compare_run_id) / "execution_compare_run.json"

    def _compare_manifest_path(self, *, compare_run_id: str) -> Path:
        return self._compare_dir(compare_run_id=compare_run_id) / "execution_compare_manifest.json"

    def has_analytics_run(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> bool:
        return self._analytics_run_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            analytics_run_id=analytics_run_id,
        ).exists()

    def has_compare_run(self, *, compare_run_id: str) -> bool:
        return self._compare_run_path(compare_run_id=compare_run_id).exists()

    def clear_analytics_run(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> None:
        for target in (
            self._analytics_run_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                analytics_run_id=analytics_run_id,
            ),
            self._analytics_row_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                analytics_run_id=analytics_run_id,
            ),
            self._analytics_summary_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                analytics_run_id=analytics_run_id,
            ),
        ):
            if target.exists():
                shutil.rmtree(target)

    def clear_compare_run(self, *, compare_run_id: str) -> None:
        target = self._compare_dir(compare_run_id=compare_run_id)
        if target.exists():
            shutil.rmtree(target)

    def save_analytics_run(self, run: ExecutionAnalyticsRunRecord) -> Path:
        path = self._analytics_run_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            analytics_run_id=run.analytics_run_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_analytics_run(
        self, run: ExecutionAnalyticsRunRecord, *, error_message: str
    ) -> ExecutionAnalyticsManifest:
        run_path = self.save_analytics_run(run)
        manifest = ExecutionAnalyticsManifest(
            analytics_run_id=run.analytics_run_id,
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            source_type=run.source_type,
            source_run_ids=run.source_run_ids,
            source_execution_task_id=run.source_execution_task_id,
            source_strategy_run_id=run.source_strategy_run_id,
            source_prediction_run_id=run.source_prediction_run_id,
            analytics_config_hash=run.analytics_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            error_message=error_message,
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self._analytics_manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            analytics_run_id=run.analytics_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def save_analytics_success(
        self,
        *,
        run: ExecutionAnalyticsRunRecord,
        rows: list[ExecutionTcaRowRecord],
        summary: ExecutionTcaSummaryRecord,
    ) -> ExecutionAnalyticsManifest:
        pd = require_parquet_support()
        run_path = self.save_analytics_run(run)
        row_dir = self._analytics_row_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            analytics_run_id=run.analytics_run_id,
        )
        summary_dir = self._analytics_summary_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            analytics_run_id=run.analytics_run_id,
        )
        row_dir.mkdir(parents=True, exist_ok=True)
        summary_dir.mkdir(parents=True, exist_ok=True)
        rows_path = row_dir / "execution_tca_rows.parquet"
        summary_path = summary_dir / "execution_tca_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in rows]).to_parquet(
            rows_path, index=False
        )
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = ExecutionAnalyticsManifest(
            analytics_run_id=run.analytics_run_id,
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            source_type=run.source_type,
            source_run_ids=run.source_run_ids,
            source_execution_task_id=run.source_execution_task_id,
            source_strategy_run_id=run.source_strategy_run_id,
            source_prediction_run_id=run.source_prediction_run_id,
            analytics_config_hash=run.analytics_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            rows_file_path=relative_path(self.project_root, rows_path),
            rows_file_hash=file_sha256(rows_path),
            row_count=len(rows),
            summary_file_path=relative_path(self.project_root, summary_path),
            summary_file_hash=file_sha256(summary_path),
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self._analytics_manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            analytics_run_id=run.analytics_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_analytics_run(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> ExecutionAnalyticsRunRecord:
        return ExecutionAnalyticsRunRecord.model_validate_json(
            self._analytics_run_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                analytics_run_id=analytics_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_analytics_manifest(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> ExecutionAnalyticsManifest:
        return ExecutionAnalyticsManifest.model_validate_json(
            self._analytics_manifest_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                analytics_run_id=analytics_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_analytics_rows(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> Any:
        pd = require_parquet_support()
        path = self._analytics_row_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            analytics_run_id=analytics_run_id,
        ) / "execution_tca_rows.parquet"
        frame = pd.read_parquet(path)
        records = [
            ExecutionTcaRowRecord.model_validate(item) for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_analytics_summary(
        self, *, trade_date: date, account_id: str, basket_id: str, analytics_run_id: str
    ) -> ExecutionTcaSummaryRecord:
        path = self._analytics_summary_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            analytics_run_id=analytics_run_id,
        ) / "execution_tca_summary.json"
        return ExecutionTcaSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_analytics_manifests(self) -> list[ExecutionAnalyticsManifest]:
        manifests: list[ExecutionAnalyticsManifest] = []
        for path in sorted(
            self.run_root.glob(
                "trade_date=*/account_id=*/basket_id=*/analytics_run_id=*/execution_analytics_manifest.json"
            )
        ):
            manifests.append(
                ExecutionAnalyticsManifest.model_validate_json(path.read_text(encoding="utf-8"))
            )
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)

    def save_compare_run(self, run: ExecutionCompareRunRecord) -> Path:
        path = self._compare_run_path(compare_run_id=run.compare_run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_compare_run(
        self, run: ExecutionCompareRunRecord, *, error_message: str
    ) -> ExecutionCompareManifest:
        run_path = self.save_compare_run(run)
        manifest = ExecutionCompareManifest(
            compare_run_id=run.compare_run_id,
            left_run_id=run.left_run_id,
            right_run_id=run.right_run_id,
            compare_basis=run.compare_basis,
            analytics_config_hash=run.analytics_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            error_message=error_message,
            source_execution_task_ids=run.source_execution_task_ids,
            source_strategy_run_ids=run.source_strategy_run_ids,
            source_prediction_run_ids=run.source_prediction_run_ids,
            source_qlib_export_run_ids=run.source_qlib_export_run_ids,
            source_standard_build_run_ids=run.source_standard_build_run_ids,
        )
        self._compare_manifest_path(compare_run_id=run.compare_run_id).write_text(
            manifest.model_dump_json(indent=2), encoding="utf-8"
        )
        return manifest

    def save_compare_success(
        self,
        *,
        run: ExecutionCompareRunRecord,
        rows: list[ExecutionCompareRowRecord],
        summary: ExecutionCompareSummaryRecord,
    ) -> ExecutionCompareManifest:
        pd = require_parquet_support()
        run_path = self.save_compare_run(run)
        compare_dir = self._compare_dir(compare_run_id=run.compare_run_id)
        compare_dir.mkdir(parents=True, exist_ok=True)
        rows_path = compare_dir / "execution_compare_rows.parquet"
        summary_path = compare_dir / "execution_compare_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in rows]).to_parquet(
            rows_path, index=False
        )
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = ExecutionCompareManifest(
            compare_run_id=run.compare_run_id,
            left_run_id=run.left_run_id,
            right_run_id=run.right_run_id,
            compare_basis=run.compare_basis,
            analytics_config_hash=run.analytics_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            rows_file_path=relative_path(self.project_root, rows_path),
            rows_file_hash=file_sha256(rows_path),
            row_count=len(rows),
            summary_file_path=relative_path(self.project_root, summary_path),
            summary_file_hash=file_sha256(summary_path),
            source_execution_task_ids=run.source_execution_task_ids,
            source_strategy_run_ids=run.source_strategy_run_ids,
            source_prediction_run_ids=run.source_prediction_run_ids,
            source_qlib_export_run_ids=run.source_qlib_export_run_ids,
            source_standard_build_run_ids=run.source_standard_build_run_ids,
        )
        self._compare_manifest_path(compare_run_id=run.compare_run_id).write_text(
            manifest.model_dump_json(indent=2), encoding="utf-8"
        )
        return manifest

    def load_compare_manifest(self, *, compare_run_id: str) -> ExecutionCompareManifest:
        return ExecutionCompareManifest.model_validate_json(
            self._compare_manifest_path(compare_run_id=compare_run_id).read_text(encoding="utf-8")
        )

    def load_compare_summary(self, *, compare_run_id: str) -> ExecutionCompareSummaryRecord:
        path = self._compare_dir(compare_run_id=compare_run_id) / "execution_compare_summary.json"
        return ExecutionCompareSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_compare_manifests(self) -> list[ExecutionCompareManifest]:
        manifests: list[ExecutionCompareManifest] = []
        for path in sorted(self.compare_root.glob("compare_run_id=*/execution_compare_manifest.json")):
            manifests.append(
                ExecutionCompareManifest.model_validate_json(path.read_text(encoding="utf-8"))
            )
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)
