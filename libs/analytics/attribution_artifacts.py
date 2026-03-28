"""File-first persistence for M13 benchmark attribution artifacts."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.analytics.attribution_schemas import (
    BenchmarkAnalyticsManifest,
    BenchmarkAnalyticsRunRecord,
    BenchmarkCompareManifest,
    BenchmarkCompareRowRecord,
    BenchmarkCompareRunRecord,
    BenchmarkCompareSummaryRecord,
    BenchmarkGroupRowRecord,
    BenchmarkPositionRowRecord,
    BenchmarkSummaryRowRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class BenchmarkAttributionArtifactStore:
    """Persist M13 benchmark analytics and compare artifacts."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "analytics" / "benchmark_analytics_runs"
        self.position_root = project_root / "data" / "analytics" / "benchmark_positions"
        self.group_root = project_root / "data" / "analytics" / "benchmark_groups"
        self.summary_root = project_root / "data" / "analytics" / "benchmark_summaries"
        self.compare_root = project_root / "data" / "analytics" / "benchmark_compares"
        for target in (self.run_root, self.position_root, self.group_root, self.summary_root, self.compare_root):
            target.mkdir(parents=True, exist_ok=True)

    def _run_dir(self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str) -> Path:
        return (
            self.run_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"benchmark_analytics_run_id={benchmark_analytics_run_id}"
        )

    def _position_dir(self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str) -> Path:
        return (
            self.position_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"benchmark_analytics_run_id={benchmark_analytics_run_id}"
        )

    def _group_dir(self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str) -> Path:
        return (
            self.group_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"benchmark_analytics_run_id={benchmark_analytics_run_id}"
        )

    def _summary_dir(self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str) -> Path:
        return (
            self.summary_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"benchmark_analytics_run_id={benchmark_analytics_run_id}"
        )

    def _compare_dir(self, *, benchmark_compare_run_id: str) -> Path:
        return self.compare_root / f"benchmark_compare_run_id={benchmark_compare_run_id}"

    def _run_path(self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str) -> Path:
        return self._run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            benchmark_analytics_run_id=benchmark_analytics_run_id,
        ) / "benchmark_analytics_run.json"

    def _manifest_path(self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str) -> Path:
        return self._run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            benchmark_analytics_run_id=benchmark_analytics_run_id,
        ) / "benchmark_analytics_manifest.json"

    def _compare_run_path(self, *, benchmark_compare_run_id: str) -> Path:
        return self._compare_dir(benchmark_compare_run_id=benchmark_compare_run_id) / "benchmark_compare_run.json"

    def _compare_manifest_path(self, *, benchmark_compare_run_id: str) -> Path:
        return self._compare_dir(benchmark_compare_run_id=benchmark_compare_run_id) / "benchmark_compare_manifest.json"

    def has_benchmark_analytics_run(
        self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str
    ) -> bool:
        return self._run_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            benchmark_analytics_run_id=benchmark_analytics_run_id,
        ).exists()

    def has_benchmark_compare_run(self, *, benchmark_compare_run_id: str) -> bool:
        return self._compare_run_path(benchmark_compare_run_id=benchmark_compare_run_id).exists()

    def clear_benchmark_analytics_run(
        self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str
    ) -> None:
        for target in (
            self._run_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            ),
            self._position_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            ),
            self._group_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            ),
            self._summary_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            ),
        ):
            if target.exists():
                shutil.rmtree(target)

    def clear_benchmark_compare_run(self, *, benchmark_compare_run_id: str) -> None:
        target = self._compare_dir(benchmark_compare_run_id=benchmark_compare_run_id)
        if target.exists():
            shutil.rmtree(target)

    def save_benchmark_analytics_run(self, run: BenchmarkAnalyticsRunRecord) -> Path:
        path = self._run_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            benchmark_analytics_run_id=run.benchmark_analytics_run_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_benchmark_analytics_run(
        self,
        run: BenchmarkAnalyticsRunRecord,
        *,
        error_message: str,
    ) -> BenchmarkAnalyticsManifest:
        run_path = self.save_benchmark_analytics_run(run)
        manifest = BenchmarkAnalyticsManifest(
            benchmark_analytics_run_id=run.benchmark_analytics_run_id,
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            source_portfolio_analytics_run_id=run.source_portfolio_analytics_run_id,
            source_run_type=run.source_run_type,
            source_run_id=run.source_run_id,
            source_execution_task_id=run.source_execution_task_id,
            source_strategy_run_id=run.source_strategy_run_id,
            source_prediction_run_id=run.source_prediction_run_id,
            benchmark_run_id=run.benchmark_run_id,
            analytics_config_hash=run.analytics_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            error_message=error_message,
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self._manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            benchmark_analytics_run_id=run.benchmark_analytics_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def save_benchmark_analytics_success(
        self,
        *,
        run: BenchmarkAnalyticsRunRecord,
        positions: list[BenchmarkPositionRowRecord],
        groups: list[BenchmarkGroupRowRecord],
        summary: BenchmarkSummaryRowRecord,
    ) -> BenchmarkAnalyticsManifest:
        pd = require_parquet_support()
        run_path = self.save_benchmark_analytics_run(run)
        position_dir = self._position_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            benchmark_analytics_run_id=run.benchmark_analytics_run_id,
        )
        group_dir = self._group_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            benchmark_analytics_run_id=run.benchmark_analytics_run_id,
        )
        summary_dir = self._summary_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            benchmark_analytics_run_id=run.benchmark_analytics_run_id,
        )
        position_dir.mkdir(parents=True, exist_ok=True)
        group_dir.mkdir(parents=True, exist_ok=True)
        summary_dir.mkdir(parents=True, exist_ok=True)
        positions_path = position_dir / "benchmark_position_rows.parquet"
        groups_path = group_dir / "benchmark_group_rows.parquet"
        summary_path = summary_dir / "benchmark_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in positions]).to_parquet(positions_path, index=False)
        pd.DataFrame([item.model_dump(mode="json") for item in groups]).to_parquet(groups_path, index=False)
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = BenchmarkAnalyticsManifest(
            benchmark_analytics_run_id=run.benchmark_analytics_run_id,
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            source_portfolio_analytics_run_id=run.source_portfolio_analytics_run_id,
            source_run_type=run.source_run_type,
            source_run_id=run.source_run_id,
            source_execution_task_id=run.source_execution_task_id,
            source_strategy_run_id=run.source_strategy_run_id,
            source_prediction_run_id=run.source_prediction_run_id,
            benchmark_run_id=run.benchmark_run_id,
            analytics_config_hash=run.analytics_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            positions_file_path=relative_path(self.project_root, positions_path),
            positions_file_hash=file_sha256(positions_path),
            position_row_count=len(positions),
            groups_file_path=relative_path(self.project_root, groups_path),
            groups_file_hash=file_sha256(groups_path),
            group_row_count=len(groups),
            summary_file_path=relative_path(self.project_root, summary_path),
            summary_file_hash=file_sha256(summary_path),
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self._manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            benchmark_analytics_run_id=run.benchmark_analytics_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_benchmark_analytics_run(
        self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str
    ) -> BenchmarkAnalyticsRunRecord:
        return BenchmarkAnalyticsRunRecord.model_validate_json(
            self._run_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_benchmark_analytics_manifest(
        self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str
    ) -> BenchmarkAnalyticsManifest:
        return BenchmarkAnalyticsManifest.model_validate_json(
            self._manifest_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_benchmark_position_rows(
        self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str
    ) -> Any:
        pd = require_parquet_support()
        path = (
            self._position_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            )
            / "benchmark_position_rows.parquet"
        )
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [BenchmarkPositionRowRecord.model_validate(item) for item in frame.to_dict(orient="records")]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_benchmark_group_rows(
        self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str
    ) -> Any:
        pd = require_parquet_support()
        path = (
            self._group_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            )
            / "benchmark_group_rows.parquet"
        )
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [BenchmarkGroupRowRecord.model_validate(item) for item in frame.to_dict(orient="records")]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_benchmark_summary(
        self, *, trade_date: date, account_id: str, basket_id: str, benchmark_analytics_run_id: str
    ) -> BenchmarkSummaryRowRecord:
        path = (
            self._summary_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                benchmark_analytics_run_id=benchmark_analytics_run_id,
            )
            / "benchmark_summary.json"
        )
        return BenchmarkSummaryRowRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_benchmark_analytics_manifests(self) -> list[BenchmarkAnalyticsManifest]:
        manifests: list[BenchmarkAnalyticsManifest] = []
        for path in sorted(
            self.run_root.glob(
                "trade_date=*/account_id=*/basket_id=*/benchmark_analytics_run_id=*/benchmark_analytics_manifest.json"
            )
        ):
            manifests.append(BenchmarkAnalyticsManifest.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)

    def save_benchmark_compare_run(self, run: BenchmarkCompareRunRecord) -> Path:
        path = self._compare_run_path(benchmark_compare_run_id=run.benchmark_compare_run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_benchmark_compare_run(
        self,
        run: BenchmarkCompareRunRecord,
        *,
        error_message: str,
    ) -> BenchmarkCompareManifest:
        run_path = self.save_benchmark_compare_run(run)
        manifest = BenchmarkCompareManifest(
            benchmark_compare_run_id=run.benchmark_compare_run_id,
            left_benchmark_analytics_run_id=run.left_benchmark_analytics_run_id,
            right_benchmark_analytics_run_id=run.right_benchmark_analytics_run_id,
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
            source_portfolio_analytics_run_ids=run.source_portfolio_analytics_run_ids,
            benchmark_run_ids=run.benchmark_run_ids,
            source_qlib_export_run_ids=run.source_qlib_export_run_ids,
            source_standard_build_run_ids=run.source_standard_build_run_ids,
        )
        self._compare_manifest_path(benchmark_compare_run_id=run.benchmark_compare_run_id).write_text(
            manifest.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return manifest

    def save_benchmark_compare_success(
        self,
        *,
        run: BenchmarkCompareRunRecord,
        rows: list[BenchmarkCompareRowRecord],
        summary: BenchmarkCompareSummaryRecord,
    ) -> BenchmarkCompareManifest:
        pd = require_parquet_support()
        run_path = self.save_benchmark_compare_run(run)
        compare_dir = self._compare_dir(benchmark_compare_run_id=run.benchmark_compare_run_id)
        compare_dir.mkdir(parents=True, exist_ok=True)
        rows_path = compare_dir / "benchmark_compare_rows.parquet"
        summary_path = compare_dir / "benchmark_compare_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in rows]).to_parquet(rows_path, index=False)
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = BenchmarkCompareManifest(
            benchmark_compare_run_id=run.benchmark_compare_run_id,
            left_benchmark_analytics_run_id=run.left_benchmark_analytics_run_id,
            right_benchmark_analytics_run_id=run.right_benchmark_analytics_run_id,
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
            source_portfolio_analytics_run_ids=run.source_portfolio_analytics_run_ids,
            benchmark_run_ids=run.benchmark_run_ids,
            source_qlib_export_run_ids=run.source_qlib_export_run_ids,
            source_standard_build_run_ids=run.source_standard_build_run_ids,
        )
        self._compare_manifest_path(benchmark_compare_run_id=run.benchmark_compare_run_id).write_text(
            manifest.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return manifest

    def load_benchmark_compare_manifest(self, *, benchmark_compare_run_id: str) -> BenchmarkCompareManifest:
        return BenchmarkCompareManifest.model_validate_json(
            self._compare_manifest_path(benchmark_compare_run_id=benchmark_compare_run_id).read_text(
                encoding="utf-8"
            )
        )

    def load_benchmark_compare_summary(self, *, benchmark_compare_run_id: str) -> BenchmarkCompareSummaryRecord:
        path = self._compare_dir(benchmark_compare_run_id=benchmark_compare_run_id) / "benchmark_compare_summary.json"
        return BenchmarkCompareSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_benchmark_compare_manifests(self) -> list[BenchmarkCompareManifest]:
        manifests: list[BenchmarkCompareManifest] = []
        for path in sorted(self.compare_root.glob("benchmark_compare_run_id=*/benchmark_compare_manifest.json")):
            manifests.append(BenchmarkCompareManifest.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)
