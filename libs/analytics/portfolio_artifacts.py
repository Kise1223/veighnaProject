"""File-first artifact persistence for M12 portfolio / risk analytics."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.analytics.portfolio_schemas import (
    PortfolioAnalyticsManifest,
    PortfolioAnalyticsRunRecord,
    PortfolioCompareManifest,
    PortfolioCompareRowRecord,
    PortfolioCompareRunRecord,
    PortfolioCompareSummaryRecord,
    PortfolioGroupRowRecord,
    PortfolioPositionRowRecord,
    PortfolioSummaryRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class PortfolioAnalyticsArtifactStore:
    """Persist M12 portfolio analytics under local file-first roots."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "analytics" / "portfolio_runs"
        self.position_root = project_root / "data" / "analytics" / "portfolio_positions"
        self.group_root = project_root / "data" / "analytics" / "portfolio_groups"
        self.summary_root = project_root / "data" / "analytics" / "portfolio_summaries"
        self.compare_root = project_root / "data" / "analytics" / "portfolio_compares"
        for target in (
            self.run_root,
            self.position_root,
            self.group_root,
            self.summary_root,
            self.compare_root,
        ):
            target.mkdir(parents=True, exist_ok=True)

    def _run_dir(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> Path:
        return (
            self.run_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"portfolio_analytics_run_id={portfolio_analytics_run_id}"
        )

    def _position_dir(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> Path:
        return (
            self.position_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"portfolio_analytics_run_id={portfolio_analytics_run_id}"
        )

    def _group_dir(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> Path:
        return (
            self.group_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"portfolio_analytics_run_id={portfolio_analytics_run_id}"
        )

    def _summary_dir(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> Path:
        return (
            self.summary_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"portfolio_analytics_run_id={portfolio_analytics_run_id}"
        )

    def _compare_dir(self, *, portfolio_compare_run_id: str) -> Path:
        return self.compare_root / f"portfolio_compare_run_id={portfolio_compare_run_id}"

    def _run_path(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> Path:
        return self._run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
        ) / "portfolio_analytics_run.json"

    def _manifest_path(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> Path:
        return self._run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
        ) / "portfolio_analytics_manifest.json"

    def _compare_run_path(self, *, portfolio_compare_run_id: str) -> Path:
        return self._compare_dir(
            portfolio_compare_run_id=portfolio_compare_run_id
        ) / "portfolio_compare_run.json"

    def _compare_manifest_path(self, *, portfolio_compare_run_id: str) -> Path:
        return self._compare_dir(
            portfolio_compare_run_id=portfolio_compare_run_id
        ) / "portfolio_compare_manifest.json"

    def has_portfolio_run(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> bool:
        return self._run_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
        ).exists()

    def has_compare_run(self, *, portfolio_compare_run_id: str) -> bool:
        return self._compare_run_path(portfolio_compare_run_id=portfolio_compare_run_id).exists()

    def clear_portfolio_run(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> None:
        for target in (
            self._run_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                portfolio_analytics_run_id=portfolio_analytics_run_id,
            ),
            self._position_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                portfolio_analytics_run_id=portfolio_analytics_run_id,
            ),
            self._group_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                portfolio_analytics_run_id=portfolio_analytics_run_id,
            ),
            self._summary_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                portfolio_analytics_run_id=portfolio_analytics_run_id,
            ),
        ):
            if target.exists():
                shutil.rmtree(target)

    def clear_compare_run(self, *, portfolio_compare_run_id: str) -> None:
        target = self._compare_dir(portfolio_compare_run_id=portfolio_compare_run_id)
        if target.exists():
            shutil.rmtree(target)

    def save_portfolio_run(self, run: PortfolioAnalyticsRunRecord) -> Path:
        path = self._run_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            portfolio_analytics_run_id=run.portfolio_analytics_run_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_portfolio_run(
        self, run: PortfolioAnalyticsRunRecord, *, error_message: str
    ) -> PortfolioAnalyticsManifest:
        run_path = self.save_portfolio_run(run)
        manifest = PortfolioAnalyticsManifest(
            portfolio_analytics_run_id=run.portfolio_analytics_run_id,
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
        self._manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            portfolio_analytics_run_id=run.portfolio_analytics_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def save_portfolio_success(
        self,
        *,
        run: PortfolioAnalyticsRunRecord,
        positions: list[PortfolioPositionRowRecord],
        groups: list[PortfolioGroupRowRecord],
        summary: PortfolioSummaryRecord,
    ) -> PortfolioAnalyticsManifest:
        pd = require_parquet_support()
        run_path = self.save_portfolio_run(run)
        position_dir = self._position_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            portfolio_analytics_run_id=run.portfolio_analytics_run_id,
        )
        group_dir = self._group_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            portfolio_analytics_run_id=run.portfolio_analytics_run_id,
        )
        summary_dir = self._summary_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            portfolio_analytics_run_id=run.portfolio_analytics_run_id,
        )
        position_dir.mkdir(parents=True, exist_ok=True)
        group_dir.mkdir(parents=True, exist_ok=True)
        summary_dir.mkdir(parents=True, exist_ok=True)
        positions_path = position_dir / "portfolio_position_rows.parquet"
        groups_path = group_dir / "portfolio_group_rows.parquet"
        summary_path = summary_dir / "portfolio_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in positions]).to_parquet(
            positions_path, index=False
        )
        pd.DataFrame([item.model_dump(mode="json") for item in groups]).to_parquet(
            groups_path, index=False
        )
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = PortfolioAnalyticsManifest(
            portfolio_analytics_run_id=run.portfolio_analytics_run_id,
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
            portfolio_analytics_run_id=run.portfolio_analytics_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_portfolio_run(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> PortfolioAnalyticsRunRecord:
        return PortfolioAnalyticsRunRecord.model_validate_json(
            self._run_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                portfolio_analytics_run_id=portfolio_analytics_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_portfolio_manifest(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> PortfolioAnalyticsManifest:
        return PortfolioAnalyticsManifest.model_validate_json(
            self._manifest_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                portfolio_analytics_run_id=portfolio_analytics_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_position_rows(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> Any:
        pd = require_parquet_support()
        path = self._position_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
        ) / "portfolio_position_rows.parquet"
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [
            PortfolioPositionRowRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_group_rows(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> Any:
        pd = require_parquet_support()
        path = self._group_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
        ) / "portfolio_group_rows.parquet"
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [
            PortfolioGroupRowRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_portfolio_summary(
        self, *, trade_date: date, account_id: str, basket_id: str, portfolio_analytics_run_id: str
    ) -> PortfolioSummaryRecord:
        path = self._summary_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            portfolio_analytics_run_id=portfolio_analytics_run_id,
        ) / "portfolio_summary.json"
        return PortfolioSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_portfolio_manifests(self) -> list[PortfolioAnalyticsManifest]:
        manifests: list[PortfolioAnalyticsManifest] = []
        for path in sorted(
            self.run_root.glob(
                "trade_date=*/account_id=*/basket_id=*/portfolio_analytics_run_id=*/portfolio_analytics_manifest.json"
            )
        ):
            manifests.append(
                PortfolioAnalyticsManifest.model_validate_json(path.read_text(encoding="utf-8"))
            )
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)

    def save_compare_run(self, run: PortfolioCompareRunRecord) -> Path:
        path = self._compare_run_path(portfolio_compare_run_id=run.portfolio_compare_run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_compare_run(
        self, run: PortfolioCompareRunRecord, *, error_message: str
    ) -> PortfolioCompareManifest:
        run_path = self.save_compare_run(run)
        manifest = PortfolioCompareManifest(
            portfolio_compare_run_id=run.portfolio_compare_run_id,
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
        self._compare_manifest_path(
            portfolio_compare_run_id=run.portfolio_compare_run_id
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def save_compare_success(
        self,
        *,
        run: PortfolioCompareRunRecord,
        rows: list[PortfolioCompareRowRecord],
        summary: PortfolioCompareSummaryRecord,
    ) -> PortfolioCompareManifest:
        pd = require_parquet_support()
        run_path = self.save_compare_run(run)
        compare_dir = self._compare_dir(portfolio_compare_run_id=run.portfolio_compare_run_id)
        compare_dir.mkdir(parents=True, exist_ok=True)
        rows_path = compare_dir / "portfolio_compare_rows.parquet"
        summary_path = compare_dir / "portfolio_compare_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in rows]).to_parquet(
            rows_path, index=False
        )
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = PortfolioCompareManifest(
            portfolio_compare_run_id=run.portfolio_compare_run_id,
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
        self._compare_manifest_path(
            portfolio_compare_run_id=run.portfolio_compare_run_id
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_compare_manifest(self, *, portfolio_compare_run_id: str) -> PortfolioCompareManifest:
        return PortfolioCompareManifest.model_validate_json(
            self._compare_manifest_path(
                portfolio_compare_run_id=portfolio_compare_run_id
            ).read_text(encoding="utf-8")
        )

    def load_compare_summary(
        self, *, portfolio_compare_run_id: str
    ) -> PortfolioCompareSummaryRecord:
        path = (
            self._compare_dir(portfolio_compare_run_id=portfolio_compare_run_id)
            / "portfolio_compare_summary.json"
        )
        return PortfolioCompareSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_compare_manifests(self) -> list[PortfolioCompareManifest]:
        manifests: list[PortfolioCompareManifest] = []
        for path in sorted(
            self.compare_root.glob(
                "portfolio_compare_run_id=*/portfolio_compare_manifest.json"
            )
        ):
            manifests.append(
                PortfolioCompareManifest.model_validate_json(path.read_text(encoding="utf-8"))
            )
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)
