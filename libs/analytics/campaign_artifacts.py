"""File-first persistence for M14 walk-forward campaign artifacts."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.analytics.campaign_schemas import (
    CampaignCompareDayRowRecord,
    CampaignCompareManifest,
    CampaignCompareRunRecord,
    CampaignCompareSummaryRecord,
    CampaignDayRowRecord,
    CampaignManifest,
    CampaignRunRecord,
    CampaignSummaryRecord,
    CampaignTimeseriesRowRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class CampaignArtifactStore:
    """Persist M14 campaign and campaign compare artifacts."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "analytics" / "campaign_runs"
        self.day_root = project_root / "data" / "analytics" / "campaign_day_rows"
        self.timeseries_root = project_root / "data" / "analytics" / "campaign_timeseries"
        self.summary_root = project_root / "data" / "analytics" / "campaign_summaries"
        self.compare_root = project_root / "data" / "analytics" / "campaign_compares"
        for target in (
            self.run_root,
            self.day_root,
            self.timeseries_root,
            self.summary_root,
            self.compare_root,
        ):
            target.mkdir(parents=True, exist_ok=True)

    def _run_dir(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> Path:
        return (
            self.run_root
            / f"date_start={date_start.isoformat()}"
            / f"date_end={date_end.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"campaign_run_id={campaign_run_id}"
        )

    def _day_dir(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> Path:
        return (
            self.day_root
            / f"date_start={date_start.isoformat()}"
            / f"date_end={date_end.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"campaign_run_id={campaign_run_id}"
        )

    def _timeseries_dir(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> Path:
        return (
            self.timeseries_root
            / f"date_start={date_start.isoformat()}"
            / f"date_end={date_end.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"campaign_run_id={campaign_run_id}"
        )

    def _summary_dir(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> Path:
        return (
            self.summary_root
            / f"date_start={date_start.isoformat()}"
            / f"date_end={date_end.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"campaign_run_id={campaign_run_id}"
        )

    def _compare_dir(self, *, campaign_compare_run_id: str) -> Path:
        return self.compare_root / f"campaign_compare_run_id={campaign_compare_run_id}"

    def _run_path(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> Path:
        return (
            self._run_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            / "campaign_run.json"
        )

    def _manifest_path(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> Path:
        return (
            self._run_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            / "campaign_manifest.json"
        )

    def _compare_run_path(self, *, campaign_compare_run_id: str) -> Path:
        return self._compare_dir(campaign_compare_run_id=campaign_compare_run_id) / "campaign_compare_run.json"

    def _compare_manifest_path(self, *, campaign_compare_run_id: str) -> Path:
        return (
            self._compare_dir(campaign_compare_run_id=campaign_compare_run_id)
            / "campaign_compare_manifest.json"
        )

    def has_campaign_run(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> bool:
        return self._run_path(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            campaign_run_id=campaign_run_id,
        ).exists()

    def has_campaign_compare_run(self, *, campaign_compare_run_id: str) -> bool:
        return self._compare_run_path(campaign_compare_run_id=campaign_compare_run_id).exists()

    def clear_campaign_run(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> None:
        for target in (
            self._run_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            ),
            self._day_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            ),
            self._timeseries_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            ),
            self._summary_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            ),
        ):
            if target.exists():
                shutil.rmtree(target)

    def clear_campaign_compare_run(self, *, campaign_compare_run_id: str) -> None:
        target = self._compare_dir(campaign_compare_run_id=campaign_compare_run_id)
        if target.exists():
            shutil.rmtree(target)

    def save_campaign_run(self, run: CampaignRunRecord) -> Path:
        path = self._run_path(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            campaign_run_id=run.campaign_run_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_campaign_run(
        self,
        run: CampaignRunRecord,
        *,
        error_message: str,
        day_rows: list[CampaignDayRowRecord] | None = None,
        timeseries_rows: list[CampaignTimeseriesRowRecord] | None = None,
    ) -> CampaignManifest:
        pd = require_parquet_support()
        run_path = self.save_campaign_run(run)
        day_rows_path = None
        timeseries_path = None
        if day_rows:
            day_dir = self._day_dir(
                date_start=run.date_start,
                date_end=run.date_end,
                account_id=run.account_id,
                basket_id=run.basket_id,
                campaign_run_id=run.campaign_run_id,
            )
            day_dir.mkdir(parents=True, exist_ok=True)
            day_rows_path = day_dir / "campaign_day_rows.parquet"
            pd.DataFrame([item.model_dump(mode="json") for item in day_rows]).to_parquet(
                day_rows_path, index=False
            )
        if timeseries_rows:
            timeseries_dir = self._timeseries_dir(
                date_start=run.date_start,
                date_end=run.date_end,
                account_id=run.account_id,
                basket_id=run.basket_id,
                campaign_run_id=run.campaign_run_id,
            )
            timeseries_dir.mkdir(parents=True, exist_ok=True)
            timeseries_path = timeseries_dir / "campaign_timeseries_rows.parquet"
            pd.DataFrame([item.model_dump(mode="json") for item in timeseries_rows]).to_parquet(
                timeseries_path, index=False
            )
        manifest = CampaignManifest(
            campaign_run_id=run.campaign_run_id,
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            execution_source_type=run.execution_source_type,
            market_replay_mode=run.market_replay_mode,
            tick_fill_model=run.tick_fill_model,
            time_in_force=run.time_in_force,
            benchmark_enabled=run.benchmark_enabled,
            benchmark_source_type=run.benchmark_source_type,
            model_run_id=run.model_run_id,
            campaign_config_hash=run.campaign_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            day_rows_file_path=relative_path(self.project_root, day_rows_path) if day_rows_path else None,
            day_rows_file_hash=file_sha256(day_rows_path) if day_rows_path else None,
            day_row_count=len(day_rows or []),
            timeseries_file_path=relative_path(self.project_root, timeseries_path) if timeseries_path else None,
            timeseries_file_hash=file_sha256(timeseries_path) if timeseries_path else None,
            timeseries_row_count=len(timeseries_rows or []),
            error_message=error_message,
        )
        self._manifest_path(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            campaign_run_id=run.campaign_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def save_campaign_success(
        self,
        *,
        run: CampaignRunRecord,
        day_rows: list[CampaignDayRowRecord],
        timeseries_rows: list[CampaignTimeseriesRowRecord],
        summary: CampaignSummaryRecord,
    ) -> CampaignManifest:
        pd = require_parquet_support()
        run_path = self.save_campaign_run(run)
        day_dir = self._day_dir(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            campaign_run_id=run.campaign_run_id,
        )
        timeseries_dir = self._timeseries_dir(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            campaign_run_id=run.campaign_run_id,
        )
        summary_dir = self._summary_dir(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            campaign_run_id=run.campaign_run_id,
        )
        day_dir.mkdir(parents=True, exist_ok=True)
        timeseries_dir.mkdir(parents=True, exist_ok=True)
        summary_dir.mkdir(parents=True, exist_ok=True)
        day_rows_path = day_dir / "campaign_day_rows.parquet"
        timeseries_path = timeseries_dir / "campaign_timeseries_rows.parquet"
        summary_path = summary_dir / "campaign_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in day_rows]).to_parquet(
            day_rows_path, index=False
        )
        pd.DataFrame([item.model_dump(mode="json") for item in timeseries_rows]).to_parquet(
            timeseries_path, index=False
        )
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = CampaignManifest(
            campaign_run_id=run.campaign_run_id,
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            execution_source_type=run.execution_source_type,
            market_replay_mode=run.market_replay_mode,
            tick_fill_model=run.tick_fill_model,
            time_in_force=run.time_in_force,
            benchmark_enabled=run.benchmark_enabled,
            benchmark_source_type=run.benchmark_source_type,
            model_run_id=run.model_run_id,
            campaign_config_hash=run.campaign_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            day_rows_file_path=relative_path(self.project_root, day_rows_path),
            day_rows_file_hash=file_sha256(day_rows_path),
            day_row_count=len(day_rows),
            timeseries_file_path=relative_path(self.project_root, timeseries_path),
            timeseries_file_hash=file_sha256(timeseries_path),
            timeseries_row_count=len(timeseries_rows),
            summary_file_path=relative_path(self.project_root, summary_path),
            summary_file_hash=file_sha256(summary_path),
        )
        self._manifest_path(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            campaign_run_id=run.campaign_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_campaign_run(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> CampaignRunRecord:
        return CampaignRunRecord.model_validate_json(
            self._run_path(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_campaign_manifest(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> CampaignManifest:
        return CampaignManifest.model_validate_json(
            self._manifest_path(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_day_rows(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        path = (
            self._day_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            / "campaign_day_rows.parquet"
        )
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [CampaignDayRowRecord.model_validate(item) for item in frame.to_dict(orient="records")]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_timeseries_rows(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        path = (
            self._timeseries_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            / "campaign_timeseries_rows.parquet"
        )
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [
            CampaignTimeseriesRowRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_campaign_summary(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        campaign_run_id: str,
    ) -> CampaignSummaryRecord:
        path = (
            self._summary_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                campaign_run_id=campaign_run_id,
            )
            / "campaign_summary.json"
        )
        return CampaignSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_campaign_manifests(self) -> list[CampaignManifest]:
        manifests: list[CampaignManifest] = []
        for path in sorted(
            self.run_root.glob(
                "date_start=*/date_end=*/account_id=*/basket_id=*/campaign_run_id=*/campaign_manifest.json"
            )
        ):
            manifests.append(CampaignManifest.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)

    def save_campaign_compare_run(self, run: CampaignCompareRunRecord) -> Path:
        path = self._compare_run_path(campaign_compare_run_id=run.campaign_compare_run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_campaign_compare_run(
        self, run: CampaignCompareRunRecord, *, error_message: str
    ) -> CampaignCompareManifest:
        run_path = self.save_campaign_compare_run(run)
        manifest = CampaignCompareManifest(
            campaign_compare_run_id=run.campaign_compare_run_id,
            left_campaign_run_id=run.left_campaign_run_id,
            right_campaign_run_id=run.right_campaign_run_id,
            compare_basis=run.compare_basis,
            compare_config_hash=run.compare_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            error_message=error_message,
            source_model_run_ids=run.source_model_run_ids,
            source_prediction_run_ids=run.source_prediction_run_ids,
            source_strategy_run_ids=run.source_strategy_run_ids,
            source_execution_task_ids=run.source_execution_task_ids,
            source_paper_run_ids=run.source_paper_run_ids,
            source_shadow_run_ids=run.source_shadow_run_ids,
            source_execution_analytics_run_ids=run.source_execution_analytics_run_ids,
            source_portfolio_analytics_run_ids=run.source_portfolio_analytics_run_ids,
            source_benchmark_analytics_run_ids=run.source_benchmark_analytics_run_ids,
            source_qlib_export_run_ids=run.source_qlib_export_run_ids,
            source_standard_build_run_ids=run.source_standard_build_run_ids,
        )
        self._compare_manifest_path(campaign_compare_run_id=run.campaign_compare_run_id).write_text(
            manifest.model_dump_json(indent=2), encoding="utf-8"
        )
        return manifest

    def save_campaign_compare_success(
        self,
        *,
        run: CampaignCompareRunRecord,
        rows: list[CampaignCompareDayRowRecord],
        summary: CampaignCompareSummaryRecord,
    ) -> CampaignCompareManifest:
        pd = require_parquet_support()
        run_path = self.save_campaign_compare_run(run)
        compare_dir = self._compare_dir(campaign_compare_run_id=run.campaign_compare_run_id)
        compare_dir.mkdir(parents=True, exist_ok=True)
        rows_path = compare_dir / "campaign_compare_day_rows.parquet"
        summary_path = compare_dir / "campaign_compare_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in rows]).to_parquet(
            rows_path, index=False
        )
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = CampaignCompareManifest(
            campaign_compare_run_id=run.campaign_compare_run_id,
            left_campaign_run_id=run.left_campaign_run_id,
            right_campaign_run_id=run.right_campaign_run_id,
            compare_basis=run.compare_basis,
            compare_config_hash=run.compare_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            day_rows_file_path=relative_path(self.project_root, rows_path),
            day_rows_file_hash=file_sha256(rows_path),
            day_row_count=len(rows),
            summary_file_path=relative_path(self.project_root, summary_path),
            summary_file_hash=file_sha256(summary_path),
            source_model_run_ids=run.source_model_run_ids,
            source_prediction_run_ids=run.source_prediction_run_ids,
            source_strategy_run_ids=run.source_strategy_run_ids,
            source_execution_task_ids=run.source_execution_task_ids,
            source_paper_run_ids=run.source_paper_run_ids,
            source_shadow_run_ids=run.source_shadow_run_ids,
            source_execution_analytics_run_ids=run.source_execution_analytics_run_ids,
            source_portfolio_analytics_run_ids=run.source_portfolio_analytics_run_ids,
            source_benchmark_analytics_run_ids=run.source_benchmark_analytics_run_ids,
            source_qlib_export_run_ids=run.source_qlib_export_run_ids,
            source_standard_build_run_ids=run.source_standard_build_run_ids,
        )
        self._compare_manifest_path(campaign_compare_run_id=run.campaign_compare_run_id).write_text(
            manifest.model_dump_json(indent=2), encoding="utf-8"
        )
        return manifest

    def load_campaign_compare_manifest(self, *, campaign_compare_run_id: str) -> CampaignCompareManifest:
        return CampaignCompareManifest.model_validate_json(
            self._compare_manifest_path(campaign_compare_run_id=campaign_compare_run_id).read_text(
                encoding="utf-8"
            )
        )

    def load_campaign_compare_day_rows(self, *, campaign_compare_run_id: str) -> Any:
        pd = require_parquet_support()
        path = self._compare_dir(campaign_compare_run_id=campaign_compare_run_id) / "campaign_compare_day_rows.parquet"
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [
            CampaignCompareDayRowRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_campaign_compare_summary(
        self, *, campaign_compare_run_id: str
    ) -> CampaignCompareSummaryRecord:
        path = self._compare_dir(campaign_compare_run_id=campaign_compare_run_id) / "campaign_compare_summary.json"
        return CampaignCompareSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_campaign_compare_manifests(self) -> list[CampaignCompareManifest]:
        manifests: list[CampaignCompareManifest] = []
        for path in sorted(
            self.compare_root.glob("campaign_compare_run_id=*/campaign_compare_manifest.json")
        ):
            manifests.append(CampaignCompareManifest.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)
