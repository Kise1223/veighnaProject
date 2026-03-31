"""File-first persistence for M15 model schedule artifacts."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.analytics.model_schedule_schemas import (
    ModelScheduleDayRowRecord,
    ModelScheduleManifest,
    ModelScheduleRunRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class ModelScheduleArtifactStore:
    """Persist M15 model schedule runs and day rows."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "analytics" / "model_schedule_runs"
        self.day_root = project_root / "data" / "analytics" / "model_schedule_day_rows"
        self.run_root.mkdir(parents=True, exist_ok=True)
        self.day_root.mkdir(parents=True, exist_ok=True)

    def _run_dir(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> Path:
        return (
            self.run_root
            / f"date_start={date_start.isoformat()}"
            / f"date_end={date_end.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"model_schedule_run_id={model_schedule_run_id}"
        )

    def _day_dir(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> Path:
        return (
            self.day_root
            / f"date_start={date_start.isoformat()}"
            / f"date_end={date_end.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"model_schedule_run_id={model_schedule_run_id}"
        )

    def _run_path(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> Path:
        return self._run_dir(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            model_schedule_run_id=model_schedule_run_id,
        ) / "model_schedule_run.json"

    def _manifest_path(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> Path:
        return self._run_dir(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            model_schedule_run_id=model_schedule_run_id,
        ) / "model_schedule_manifest.json"

    def has_schedule_run(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> bool:
        return self._run_path(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            model_schedule_run_id=model_schedule_run_id,
        ).exists()

    def clear_schedule_run(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> None:
        for target in (
            self._run_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                model_schedule_run_id=model_schedule_run_id,
            ),
            self._day_dir(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                model_schedule_run_id=model_schedule_run_id,
            ),
        ):
            if target.exists():
                shutil.rmtree(target)

    def save_schedule_run(self, run: ModelScheduleRunRecord) -> Path:
        path = self._run_path(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            model_schedule_run_id=run.model_schedule_run_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_schedule_run(
        self,
        run: ModelScheduleRunRecord,
        *,
        error_message: str,
        day_rows: list[ModelScheduleDayRowRecord] | None = None,
    ) -> ModelScheduleManifest:
        pd = require_parquet_support()
        run_path = self.save_schedule_run(run)
        day_rows_path = None
        if day_rows:
            day_dir = self._day_dir(
                date_start=run.date_start,
                date_end=run.date_end,
                account_id=run.account_id,
                basket_id=run.basket_id,
                model_schedule_run_id=run.model_schedule_run_id,
            )
            day_dir.mkdir(parents=True, exist_ok=True)
            day_rows_path = day_dir / "model_schedule_day_rows.parquet"
            pd.DataFrame([item.model_dump(mode="json") for item in day_rows]).to_parquet(
                day_rows_path, index=False
            )
        manifest = ModelScheduleManifest(
            model_schedule_run_id=run.model_schedule_run_id,
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            schedule_mode=run.schedule_mode,
            fixed_model_run_id=run.fixed_model_run_id,
            latest_model_resolved_run_id=run.latest_model_resolved_run_id,
            retrain_every_n_trade_days=run.retrain_every_n_trade_days,
            training_window_mode=run.training_window_mode,
            lookback_trade_days=run.lookback_trade_days,
            explicit_schedule_path=run.explicit_schedule_path,
            benchmark_enabled=run.benchmark_enabled,
            benchmark_source_type=run.benchmark_source_type,
            campaign_config_hash=run.campaign_config_hash,
            campaign_run_id=run.campaign_run_id,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            day_rows_file_path=relative_path(self.project_root, day_rows_path) if day_rows_path else None,
            day_rows_file_hash=file_sha256(day_rows_path) if day_rows_path else None,
            day_row_count=len(day_rows or []),
            error_message=error_message,
        )
        self._manifest_path(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            model_schedule_run_id=run.model_schedule_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def save_schedule_success(
        self,
        *,
        run: ModelScheduleRunRecord,
        day_rows: list[ModelScheduleDayRowRecord],
    ) -> ModelScheduleManifest:
        pd = require_parquet_support()
        run_path = self.save_schedule_run(run)
        day_dir = self._day_dir(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            model_schedule_run_id=run.model_schedule_run_id,
        )
        day_dir.mkdir(parents=True, exist_ok=True)
        day_rows_path = day_dir / "model_schedule_day_rows.parquet"
        pd.DataFrame([item.model_dump(mode="json") for item in day_rows]).to_parquet(
            day_rows_path, index=False
        )
        manifest = ModelScheduleManifest(
            model_schedule_run_id=run.model_schedule_run_id,
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            schedule_mode=run.schedule_mode,
            fixed_model_run_id=run.fixed_model_run_id,
            latest_model_resolved_run_id=run.latest_model_resolved_run_id,
            retrain_every_n_trade_days=run.retrain_every_n_trade_days,
            training_window_mode=run.training_window_mode,
            lookback_trade_days=run.lookback_trade_days,
            explicit_schedule_path=run.explicit_schedule_path,
            benchmark_enabled=run.benchmark_enabled,
            benchmark_source_type=run.benchmark_source_type,
            campaign_config_hash=run.campaign_config_hash,
            campaign_run_id=run.campaign_run_id,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            day_rows_file_path=relative_path(self.project_root, day_rows_path),
            day_rows_file_hash=file_sha256(day_rows_path),
            day_row_count=len(day_rows),
            error_message=None,
        )
        self._manifest_path(
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            model_schedule_run_id=run.model_schedule_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_schedule_run(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> ModelScheduleRunRecord:
        return ModelScheduleRunRecord.model_validate_json(
            self._run_path(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                model_schedule_run_id=model_schedule_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_schedule_manifest(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> ModelScheduleManifest:
        return ModelScheduleManifest.model_validate_json(
            self._manifest_path(
                date_start=date_start,
                date_end=date_end,
                account_id=account_id,
                basket_id=basket_id,
                model_schedule_run_id=model_schedule_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_day_rows(
        self,
        *,
        date_start: date,
        date_end: date,
        account_id: str,
        basket_id: str,
        model_schedule_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        path = self._day_dir(
            date_start=date_start,
            date_end=date_end,
            account_id=account_id,
            basket_id=basket_id,
            model_schedule_run_id=model_schedule_run_id,
        ) / "model_schedule_day_rows.parquet"
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [
            ModelScheduleDayRowRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def list_schedule_manifests(self) -> list[ModelScheduleManifest]:
        manifests: list[ModelScheduleManifest] = []
        for path in sorted(
            self.run_root.glob(
                "date_start=*/date_end=*/account_id=*/basket_id=*/model_schedule_run_id=*/model_schedule_manifest.json"
            )
        ):
            manifests.append(ModelScheduleManifest.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)
