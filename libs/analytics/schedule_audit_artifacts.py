"""File-first persistence for M16 schedule audit artifacts."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from libs.analytics.schedule_audit_schemas import (
    ScheduleAuditDayRowRecord,
    ScheduleAuditManifest,
    ScheduleAuditRunRecord,
    ScheduleAuditSummaryRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class ScheduleAuditArtifactStore:
    """Persist M16 schedule audit runs, day rows, and summaries."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "analytics" / "schedule_audit_runs"
        self.day_root = project_root / "data" / "analytics" / "schedule_audit_day_rows"
        self.summary_root = project_root / "data" / "analytics" / "schedule_audit_summaries"
        self.run_root.mkdir(parents=True, exist_ok=True)
        self.day_root.mkdir(parents=True, exist_ok=True)
        self.summary_root.mkdir(parents=True, exist_ok=True)

    def _run_dir(self, *, schedule_audit_run_id: str) -> Path:
        return self.run_root / f"schedule_audit_run_id={schedule_audit_run_id}"

    def _day_dir(self, *, schedule_audit_run_id: str) -> Path:
        return self.day_root / f"schedule_audit_run_id={schedule_audit_run_id}"

    def _summary_dir(self, *, schedule_audit_run_id: str) -> Path:
        return self.summary_root / f"schedule_audit_run_id={schedule_audit_run_id}"

    def _run_path(self, *, schedule_audit_run_id: str) -> Path:
        return self._run_dir(schedule_audit_run_id=schedule_audit_run_id) / "schedule_audit_run.json"

    def _manifest_path(self, *, schedule_audit_run_id: str) -> Path:
        return self._run_dir(schedule_audit_run_id=schedule_audit_run_id) / "schedule_audit_manifest.json"

    def has_audit_run(self, *, schedule_audit_run_id: str) -> bool:
        return self._run_path(schedule_audit_run_id=schedule_audit_run_id).exists()

    def clear_audit_run(self, *, schedule_audit_run_id: str) -> None:
        for target in (
            self._run_dir(schedule_audit_run_id=schedule_audit_run_id),
            self._day_dir(schedule_audit_run_id=schedule_audit_run_id),
            self._summary_dir(schedule_audit_run_id=schedule_audit_run_id),
        ):
            if target.exists():
                shutil.rmtree(target)

    def save_audit_run(self, run: ScheduleAuditRunRecord) -> Path:
        path = self._run_path(schedule_audit_run_id=run.schedule_audit_run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_audit_run(
        self,
        run: ScheduleAuditRunRecord,
        *,
        error_message: str,
        day_rows: list[ScheduleAuditDayRowRecord] | None = None,
    ) -> ScheduleAuditManifest:
        pd = require_parquet_support()
        run_path = self.save_audit_run(run)
        day_rows_path = None
        if day_rows:
            day_dir = self._day_dir(schedule_audit_run_id=run.schedule_audit_run_id)
            day_dir.mkdir(parents=True, exist_ok=True)
            day_rows_path = day_dir / "schedule_audit_day_rows.parquet"
            pd.DataFrame([item.model_dump(mode="json") for item in day_rows]).to_parquet(
                day_rows_path,
                index=False,
            )
        manifest = ScheduleAuditManifest(
            schedule_audit_run_id=run.schedule_audit_run_id,
            model_schedule_run_id=run.model_schedule_run_id,
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            schedule_mode=run.schedule_mode,
            training_window_mode=run.training_window_mode,
            explicit_schedule_path=run.explicit_schedule_path,
            audit_config_hash=run.audit_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            day_rows_file_path=relative_path(self.project_root, day_rows_path) if day_rows_path else None,
            day_rows_file_hash=file_sha256(day_rows_path) if day_rows_path else None,
            day_row_count=len(day_rows or []),
            error_message=error_message,
        )
        self._manifest_path(schedule_audit_run_id=run.schedule_audit_run_id).write_text(
            manifest.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return manifest

    def save_audit_success(
        self,
        *,
        run: ScheduleAuditRunRecord,
        day_rows: list[ScheduleAuditDayRowRecord],
        summary: ScheduleAuditSummaryRecord,
    ) -> ScheduleAuditManifest:
        pd = require_parquet_support()
        run_path = self.save_audit_run(run)
        day_dir = self._day_dir(schedule_audit_run_id=run.schedule_audit_run_id)
        summary_dir = self._summary_dir(schedule_audit_run_id=run.schedule_audit_run_id)
        day_dir.mkdir(parents=True, exist_ok=True)
        summary_dir.mkdir(parents=True, exist_ok=True)
        day_rows_path = day_dir / "schedule_audit_day_rows.parquet"
        summary_path = summary_dir / "schedule_audit_summary.json"
        pd.DataFrame([item.model_dump(mode="json") for item in day_rows]).to_parquet(
            day_rows_path,
            index=False,
        )
        summary_path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        manifest = ScheduleAuditManifest(
            schedule_audit_run_id=run.schedule_audit_run_id,
            model_schedule_run_id=run.model_schedule_run_id,
            date_start=run.date_start,
            date_end=run.date_end,
            account_id=run.account_id,
            basket_id=run.basket_id,
            schedule_mode=run.schedule_mode,
            training_window_mode=run.training_window_mode,
            explicit_schedule_path=run.explicit_schedule_path,
            audit_config_hash=run.audit_config_hash,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            day_rows_file_path=relative_path(self.project_root, day_rows_path),
            day_rows_file_hash=file_sha256(day_rows_path),
            day_row_count=len(day_rows),
            summary_file_path=relative_path(self.project_root, summary_path),
            summary_file_hash=file_sha256(summary_path),
            error_message=None,
        )
        self._manifest_path(schedule_audit_run_id=run.schedule_audit_run_id).write_text(
            manifest.model_dump_json(indent=2),
            encoding="utf-8",
        )
        return manifest

    def load_audit_manifest(self, *, schedule_audit_run_id: str) -> ScheduleAuditManifest:
        return ScheduleAuditManifest.model_validate_json(
            self._manifest_path(schedule_audit_run_id=schedule_audit_run_id).read_text(encoding="utf-8")
        )

    def load_audit_summary(self, *, schedule_audit_run_id: str) -> ScheduleAuditSummaryRecord:
        path = self._summary_dir(schedule_audit_run_id=schedule_audit_run_id) / "schedule_audit_summary.json"
        return ScheduleAuditSummaryRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def load_audit_day_rows(self, *, schedule_audit_run_id: str) -> Any:
        pd = require_parquet_support()
        path = self._day_dir(schedule_audit_run_id=schedule_audit_run_id) / "schedule_audit_day_rows.parquet"
        frame = pd.read_parquet(path).astype(object)
        frame = frame.where(pd.notna(frame), None)
        records = [
            ScheduleAuditDayRowRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def list_audit_manifests(self) -> list[ScheduleAuditManifest]:
        manifests: list[ScheduleAuditManifest] = []
        for path in sorted(self.run_root.glob("schedule_audit_run_id=*/schedule_audit_manifest.json")):
            manifests.append(ScheduleAuditManifest.model_validate_json(path.read_text(encoding="utf-8")))
        return sorted(manifests, key=lambda item: item.created_at, reverse=True)
