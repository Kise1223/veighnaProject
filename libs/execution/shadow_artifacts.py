"""File-first artifact persistence for M8 shadow sessions."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.execution.shadow_schemas import (
    ShadowFillEventRecord,
    ShadowOrderStateEventRecord,
    ShadowSessionManifest,
    ShadowSessionReportRecord,
    ShadowSessionRunRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class ShadowArtifactStore:
    """Persist M8 shadow-session artifacts under local file-first roots."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "trading" / "shadow_runs"
        self.order_event_root = project_root / "data" / "trading" / "shadow_order_events"
        self.fill_event_root = project_root / "data" / "trading" / "shadow_fill_events"
        self.report_root = project_root / "data" / "trading" / "shadow_reports"
        for target in (self.run_root, self.order_event_root, self.fill_event_root, self.report_root):
            target.mkdir(parents=True, exist_ok=True)

    def run_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> Path:
        return (
            self.run_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"shadow_run_id={shadow_run_id}"
        )

    def order_event_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> Path:
        return (
            self.order_event_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"shadow_run_id={shadow_run_id}"
        )

    def fill_event_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> Path:
        return (
            self.fill_event_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"shadow_run_id={shadow_run_id}"
        )

    def report_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> Path:
        return (
            self.report_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"shadow_run_id={shadow_run_id}"
        )

    def run_record_path(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> Path:
        return self.run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        ) / "shadow_session_run.json"

    def manifest_path(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> Path:
        return self.run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        ) / "shadow_session_manifest.json"

    def has_run(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> bool:
        return self.run_record_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        ).exists()

    def clear_run(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> None:
        for target_dir in (
            self.run_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                shadow_run_id=shadow_run_id,
            ),
            self.order_event_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                shadow_run_id=shadow_run_id,
            ),
            self.fill_event_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                shadow_run_id=shadow_run_id,
            ),
            self.report_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                shadow_run_id=shadow_run_id,
            ),
        ):
            if target_dir.exists():
                shutil.rmtree(target_dir)

    def save_run(self, run: ShadowSessionRunRecord) -> Path:
        path = self.run_record_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            shadow_run_id=run.shadow_run_id,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return path

    def save_failed_run(
        self,
        run: ShadowSessionRunRecord,
        *,
        error_message: str,
    ) -> ShadowSessionManifest:
        run_path = self.save_run(run)
        manifest = ShadowSessionManifest(
            shadow_run_id=run.shadow_run_id,
            paper_run_id=run.paper_run_id,
            strategy_run_id=run.strategy_run_id,
            execution_task_id=run.execution_task_id,
            account_id=run.account_id,
            basket_id=run.basket_id,
            trade_date=run.trade_date,
            status=run.status,
            created_at=run.created_at,
            started_at=run.started_at,
            ended_at=run.ended_at,
            market_replay_mode=run.market_replay_mode,
            tick_source_hash=run.tick_source_hash,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            error_message=error_message,
            source_prediction_run_id=run.source_prediction_run_id,
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self.manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            shadow_run_id=run.shadow_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def save_success(
        self,
        *,
        run: ShadowSessionRunRecord,
        order_events: list[ShadowOrderStateEventRecord],
        fill_events: list[ShadowFillEventRecord],
        report: ShadowSessionReportRecord,
        paper_report_path: str | None,
        paper_report_hash: str | None,
    ) -> ShadowSessionManifest:
        pd = require_parquet_support()
        run_path = self.save_run(run)
        order_dir = self.order_event_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            shadow_run_id=run.shadow_run_id,
        )
        fill_dir = self.fill_event_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            shadow_run_id=run.shadow_run_id,
        )
        report_dir = self.report_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            shadow_run_id=run.shadow_run_id,
        )
        order_dir.mkdir(parents=True, exist_ok=True)
        fill_dir.mkdir(parents=True, exist_ok=True)
        report_dir.mkdir(parents=True, exist_ok=True)

        order_events_path = order_dir / "shadow_order_events.parquet"
        fill_events_path = fill_dir / "shadow_fill_events.parquet"
        report_path = report_dir / "shadow_session_report.json"
        pd.DataFrame([item.model_dump(mode="json") for item in order_events]).to_parquet(
            order_events_path,
            index=False,
        )
        pd.DataFrame([item.model_dump(mode="json") for item in fill_events]).to_parquet(
            fill_events_path,
            index=False,
        )
        report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
        manifest = ShadowSessionManifest(
            shadow_run_id=run.shadow_run_id,
            paper_run_id=run.paper_run_id,
            strategy_run_id=run.strategy_run_id,
            execution_task_id=run.execution_task_id,
            account_id=run.account_id,
            basket_id=run.basket_id,
            trade_date=run.trade_date,
            status=run.status,
            created_at=run.created_at,
            started_at=run.started_at,
            ended_at=run.ended_at,
            market_replay_mode=run.market_replay_mode,
            tick_source_hash=run.tick_source_hash,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            order_events_file_path=relative_path(self.project_root, order_events_path),
            order_events_file_hash=file_sha256(order_events_path),
            order_events_count=len(order_events),
            fill_events_file_path=relative_path(self.project_root, fill_events_path),
            fill_events_file_hash=file_sha256(fill_events_path),
            fill_events_count=len(fill_events),
            report_file_path=relative_path(self.project_root, report_path),
            report_file_hash=file_sha256(report_path),
            paper_report_file_path=paper_report_path,
            paper_report_file_hash=paper_report_hash,
            source_prediction_run_id=run.source_prediction_run_id,
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self.manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            shadow_run_id=run.shadow_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_run(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> ShadowSessionRunRecord:
        return ShadowSessionRunRecord.model_validate_json(
            self.run_record_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                shadow_run_id=shadow_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_manifest(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> ShadowSessionManifest:
        return ShadowSessionManifest.model_validate_json(
            self.manifest_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                shadow_run_id=shadow_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_order_events(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        path = self.order_event_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        ) / "shadow_order_events.parquet"
        frame = pd.read_parquet(path)
        records = [
            ShadowOrderStateEventRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame(
            [
                {
                    **item.model_dump(mode="python"),
                    "event_type": item.event_type.value,
                    "state_before": item.state_before.value if item.state_before else None,
                    "state_after": item.state_after.value,
                }
                for item in records
            ]
        )

    def load_fill_events(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        path = self.fill_event_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        ) / "shadow_fill_events.parquet"
        frame = pd.read_parquet(path)
        records = [
            ShadowFillEventRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_report(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        shadow_run_id: str,
    ) -> ShadowSessionReportRecord:
        path = self.report_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            shadow_run_id=shadow_run_id,
        ) / "shadow_session_report.json"
        return ShadowSessionReportRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_runs(self) -> list[ShadowSessionRunRecord]:
        records: list[ShadowSessionRunRecord] = []
        for path in sorted(
            self.run_root.glob(
                "trade_date=*/account_id=*/basket_id=*/shadow_run_id=*/shadow_session_run.json"
            )
        ):
            records.append(
                ShadowSessionRunRecord.model_validate_json(path.read_text(encoding="utf-8"))
            )
        return sorted(records, key=lambda item: item.created_at, reverse=True)
