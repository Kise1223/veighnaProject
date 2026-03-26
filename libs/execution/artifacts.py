"""File-first artifact persistence for M7 paper execution."""

from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.execution.schemas import (
    PaperAccountSnapshotRecord,
    PaperExecutionManifest,
    PaperExecutionRunRecord,
    PaperOrderRecord,
    PaperPositionSnapshotRecord,
    PaperReconcileReportRecord,
    PaperTradeRecord,
)
from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support


class ExecutionArtifactStore:
    """Persist M7 paper execution artifacts under local file-first roots."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.run_root = project_root / "data" / "trading" / "paper_runs"
        self.order_root = project_root / "data" / "trading" / "paper_orders"
        self.trade_root = project_root / "data" / "trading" / "paper_trades"
        self.account_root = project_root / "data" / "trading" / "paper_accounts"
        self.position_root = project_root / "data" / "trading" / "paper_positions"
        self.report_root = project_root / "data" / "trading" / "paper_reports"
        for target in (
            self.run_root,
            self.order_root,
            self.trade_root,
            self.account_root,
            self.position_root,
            self.report_root,
        ):
            target.mkdir(parents=True, exist_ok=True)

    def run_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> Path:
        return (
            self.run_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"paper_run_id={paper_run_id}"
        )

    def order_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> Path:
        return (
            self.order_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"paper_run_id={paper_run_id}"
        )

    def trade_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> Path:
        return (
            self.trade_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"paper_run_id={paper_run_id}"
        )

    def account_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        paper_run_id: str,
    ) -> Path:
        return (
            self.account_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"paper_run_id={paper_run_id}"
        )

    def position_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        paper_run_id: str,
    ) -> Path:
        return (
            self.position_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"paper_run_id={paper_run_id}"
        )

    def report_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> Path:
        return (
            self.report_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"paper_run_id={paper_run_id}"
        )

    def run_record_path(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> Path:
        return self.run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        ) / "paper_execution_run.json"

    def run_manifest_path(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> Path:
        return self.run_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        ) / "paper_execution_manifest.json"

    def has_run(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> bool:
        return self.run_record_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        ).exists()

    def clear_run(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> None:
        for target_dir in (
            self.run_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                paper_run_id=paper_run_id,
            ),
            self.order_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                paper_run_id=paper_run_id,
            ),
            self.trade_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                paper_run_id=paper_run_id,
            ),
            self.account_dir(
                trade_date=trade_date,
                account_id=account_id,
                paper_run_id=paper_run_id,
            ),
            self.position_dir(
                trade_date=trade_date,
                account_id=account_id,
                paper_run_id=paper_run_id,
            ),
            self.report_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                paper_run_id=paper_run_id,
            ),
        ):
            if target_dir.exists():
                shutil.rmtree(target_dir)

    def save_run(self, run: PaperExecutionRunRecord) -> Path:
        target_path = self.run_record_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            paper_run_id=run.paper_run_id,
        )
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(run.model_dump_json(indent=2), encoding="utf-8")
        return target_path

    def save_failed_run(
        self,
        run: PaperExecutionRunRecord,
        *,
        error_message: str,
    ) -> PaperExecutionManifest:
        run_path = self.save_run(run)
        manifest = PaperExecutionManifest(
            paper_run_id=run.paper_run_id,
            strategy_run_id=run.strategy_run_id,
            execution_task_id=run.execution_task_id,
            account_id=run.account_id,
            basket_id=run.basket_id,
            trade_date=run.trade_date,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            error_message=error_message,
            source_prediction_run_id=run.source_prediction_run_id,
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self.run_manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            paper_run_id=run.paper_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def save_success(
        self,
        *,
        run: PaperExecutionRunRecord,
        orders: list[PaperOrderRecord],
        trades: list[PaperTradeRecord],
        account_snapshot: PaperAccountSnapshotRecord,
        positions: list[PaperPositionSnapshotRecord],
        report: PaperReconcileReportRecord,
    ) -> PaperExecutionManifest:
        pd = require_parquet_support()
        run_path = self.save_run(run)
        order_dir = self.order_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            paper_run_id=run.paper_run_id,
        )
        trade_dir = self.trade_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            paper_run_id=run.paper_run_id,
        )
        account_dir = self.account_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            paper_run_id=run.paper_run_id,
        )
        position_dir = self.position_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            paper_run_id=run.paper_run_id,
        )
        report_dir = self.report_dir(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            paper_run_id=run.paper_run_id,
        )
        order_dir.mkdir(parents=True, exist_ok=True)
        trade_dir.mkdir(parents=True, exist_ok=True)
        account_dir.mkdir(parents=True, exist_ok=True)
        position_dir.mkdir(parents=True, exist_ok=True)
        report_dir.mkdir(parents=True, exist_ok=True)

        orders_path = order_dir / "paper_orders.parquet"
        trades_path = trade_dir / "paper_trades.parquet"
        account_path = account_dir / "paper_account_snapshot.json"
        positions_path = position_dir / "paper_positions.parquet"
        report_path = report_dir / "paper_reconcile_report.json"

        pd.DataFrame([item.model_dump(mode="json") for item in orders]).to_parquet(
            orders_path, index=False
        )
        pd.DataFrame([item.model_dump(mode="json") for item in trades]).to_parquet(
            trades_path, index=False
        )
        account_path.write_text(account_snapshot.model_dump_json(indent=2), encoding="utf-8")
        pd.DataFrame([item.model_dump(mode="json") for item in positions]).to_parquet(
            positions_path, index=False
        )
        report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")

        manifest = PaperExecutionManifest(
            paper_run_id=run.paper_run_id,
            strategy_run_id=run.strategy_run_id,
            execution_task_id=run.execution_task_id,
            account_id=run.account_id,
            basket_id=run.basket_id,
            trade_date=run.trade_date,
            status=run.status,
            created_at=run.created_at,
            run_file_path=relative_path(self.project_root, run_path),
            run_file_hash=file_sha256(run_path),
            orders_file_path=relative_path(self.project_root, orders_path),
            orders_file_hash=file_sha256(orders_path),
            orders_count=len(orders),
            trades_file_path=relative_path(self.project_root, trades_path),
            trades_file_hash=file_sha256(trades_path),
            trades_count=len(trades),
            account_file_path=relative_path(self.project_root, account_path),
            account_file_hash=file_sha256(account_path),
            positions_file_path=relative_path(self.project_root, positions_path),
            positions_file_hash=file_sha256(positions_path),
            positions_count=len(positions),
            report_file_path=relative_path(self.project_root, report_path),
            report_file_hash=file_sha256(report_path),
            source_prediction_run_id=run.source_prediction_run_id,
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        self.run_manifest_path(
            trade_date=run.trade_date,
            account_id=run.account_id,
            basket_id=run.basket_id,
            paper_run_id=run.paper_run_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def load_run(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> PaperExecutionRunRecord:
        return PaperExecutionRunRecord.model_validate_json(
            self.run_record_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                paper_run_id=paper_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_manifest(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> PaperExecutionManifest:
        return PaperExecutionManifest.model_validate_json(
            self.run_manifest_path(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                paper_run_id=paper_run_id,
            ).read_text(encoding="utf-8")
        )

    def load_paper_orders(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        path = self.order_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        ) / "paper_orders.parquet"
        frame = pd.read_parquet(path)
        records = [PaperOrderRecord.model_validate(item) for item in frame.to_dict(orient="records")]
        return pd.DataFrame(
            [{**item.model_dump(mode="python"), "status": item.status.value} for item in records]
        )

    def load_paper_trades(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        path = self.trade_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        ) / "paper_trades.parquet"
        frame = pd.read_parquet(path)
        records = [PaperTradeRecord.model_validate(item) for item in frame.to_dict(orient="records")]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_account_snapshot(
        self,
        *,
        trade_date: date,
        account_id: str,
        paper_run_id: str,
    ) -> PaperAccountSnapshotRecord:
        path = self.account_dir(
            trade_date=trade_date,
            account_id=account_id,
            paper_run_id=paper_run_id,
        ) / "paper_account_snapshot.json"
        return PaperAccountSnapshotRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def load_position_snapshots(
        self,
        *,
        trade_date: date,
        account_id: str,
        paper_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        path = self.position_dir(
            trade_date=trade_date,
            account_id=account_id,
            paper_run_id=paper_run_id,
        ) / "paper_positions.parquet"
        frame = pd.read_parquet(path)
        records = [
            PaperPositionSnapshotRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame([item.model_dump(mode="python") for item in records])

    def load_reconcile_report(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        paper_run_id: str,
    ) -> PaperReconcileReportRecord:
        path = self.report_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            paper_run_id=paper_run_id,
        ) / "paper_reconcile_report.json"
        return PaperReconcileReportRecord.model_validate_json(path.read_text(encoding="utf-8"))

    def list_runs(self) -> list[PaperExecutionRunRecord]:
        records: list[PaperExecutionRunRecord] = []
        for path in sorted(
            self.run_root.glob(
                "trade_date=*/account_id=*/basket_id=*/paper_run_id=*/paper_execution_run.json"
            )
        ):
            records.append(
                PaperExecutionRunRecord.model_validate_json(path.read_text(encoding="utf-8"))
            )
        return sorted(records, key=lambda item: item.created_at, reverse=True)
