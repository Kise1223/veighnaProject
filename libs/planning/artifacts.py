"""File-first artifact persistence for M6 planning workflows."""

from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any

from libs.marketdata.raw_store import file_sha256, relative_path, require_parquet_support
from libs.planning.schemas import (
    ApprovedTargetWeightManifest,
    ApprovedTargetWeightRecord,
    ExecutionTaskManifest,
    ExecutionTaskRecord,
    OrderIntentPreviewRecord,
    OrderRequestPreview,
)


class PlanningArtifactStore:
    """Persist M6 planning artifacts under file-first research and trading roots."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.research_root = project_root / "data" / "research" / "approved_target_weights"
        self.trading_task_root = project_root / "data" / "trading" / "execution_tasks"
        self.trading_preview_root = project_root / "data" / "trading" / "order_intents"
        self.research_root.mkdir(parents=True, exist_ok=True)
        self.trading_task_root.mkdir(parents=True, exist_ok=True)
        self.trading_preview_root.mkdir(parents=True, exist_ok=True)

    def target_weight_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        strategy_run_id: str,
    ) -> Path:
        return (
            self.research_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"strategy_run_id={strategy_run_id}"
        )

    def target_weight_manifest_path(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        strategy_run_id: str,
    ) -> Path:
        return self.target_weight_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            strategy_run_id=strategy_run_id,
        ) / "approved_target_weight_manifest.json"

    def has_target_weight(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        strategy_run_id: str,
    ) -> bool:
        return self.target_weight_manifest_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            strategy_run_id=strategy_run_id,
        ).exists()

    def clear_target_weights(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        strategy_run_id: str,
    ) -> None:
        target_dir = self.target_weight_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            strategy_run_id=strategy_run_id,
        )
        if target_dir.exists():
            shutil.rmtree(target_dir)

    def save_target_weights(
        self,
        *,
        manifest: ApprovedTargetWeightManifest,
        records: list[ApprovedTargetWeightRecord],
    ) -> ApprovedTargetWeightManifest:
        pd = require_parquet_support()
        target_dir = self.target_weight_dir(
            trade_date=manifest.trade_date,
            account_id=manifest.account_id,
            basket_id=manifest.basket_id,
            strategy_run_id=manifest.strategy_run_id,
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        frame_path = target_dir / "approved_target_weights.parquet"
        pd.DataFrame([record.model_dump(mode="json") for record in records]).to_parquet(
            frame_path, index=False
        )
        finalized = manifest.model_copy(
            update={
                "file_path": relative_path(self.project_root, frame_path),
                "file_hash": file_sha256(frame_path),
            }
        )
        self.target_weight_manifest_path(
            trade_date=manifest.trade_date,
            account_id=manifest.account_id,
            basket_id=manifest.basket_id,
            strategy_run_id=manifest.strategy_run_id,
        ).write_text(finalized.model_dump_json(indent=2), encoding="utf-8")
        return finalized

    def load_target_weight_manifest(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        strategy_run_id: str,
    ) -> ApprovedTargetWeightManifest:
        path = self.target_weight_manifest_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            strategy_run_id=strategy_run_id,
        )
        return ApprovedTargetWeightManifest.model_validate_json(path.read_text(encoding="utf-8"))

    def load_target_weights(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        strategy_run_id: str,
    ) -> Any:
        pd = require_parquet_support()
        frame_path = self.target_weight_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            strategy_run_id=strategy_run_id,
        ) / "approved_target_weights.parquet"
        frame = pd.read_parquet(frame_path)
        records = [
            ApprovedTargetWeightRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame(
            [
                {
                    **record.model_dump(mode="python"),
                    "status": record.status.value,
                }
                for record in records
            ]
        )

    def list_target_weight_manifests(self) -> list[ApprovedTargetWeightManifest]:
        manifests: list[ApprovedTargetWeightManifest] = []
        for path in sorted(
            self.research_root.glob(
                "trade_date=*/account_id=*/basket_id=*/strategy_run_id=*/approved_target_weight_manifest.json"
            )
        ):
            manifests.append(
                ApprovedTargetWeightManifest.model_validate_json(path.read_text(encoding="utf-8"))
            )
        return sorted(
            manifests,
            key=lambda item: (item.trade_date, item.strategy_run_id),
            reverse=True,
        )

    def execution_task_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> Path:
        return (
            self.trading_task_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"execution_task_id={execution_task_id}"
        )

    def order_intent_dir(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> Path:
        return (
            self.trading_preview_root
            / f"trade_date={trade_date.isoformat()}"
            / f"account_id={account_id}"
            / f"basket_id={basket_id}"
            / f"execution_task_id={execution_task_id}"
        )

    def execution_task_manifest_path(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> Path:
        return self.execution_task_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        ) / "execution_task_manifest.json"

    def has_execution_task(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> bool:
        return self.execution_task_manifest_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        ).exists()

    def clear_execution_task(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> None:
        for target_dir in (
            self.execution_task_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                execution_task_id=execution_task_id,
            ),
            self.order_intent_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                execution_task_id=execution_task_id,
            ),
        ):
            if target_dir.exists():
                shutil.rmtree(target_dir)

    def save_execution_task(
        self,
        *,
        task: ExecutionTaskRecord,
        previews: list[OrderIntentPreviewRecord],
    ) -> ExecutionTaskManifest:
        pd = require_parquet_support()
        task_dir = self.execution_task_dir(
            trade_date=task.trade_date,
            account_id=task.account_id,
            basket_id=task.basket_id,
            execution_task_id=task.execution_task_id,
        )
        preview_dir = self.order_intent_dir(
            trade_date=task.trade_date,
            account_id=task.account_id,
            basket_id=task.basket_id,
            execution_task_id=task.execution_task_id,
        )
        task_dir.mkdir(parents=True, exist_ok=True)
        preview_dir.mkdir(parents=True, exist_ok=True)
        task_path = task_dir / "execution_task.json"
        preview_path = preview_dir / "order_intents.parquet"
        task_path.write_text(task.model_dump_json(indent=2), encoding="utf-8")
        pd.DataFrame([preview.model_dump(mode="json") for preview in previews]).to_parquet(
            preview_path, index=False
        )
        manifest = ExecutionTaskManifest(
            execution_task_id=task.execution_task_id,
            strategy_run_id=task.strategy_run_id,
            account_id=task.account_id,
            basket_id=task.basket_id,
            trade_date=task.trade_date,
            status=task.status,
            created_at=task.created_at,
            source_target_weight_hash=task.source_target_weight_hash,
            planner_config_hash=task.planner_config_hash,
            plan_only=task.plan_only,
            file_path=relative_path(self.project_root, task_path),
            file_hash=file_sha256(task_path),
            preview_file_path=relative_path(self.project_root, preview_path),
            preview_file_hash=file_sha256(preview_path),
            preview_row_count=len(previews),
            source_qlib_export_run_id=task.source_qlib_export_run_id,
            source_standard_build_run_id=task.source_standard_build_run_id,
        )
        self.execution_task_manifest_path(
            trade_date=task.trade_date,
            account_id=task.account_id,
            basket_id=task.basket_id,
            execution_task_id=task.execution_task_id,
        ).write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
        return manifest

    def update_execution_task(self, task: ExecutionTaskRecord) -> ExecutionTaskManifest:
        preview_frame = self.load_order_intents(
            trade_date=task.trade_date,
            account_id=task.account_id,
            basket_id=task.basket_id,
            execution_task_id=task.execution_task_id,
        )
        previews = [
            OrderIntentPreviewRecord.model_validate(item)
            for item in preview_frame.to_dict(orient="records")
        ]
        return self.save_execution_task(task=task, previews=previews)

    def load_execution_task(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> ExecutionTaskRecord:
        task_path = self.execution_task_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        ) / "execution_task.json"
        return ExecutionTaskRecord.model_validate_json(task_path.read_text(encoding="utf-8"))

    def load_execution_task_manifest(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> ExecutionTaskManifest:
        path = self.execution_task_manifest_path(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        )
        return ExecutionTaskManifest.model_validate_json(path.read_text(encoding="utf-8"))

    def load_order_intents(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> Any:
        pd = require_parquet_support()
        preview_path = self.order_intent_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        ) / "order_intents.parquet"
        frame = pd.read_parquet(preview_path)
        records = [
            OrderIntentPreviewRecord.model_validate(item)
            for item in frame.to_dict(orient="records")
        ]
        return pd.DataFrame(
            [
                {
                    **record.model_dump(mode="python"),
                    "validation_status": record.validation_status.value,
                }
                for record in records
            ]
        )

    def save_ingestion_preview(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
        payload: list[OrderRequestPreview],
    ) -> Path:
        target_dir = self.order_intent_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / "dry_run_order_request_preview.json"
        target_path.write_text(
            json.dumps(
                [item.model_dump(mode="json") for item in payload],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return target_path

    def has_ingestion_preview(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> bool:
        return (
            self.order_intent_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                execution_task_id=execution_task_id,
            )
            / "dry_run_order_request_preview.json"
        ).exists()

    def load_ingestion_preview(
        self,
        *,
        trade_date: date,
        account_id: str,
        basket_id: str,
        execution_task_id: str,
    ) -> list[OrderRequestPreview]:
        target_path = (
            self.order_intent_dir(
                trade_date=trade_date,
                account_id=account_id,
                basket_id=basket_id,
                execution_task_id=execution_task_id,
            )
            / "dry_run_order_request_preview.json"
        )
        payload = json.loads(target_path.read_text(encoding="utf-8"))
        return [OrderRequestPreview.model_validate(item) for item in payload]

    def list_execution_task_manifests(self) -> list[ExecutionTaskManifest]:
        manifests: list[ExecutionTaskManifest] = []
        for path in sorted(
            self.trading_task_root.glob(
                "trade_date=*/account_id=*/basket_id=*/execution_task_id=*/execution_task_manifest.json"
            )
        ):
            manifests.append(
                ExecutionTaskManifest.model_validate_json(path.read_text(encoding="utf-8"))
            )
        return sorted(
            manifests,
            key=lambda item: (item.trade_date, item.execution_task_id),
            reverse=True,
        )
