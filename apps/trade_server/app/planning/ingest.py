"""Dry-run execution-task ingestion for the trade server."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from apps.trade_server.app.planning.contracts import (
    DryRunIngestionResult,
    OrderRequestPreviewPayload,
)
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.schemas import (
    ExecutionTaskManifest,
    ExecutionTaskStatus,
    OrderRequestPreview,
    ValidationStatus,
)


def ingest_execution_task_dry_run(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str | None = None,
) -> DryRunIngestionResult:
    store = PlanningArtifactStore(project_root)
    manifest = _resolve_execution_task_manifest(
        store,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=execution_task_id,
    )
    if store.has_ingestion_preview(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=manifest.execution_task_id,
    ):
        payload = store.load_ingestion_preview(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=manifest.execution_task_id,
        )
        return DryRunIngestionResult(
            execution_task_id=manifest.execution_task_id,
            strategy_run_id=manifest.strategy_run_id,
            account_id=account_id,
            basket_id=basket_id,
            trade_date=trade_date,
            dry_run=True,
            preview_count=len(payload),
            accepted_count=len(
                [item for item in payload if item.validation_status == ValidationStatus.ACCEPTED]
            ),
            send_order_called=False,
            file_path=manifest.preview_file_path,
        )

    task = store.load_execution_task(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=manifest.execution_task_id,
    )
    previews = store.load_order_intents(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=manifest.execution_task_id,
    )
    payload = [
        OrderRequestPreview(
            execution_task_id=task.execution_task_id,
            strategy_run_id=task.strategy_run_id,
            account_id=task.account_id,
            basket_id=task.basket_id,
            trade_date=task.trade_date,
            instrument_key=str(row["instrument_key"]),
            symbol=str(row["symbol"]),
            exchange=str(row["exchange"]),
            side=str(row["side"]),
            quantity=abs(int(row["delta_quantity"])),
            price=row["reference_price"],
            reference=f"{task.strategy_run_id}:{row['symbol']}:{str(row['side']).lower()}",
            validation_status=ValidationStatus(str(row["validation_status"])),
            validation_reason=(
                str(row["validation_reason"])
                if row.get("validation_reason") is not None
                else None
            ),
            created_at=row["created_at"],
        )
        for row in previews.to_dict(orient="records")
        if abs(int(row["delta_quantity"])) > 0
        and str(row["validation_status"]) == ValidationStatus.ACCEPTED.value
    ]
    store.save_ingestion_preview(
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        execution_task_id=task.execution_task_id,
        payload=payload,
    )
    if task.status != ExecutionTaskStatus.INGESTED_DRY_RUN:
        updated = task.model_copy(update={"status": ExecutionTaskStatus.INGESTED_DRY_RUN})
        store.update_execution_task(updated)
    return DryRunIngestionResult(
        execution_task_id=task.execution_task_id,
        strategy_run_id=task.strategy_run_id,
        account_id=account_id,
        basket_id=task.basket_id,
        trade_date=task.trade_date,
        dry_run=True,
        preview_count=len(payload),
        accepted_count=len(
            [item for item in payload if item.validation_status == ValidationStatus.ACCEPTED]
        ),
        send_order_called=False,
        file_path=store.order_intent_dir(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=task.execution_task_id,
        ).joinpath("dry_run_order_request_preview.json").relative_to(project_root).as_posix(),
    )


def load_dry_run_order_request_preview(
    *,
    project_root: Path,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str,
) -> list[OrderRequestPreviewPayload]:
    store = PlanningArtifactStore(project_root)
    return [
        OrderRequestPreviewPayload.model_validate(item.model_dump(mode="json"))
        for item in store.load_ingestion_preview(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        )
    ]


def _resolve_execution_task_manifest(
    store: PlanningArtifactStore,
    *,
    trade_date: date,
    account_id: str,
    basket_id: str,
    execution_task_id: str | None,
) -> ExecutionTaskManifest:
    if execution_task_id is not None:
        return store.load_execution_task_manifest(
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            execution_task_id=execution_task_id,
        )
    for manifest in store.list_execution_task_manifests():
        if (
            manifest.trade_date == trade_date
            and manifest.account_id == account_id
            and manifest.basket_id == basket_id
        ):
            return manifest
    raise FileNotFoundError(
        f"no execution_task artifact found for trade_date={trade_date.isoformat()} "
        f"account_id={account_id} basket_id={basket_id}"
    )
