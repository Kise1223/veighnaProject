"""Load and normalize M6-M11 artifacts for M12 portfolio analytics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from libs.analytics.artifacts import ExecutionAnalyticsArtifactStore
from libs.analytics.loaders import LoadedExecutionSource, select_execution_source
from libs.analytics.portfolio_schemas import PortfolioSourceType
from libs.analytics.schemas import (
    ExecutionAnalyticsStatus,
    ExecutionTcaRowRecord,
    ExecutionTcaSummaryRecord,
)
from libs.analytics.tca import build_execution_tca_rows
from libs.common.time import ensure_cn_aware
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.schemas import (
    PaperAccountSnapshotRecord,
    PaperPositionSnapshotRecord,
    PaperReconcileReportRecord,
)
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.schemas import ApprovedTargetWeightRecord, OrderIntentPreviewRecord
from libs.schemas.master_data import Instrument


@dataclass(frozen=True)
class LoadedPortfolioSource:
    source_type: PortfolioSourceType
    source_run_id: str
    source_run_ids: list[str]
    trade_date: date
    account_id: str
    basket_id: str
    source_execution_task_id: str
    source_strategy_run_id: str
    source_prediction_run_id: str
    source_qlib_export_run_id: str | None
    source_standard_build_run_id: str | None
    replay_mode: str | None
    fill_model_name: str | None
    time_in_force: str | None
    paper_run_id: str
    shadow_run_id: str | None
    account_snapshot: PaperAccountSnapshotRecord
    position_snapshots: list[PaperPositionSnapshotRecord]
    reconcile_report: PaperReconcileReportRecord
    target_weights: list[ApprovedTargetWeightRecord]
    order_intents: list[OrderIntentPreviewRecord]
    tca_rows: list[ExecutionTcaRowRecord]
    tca_summary: ExecutionTcaSummaryRecord
    tca_source: str
    instruments: dict[str, Instrument]


def select_portfolio_source(
    *,
    project_root: Path,
    trade_date: date | None = None,
    account_id: str | None = None,
    basket_id: str | None = None,
    paper_run_id: str | None = None,
    shadow_run_id: str | None = None,
    latest: bool = False,
) -> LoadedPortfolioSource:
    loaded_exec = select_execution_source(
        project_root=project_root,
        trade_date=trade_date,
        account_id=account_id,
        basket_id=basket_id,
        paper_run_id=paper_run_id,
        shadow_run_id=shadow_run_id,
        latest=latest,
    )
    execution_store = ExecutionArtifactStore(project_root)
    shadow_store = ShadowArtifactStore(project_root)
    planning_store = PlanningArtifactStore(project_root)
    if loaded_exec.source.source_type.value == PortfolioSourceType.PAPER_RUN.value:
        paper_run_id_resolved = loaded_exec.source.source_run_id
        shadow_run_id_resolved = None
        source_type = PortfolioSourceType.PAPER_RUN
    else:
        shadow_run = shadow_store.load_run(
            trade_date=loaded_exec.source.trade_date,
            account_id=loaded_exec.source.account_id,
            basket_id=loaded_exec.source.basket_id,
            shadow_run_id=loaded_exec.source.source_run_id,
        )
        if shadow_run.paper_run_id is None:
            raise ValueError(f"shadow run {shadow_run.shadow_run_id} is missing paper_run_id")
        paper_run_id_resolved = shadow_run.paper_run_id
        shadow_run_id_resolved = shadow_run.shadow_run_id
        source_type = PortfolioSourceType.SHADOW_RUN
    paper_run = execution_store.load_run(
        trade_date=loaded_exec.source.trade_date,
        account_id=loaded_exec.source.account_id,
        basket_id=loaded_exec.source.basket_id,
        paper_run_id=paper_run_id_resolved,
    )
    account_snapshot = execution_store.load_account_snapshot(
        trade_date=paper_run.trade_date,
        account_id=paper_run.account_id,
        paper_run_id=paper_run.paper_run_id,
    )
    position_frame = execution_store.load_position_snapshots(
        trade_date=paper_run.trade_date,
        account_id=paper_run.account_id,
        paper_run_id=paper_run.paper_run_id,
    )
    position_snapshots = [
        PaperPositionSnapshotRecord.model_validate(item)
        for item in position_frame.to_dict(orient="records")
    ]
    reconcile_report = execution_store.load_reconcile_report(
        trade_date=paper_run.trade_date,
        account_id=paper_run.account_id,
        basket_id=paper_run.basket_id,
        paper_run_id=paper_run.paper_run_id,
    )
    target_frame = planning_store.load_target_weights(
        trade_date=paper_run.trade_date,
        account_id=paper_run.account_id,
        basket_id=paper_run.basket_id,
        strategy_run_id=paper_run.strategy_run_id,
    )
    target_weights = [
        ApprovedTargetWeightRecord.model_validate(item)
        for item in target_frame.to_dict(orient="records")
    ]
    intent_frame = planning_store.load_order_intents(
        trade_date=paper_run.trade_date,
        account_id=paper_run.account_id,
        basket_id=paper_run.basket_id,
        execution_task_id=paper_run.execution_task_id,
    )
    order_intents = [
        OrderIntentPreviewRecord.model_validate(item)
        for item in intent_frame.to_dict(orient="records")
    ]
    tca_rows, tca_summary, tca_source = _resolve_tca_source(
        project_root=project_root,
        loaded_exec=loaded_exec,
    )
    catalog = InstrumentCatalog.from_bootstrap_dir(project_root / "data" / "master" / "bootstrap")
    instruments = _resolve_instruments(
        catalog=catalog,
        instrument_keys={
            *(item.instrument_key for item in target_weights),
            *(item.instrument_key for item in order_intents),
            *(item.instrument_key for item in position_snapshots),
            *(item.instrument_key for item in tca_rows),
        },
    )
    return LoadedPortfolioSource(
        source_type=source_type,
        source_run_id=loaded_exec.source.source_run_id,
        source_run_ids=loaded_exec.source_run_ids,
        trade_date=paper_run.trade_date,
        account_id=paper_run.account_id,
        basket_id=paper_run.basket_id,
        source_execution_task_id=paper_run.execution_task_id,
        source_strategy_run_id=paper_run.strategy_run_id,
        source_prediction_run_id=paper_run.source_prediction_run_id,
        source_qlib_export_run_id=paper_run.source_qlib_export_run_id,
        source_standard_build_run_id=paper_run.source_standard_build_run_id,
        replay_mode=loaded_exec.source.replay_mode,
        fill_model_name=loaded_exec.source.fill_model_name,
        time_in_force=loaded_exec.source.time_in_force,
        paper_run_id=paper_run_id_resolved,
        shadow_run_id=shadow_run_id_resolved,
        account_snapshot=account_snapshot,
        position_snapshots=position_snapshots,
        reconcile_report=reconcile_report,
        target_weights=target_weights,
        order_intents=order_intents,
        tca_rows=tca_rows,
        tca_summary=tca_summary,
        tca_source=tca_source,
        instruments=instruments,
    )


def _resolve_tca_source(
    *,
    project_root: Path,
    loaded_exec: LoadedExecutionSource,
) -> tuple[list[ExecutionTcaRowRecord], ExecutionTcaSummaryRecord, str]:
    analytics_store = ExecutionAnalyticsArtifactStore(project_root)
    candidates = [
        item
        for item in analytics_store.list_analytics_manifests()
        if item.status == ExecutionAnalyticsStatus.SUCCESS
        and item.trade_date == loaded_exec.source.trade_date
        and item.account_id == loaded_exec.source.account_id
        and item.basket_id == loaded_exec.source.basket_id
        and item.source_type.value == loaded_exec.source.source_type.value
        and set(item.source_run_ids) == set(loaded_exec.source_run_ids)
    ]
    if candidates:
        manifest = candidates[0]
        rows_frame = analytics_store.load_analytics_rows(
            trade_date=manifest.trade_date,
            account_id=manifest.account_id,
            basket_id=manifest.basket_id,
            analytics_run_id=manifest.analytics_run_id,
        )
        rows = [
            ExecutionTcaRowRecord.model_validate(item)
            for item in rows_frame.to_dict(orient="records")
        ]
        summary = analytics_store.load_analytics_summary(
            trade_date=manifest.trade_date,
            account_id=manifest.account_id,
            basket_id=manifest.basket_id,
            analytics_run_id=manifest.analytics_run_id,
        )
        return rows, summary, "m11_cached"
    created_at = ensure_cn_aware(datetime.now())
    rows, summary = build_execution_tca_rows(
        loaded=loaded_exec,
        analytics_run_id="portfolio_source_preview",
        created_at=created_at,
    )
    return rows, summary, "derived_from_execution_source"


def _resolve_instruments(
    *,
    catalog: InstrumentCatalog,
    instrument_keys: set[str],
) -> dict[str, Instrument]:
    instruments: dict[str, Instrument] = {}
    for instrument_key in sorted(instrument_keys):
        try:
            instruments[instrument_key] = catalog.resolve(instrument_key=instrument_key).instrument
        except KeyError:
            continue
    return instruments
