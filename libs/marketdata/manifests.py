"""Manifest builders shared by recorder, ETL, DQ, and research exports."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import file_sha256, relative_path, stable_hash
from libs.marketdata.schemas import DQReport, RawFileManifest, RecordingRun, StandardFileManifest


def make_run_id(prefix: str, seed: str | None = None) -> str:
    timestamp = ensure_cn_aware(datetime.now()).strftime("%Y%m%dT%H%M%S")
    if not seed:
        return f"{prefix}_{timestamp}"
    return f"{prefix}_{timestamp}_{stable_hash({'seed': seed})[:8]}"


def make_recording_run(
    *,
    run_id: str,
    source_gateway: str,
    mode: str,
    status: str = "running",
    notes: str | None = None,
) -> RecordingRun:
    return RecordingRun(
        run_id=run_id,
        source_gateway=source_gateway,
        mode=mode,
        started_at=ensure_cn_aware(datetime.now()),
        status=status,
        notes=notes,
    )


def finish_recording_run(run: RecordingRun, *, status: str, notes: str | None = None) -> RecordingRun:
    payload = run.model_copy(update={"finished_at": ensure_cn_aware(datetime.now()), "status": status})
    if notes is not None:
        payload.notes = notes
    return payload


def make_raw_file_manifest(
    *,
    project_root: Path,
    run_id: str,
    trade_date: date,
    instrument_key: str,
    symbol: str,
    exchange: str,
    gateway_name: str,
    row_count: int,
    file_path: Path,
) -> RawFileManifest:
    relative = relative_path(project_root, file_path)
    file_hash = file_sha256(file_path)
    file_id = stable_hash({"run_id": run_id, "file_path": relative, "file_hash": file_hash})[:24]
    return RawFileManifest(
        file_id=file_id,
        run_id=run_id,
        trade_date=trade_date,
        instrument_key=instrument_key,
        symbol=symbol,
        exchange=exchange,
        gateway_name=gateway_name,
        row_count=row_count,
        file_path=relative,
        file_hash=file_hash,
        created_at=ensure_cn_aware(datetime.now()),
    )


def make_standard_file_manifest(
    *,
    project_root: Path,
    build_run_id: str,
    layer: str,
    row_count: int,
    file_path: Path,
    trade_date: date | None = None,
    instrument_key: str | None = None,
    symbol: str | None = None,
    exchange: str | None = None,
) -> StandardFileManifest:
    relative = relative_path(project_root, file_path)
    file_hash = file_sha256(file_path)
    file_id = stable_hash(
        {"build_run_id": build_run_id, "layer": layer, "file_path": relative, "file_hash": file_hash}
    )[:24]
    return StandardFileManifest(
        file_id=file_id,
        build_run_id=build_run_id,
        layer=layer,
        trade_date=trade_date,
        instrument_key=instrument_key,
        symbol=symbol,
        exchange=exchange,
        row_count=row_count,
        file_path=relative,
        file_hash=file_hash,
        created_at=ensure_cn_aware(datetime.now()),
    )


def finalize_dq_report(project_root: Path, report: DQReport, report_path: Path) -> DQReport:
    return report.model_copy(
        update={
            "report_path": relative_path(project_root, report_path),
            "created_at": ensure_cn_aware(datetime.now()),
            "issue_count": len(report.issues),
            "status": "passed" if not report.issues else "failed",
        }
    )
