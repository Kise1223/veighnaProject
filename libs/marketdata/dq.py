"""Data quality checks and report generation for M4."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from libs.common.time import ensure_cn_aware
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.manifests import finalize_dq_report, make_run_id
from libs.marketdata.schemas import DQIssue, DQReport
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.rules_engine.market_rules import RulesRepository


def evaluate_raw_tick_dq(raw_frame, *, catalog: InstrumentCatalog, rules_repo: RulesRepository):  # type: ignore[no-untyped-def]
    issues: list[DQIssue] = []
    if raw_frame.empty:
        return issues

    required_columns = ["instrument_key", "symbol", "exchange", "exchange_ts", "received_ts", "last_price"]
    for column in required_columns:
        missing = raw_frame[raw_frame[column].isna()]
        for row in missing.to_dict(orient="records"):
            issues.append(_issue("missing_field", row, f"required field is null: {column}"))

    duplicates = raw_frame[raw_frame.duplicated(subset=["raw_hash"], keep=False)]
    for row in duplicates.to_dict(orient="records"):
        issues.append(_issue("duplicate_tick", row, "duplicate raw_hash detected"))

    ordered = _ordered_for_ingest(raw_frame)
    previous_ts: dict[tuple[str, str], datetime] = {}
    for row in ordered.to_dict(orient="records"):
        key = (row["symbol"], row["exchange"])
        exchange_ts = _as_datetime(row["exchange_ts"])
        if key in previous_ts and exchange_ts < previous_ts[key]:
            issues.append(_issue("time_regression", row, "exchange timestamp moved backwards"))
        previous_ts[key] = exchange_ts
        if float(row["last_price"]) < 0 or float(row.get("volume", 0.0) or 0.0) < 0:
            issues.append(_issue("negative_value", row, "negative price or volume detected"))
        try:
            resolved = catalog.resolve(
                instrument_key=row.get("instrument_key"),
                symbol=row.get("symbol"),
                exchange=row.get("exchange"),
            )
        except KeyError:
            issues.append(_issue("symbol_mapping_missing", row, "instrument mapping missing"))
            continue
        phase = rules_repo.get_trading_phase(ensure_cn_aware(exchange_ts), resolved.instrument)
        if phase.value == "CLOSED":
            issues.append(_issue("out_of_session", row, "tick recorded outside tradable sessions"))
    return issues


def _ordered_for_ingest(raw_frame):  # type: ignore[no-untyped-def]
    if "ingest_seq" in raw_frame.columns and not raw_frame["ingest_seq"].isna().all():
        return raw_frame.sort_values(["symbol", "exchange", "ingest_seq"]).reset_index(drop=True)
    return raw_frame.reset_index(drop=True)


def write_dq_report(
    *,
    project_root: Path,
    report_root: Path,
    manifest_store: ManifestStore,
    layer: str,
    trade_date: date | None,
    scope: str,
    issues: list[DQIssue],
) -> DQReport:
    report_root.mkdir(parents=True, exist_ok=True)
    report_id = make_run_id("dq", scope.replace("=", "_"))
    report_path = report_root / f"{report_id}.json"
    draft = DQReport(
        report_id=report_id,
        layer=layer,
        trade_date=trade_date,
        scope=scope,
        status="pending",
        issue_count=len(issues),
        report_path="",
        created_at=ensure_cn_aware(datetime.now()),
        issues=issues,
    )
    finalized = finalize_dq_report(project_root, draft, report_path)
    report_path.write_text(
        json.dumps(finalized.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    manifest_store.upsert_dq_report(finalized)
    return finalized


def _issue(code: str, row: dict[str, object], message: str) -> DQIssue:
    return DQIssue(
        code=code,
        severity="error",
        message=message,
        symbol=str(row.get("symbol")) if row.get("symbol") is not None else None,
        exchange=str(row.get("exchange")) if row.get("exchange") is not None else None,
        instrument_key=(
            str(row.get("instrument_key")) if row.get("instrument_key") is not None else None
        ),
        exchange_ts=_as_datetime(row["exchange_ts"]) if row.get("exchange_ts") is not None else None,
    )


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return ensure_cn_aware(value)
    return ensure_cn_aware(datetime.fromisoformat(str(value)))
