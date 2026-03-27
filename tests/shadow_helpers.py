from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from libs.common.time import CN_TZ
from libs.marketdata.raw_store import write_partition_frame
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.schemas import (
    ApprovedTargetWeightManifest,
    ApprovedTargetWeightRecord,
    ApprovedTargetWeightStatus,
    ExecutionTaskRecord,
    ExecutionTaskStatus,
    OrderIntentPreviewRecord,
    ValidationStatus,
)
from tests.research_helpers import prepare_research_workspace


def prepare_shadow_workspace(
    tmp_path: Path,
    *,
    previews: list[OrderIntentPreviewRecord],
    bars_by_instrument: dict[str, list[dict[str, object]]],
    account_payload: dict[str, object] | None = None,
    positions_payload: list[dict[str, object]] | None = None,
    market_payload: list[dict[str, object]] | None = None,
    strategy_run_id: str = "strategy_shadow",
    execution_task_id: str = "task_shadow",
    prediction_run_id: str = "model_shadow",
    source_qlib_export_run_id: str = "qlib_shadow",
    source_standard_build_run_id: str = "build_shadow",
    extra_instrument_rows: list[str] | None = None,
    extra_mapping_rows: list[str] | None = None,
) -> tuple[Path, str]:
    workspace = prepare_research_workspace(tmp_path)
    if extra_instrument_rows:
        instruments_path = workspace / "data" / "master" / "bootstrap" / "instruments.csv"
        existing = instruments_path.read_text(encoding="utf-8").rstrip("\n")
        instruments_path.write_text(
            existing + "\n" + "\n".join(extra_instrument_rows) + "\n",
            encoding="utf-8",
        )
    if extra_mapping_rows:
        mappings_path = workspace / "data" / "master" / "bootstrap" / "instrument_keys.csv"
        existing = mappings_path.read_text(encoding="utf-8").rstrip("\n")
        mappings_path.write_text(
            existing + "\n" + "\n".join(extra_mapping_rows) + "\n",
            encoding="utf-8",
        )
    catalog = InstrumentCatalog.from_bootstrap_dir(workspace / "data" / "master" / "bootstrap")
    trade_date = previews[0].trade_date
    sample_root = workspace / "data" / "bootstrap" / "execution_sample"
    sample_root.mkdir(parents=True, exist_ok=True)

    effective_account_payload = account_payload or {
        "account_id": "demo_equity",
        "available_cash": "20000.00",
        "frozen_cash": "0.00",
        "nav": "20000.00",
    }
    effective_positions_payload = positions_payload or []
    effective_market_payload = market_payload or [
        {
            "instrument_key": preview.instrument_key,
            "last_price": str(preview.reference_price or Decimal("10.00")),
            "previous_close": str(preview.previous_close or preview.reference_price or Decimal("10.00")),
            "upper_limit": None,
            "lower_limit": None,
            "is_paused": False,
            "exchange_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
            "received_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
        }
        for preview in previews
    ]
    (sample_root / "account_demo.json").write_text(
        json.dumps(effective_account_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (sample_root / "positions_demo.json").write_text(
        json.dumps({"positions": effective_positions_payload}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (sample_root / f"market_snapshot_{trade_date.isoformat()}.json").write_text(
        json.dumps({"market_snapshots": effective_market_payload}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    store = PlanningArtifactStore(workspace)
    target_records = []
    seen_instruments: set[str] = set()
    for rank, preview in enumerate(previews, start=1):
        if preview.instrument_key in seen_instruments:
            continue
        seen_instruments.add(preview.instrument_key)
        resolved = catalog.resolve(instrument_key=preview.instrument_key)
        target_records.append(
            ApprovedTargetWeightRecord(
                strategy_run_id=strategy_run_id,
                prediction_run_id=prediction_run_id,
                account_id=preview.account_id,
                basket_id=preview.basket_id,
                trade_date=preview.trade_date,
                instrument_key=preview.instrument_key,
                qlib_symbol=resolved.mapping.qlib_symbol,
                score=1.0 / rank,
                rank=rank,
                target_weight=Decimal("0.10"),
                status=ApprovedTargetWeightStatus.APPROVED,
                approved_by="test",
                approved_at=datetime(2026, 3, 26, 9, 20, tzinfo=CN_TZ),
                model_version="baseline_v1",
                feature_set_version="baseline_features_v1",
                config_hash="cfg_shadow",
                source_qlib_export_run_id=source_qlib_export_run_id,
                source_standard_build_run_id=source_standard_build_run_id,
                created_at=datetime(2026, 3, 26, 9, 20, tzinfo=CN_TZ),
            )
        )
    target_manifest = store.save_target_weights(
        manifest=ApprovedTargetWeightManifest(
            strategy_run_id=strategy_run_id,
            prediction_run_id=prediction_run_id,
            account_id=previews[0].account_id,
            basket_id=previews[0].basket_id,
            trade_date=trade_date,
            row_count=len(target_records),
            status=ApprovedTargetWeightStatus.APPROVED,
            approved_by="test",
            approved_at=datetime(2026, 3, 26, 9, 20, tzinfo=CN_TZ),
            model_version="baseline_v1",
            feature_set_version="baseline_features_v1",
            config_hash="cfg_shadow",
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
            created_at=datetime(2026, 3, 26, 9, 20, tzinfo=CN_TZ),
            file_path="pending",
            file_hash="pending",
            prediction_path="predictions/prediction.parquet",
            prediction_file_hash="prediction_hash",
        ),
        records=target_records,
    )
    store.save_execution_task(
        task=ExecutionTaskRecord(
            execution_task_id=execution_task_id,
            strategy_run_id=strategy_run_id,
            account_id=previews[0].account_id,
            basket_id=previews[0].basket_id,
            trade_date=trade_date,
            exec_style="close_reference",
            status=ExecutionTaskStatus.PLANNED,
            created_at=datetime(2026, 3, 26, 9, 25, tzinfo=CN_TZ),
            source_target_weight_hash=target_manifest.file_hash,
            planner_config_hash="planner_shadow",
            plan_only=True,
            summary_json={"preview_count": len(previews)},
            source_qlib_export_run_id=source_qlib_export_run_id,
            source_standard_build_run_id=source_standard_build_run_id,
        ),
        previews=previews,
    )

    standard_root = workspace / "data" / "standard" / "bars_1m"
    for instrument_key, rows in bars_by_instrument.items():
        resolved = catalog.resolve(instrument_key=instrument_key)
        normalized_rows = [
            {
                "instrument_key": instrument_key,
                "symbol": resolved.instrument.symbol,
                "exchange": resolved.instrument.exchange.value,
                "vt_symbol": resolved.mapping.vt_symbol,
                "bar_dt": _bar_dt(row["bar_dt"]),
                "open": row.get("open", row["close"]),
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row.get("volume", 1000),
                "turnover": row.get("turnover", 100000),
                "trade_count": row.get("trade_count", 10),
                "vwap": row.get("vwap", row["close"]),
                "session_tag": row.get("session_tag", "continuous"),
                "is_synthetic": row.get("is_synthetic", False),
                "build_run_id": row.get("build_run_id", source_standard_build_run_id),
            }
            for row in rows
        ]
        write_partition_frame(
            normalized_rows,
            base_dir=standard_root,
            trade_date=trade_date,
            exchange=resolved.instrument.exchange.value,
            symbol=resolved.instrument.symbol,
            file_stem=f"bars_1m_{source_standard_build_run_id}",
        )
    return workspace, execution_task_id


def make_preview(
    *,
    instrument_key: str,
    symbol: str,
    exchange: str,
    side: str,
    quantity: int,
    reference_price: str,
    previous_close: str,
    created_at: datetime,
    execution_task_id: str = "task_shadow",
    strategy_run_id: str = "strategy_shadow",
    account_id: str = "demo_equity",
    basket_id: str = "baseline_long_only",
    trade_date: date = date(2026, 3, 26),
    validation_status: ValidationStatus = ValidationStatus.ACCEPTED,
    validation_reason: str | None = None,
    source_standard_build_run_id: str = "build_shadow",
) -> OrderIntentPreviewRecord:
    signed_delta = quantity if side == "BUY" else -quantity
    return OrderIntentPreviewRecord(
        execution_task_id=execution_task_id,
        strategy_run_id=strategy_run_id,
        account_id=account_id,
        basket_id=basket_id,
        trade_date=trade_date,
        instrument_key=instrument_key,
        symbol=symbol,
        exchange=exchange,
        side=side,
        current_quantity=0,
        sellable_quantity=0 if side == "BUY" else quantity,
        target_quantity=quantity if side == "BUY" else 0,
        delta_quantity=signed_delta,
        reference_price=Decimal(reference_price),
        previous_close=Decimal(previous_close),
        estimated_notional=(Decimal(reference_price) * Decimal(quantity)).quantize(Decimal("0.01")),
        estimated_cost=Decimal("5.00"),
        validation_status=validation_status,
        validation_reason=validation_reason,
        session_tag="continuous",
        created_at=created_at,
        source_target_weight_hash="target_hash",
        source_qlib_export_run_id="qlib_shadow",
        source_standard_build_run_id=source_standard_build_run_id,
        estimated_cost_breakdown=None,
    )


def _bar_dt(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
