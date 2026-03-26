from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash
from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.rebalance import plan_rebalance
from libs.planning.schemas import (
    ApprovedTargetWeightManifest,
    ApprovedTargetWeightRecord,
    ApprovedTargetWeightStatus,
    ValidationStatus,
)
from tests.research_helpers import prepare_research_workspace


def test_rebalance_planner_splits_odd_lot_sells_rounds_buys_and_estimates_costs(
    tmp_path: Path,
) -> None:
    workspace = prepare_research_workspace(tmp_path)
    _seed_target_weights(workspace)

    result = plan_rebalance(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
    )
    store = PlanningArtifactStore(workspace)
    previews = store.load_order_intents(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=str(result["execution_task_id"]),
    )
    accepted = previews[previews["validation_status"] == ValidationStatus.ACCEPTED.value]

    sell_deltas = sorted(
        accepted[accepted["instrument_key"] == "EQ_SH_600000"]["delta_quantity"].tolist()
    )
    assert sell_deltas == [-200, -50]
    buy_qty = accepted[accepted["instrument_key"] == "EQ_SZ_000001"]["delta_quantity"].iloc[0]
    assert buy_qty == 700
    assert (accepted["estimated_cost"] > 0).all()

    rejected = previews[previews["validation_status"] == ValidationStatus.REJECTED.value]
    assert any("insufficient_cash_for_planned_buy" in str(item) for item in rejected["validation_reason"].tolist())


def test_rebalance_planner_rejects_missing_previous_close_and_sellable_shortage(
    tmp_path: Path,
) -> None:
    workspace = prepare_research_workspace(tmp_path)
    _seed_target_weights(workspace)
    _write_sellable_shortage_positions(workspace)
    _remove_previous_close(workspace, instrument_key="EQ_SZ_000001")

    result = plan_rebalance(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        force=True,
    )
    store = PlanningArtifactStore(workspace)
    previews = store.load_order_intents(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=str(result["execution_task_id"]),
    )
    reasons = previews["validation_reason"].dropna().tolist()
    assert "sell_quantity_exceeds_sellable" in reasons
    assert "previous_close_missing" in reasons


def _seed_target_weights(workspace: Path) -> None:
    store = PlanningArtifactStore(workspace)
    created_at = ensure_cn_aware(datetime(2026, 3, 26, 9, 0))
    config_hash = stable_hash({"fixture": "m6_rebalance"})
    records = [
        ApprovedTargetWeightRecord(
            strategy_run_id="strategy_fixture",
            prediction_run_id="model_fixture",
            account_id="demo_equity",
            basket_id="baseline_long_only",
            trade_date=date(2026, 3, 26),
            instrument_key="EQ_SZ_000001",
            qlib_symbol="SZ000001",
            score=0.9,
            rank=1,
            target_weight=Decimal("0.45"),
            status=ApprovedTargetWeightStatus.APPROVED,
            approved_by="fixture",
            approved_at=created_at,
            model_version="baseline_linear_v1",
            feature_set_version="baseline_features_v1",
            config_hash=config_hash,
            source_qlib_export_run_id="research_sample_qlib_day_v1",
            source_standard_build_run_id="research_sample_standard_v1",
            created_at=created_at,
        ),
        ApprovedTargetWeightRecord(
            strategy_run_id="strategy_fixture",
            prediction_run_id="model_fixture",
            account_id="demo_equity",
            basket_id="baseline_long_only",
            trade_date=date(2026, 3, 26),
            instrument_key="ETF_SH_510300",
            qlib_symbol="SH510300",
            score=0.8,
            rank=2,
            target_weight=Decimal("0.45"),
            status=ApprovedTargetWeightStatus.APPROVED,
            approved_by="fixture",
            approved_at=created_at,
            model_version="baseline_linear_v1",
            feature_set_version="baseline_features_v1",
            config_hash=config_hash,
            source_qlib_export_run_id="research_sample_qlib_day_v1",
            source_standard_build_run_id="research_sample_standard_v1",
            created_at=created_at,
        ),
    ]
    manifest = ApprovedTargetWeightManifest(
        strategy_run_id="strategy_fixture",
        prediction_run_id="model_fixture",
        account_id="demo_equity",
        basket_id="baseline_long_only",
        trade_date=date(2026, 3, 26),
        row_count=len(records),
        status=ApprovedTargetWeightStatus.APPROVED,
        approved_by="fixture",
        approved_at=created_at,
        model_version="baseline_linear_v1",
        feature_set_version="baseline_features_v1",
        config_hash=config_hash,
        source_qlib_export_run_id="research_sample_qlib_day_v1",
        source_standard_build_run_id="research_sample_standard_v1",
        created_at=created_at,
        file_path="pending",
        file_hash="pending",
        prediction_path="data/research/predictions/trade_date=2026-03-26/run_id=model_fixture/predictions.parquet",
        prediction_file_hash=stable_hash({"fixture": "prediction"}),
    )
    store.save_target_weights(manifest=manifest, records=records)


def _write_sellable_shortage_positions(workspace: Path) -> None:
    path = workspace / "data" / "bootstrap" / "execution_sample" / "positions_demo.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["positions"][0]["sellable_quantity"] = 200
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _remove_previous_close(workspace: Path, *, instrument_key: str) -> None:
    path = workspace / "data" / "bootstrap" / "execution_sample" / "market_snapshot_2026-03-26.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    for item in payload["market_snapshots"]:
        if item["instrument_key"] == instrument_key:
            item["previous_close"] = None
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
