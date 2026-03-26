from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from libs.planning.artifacts import PlanningArtifactStore
from libs.planning.lineage import resolve_target_weight_lineage
from libs.planning.target_weights import build_target_weights
from libs.research.artifacts import ResearchArtifactStore
from tests.planning_helpers import prepare_m6_workspace


def test_target_weights_select_top_k_and_respect_caps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace = prepare_m6_workspace(tmp_path)
    result = build_target_weights(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        approved_by="local_smoke",
    )
    assert result["reused"] is False

    planning_store = PlanningArtifactStore(workspace)
    frame = planning_store.load_target_weights(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        strategy_run_id=str(result["strategy_run_id"]),
    )
    research_store = ResearchArtifactStore(workspace, workspace / "data" / "research")
    prediction_manifest = next(
        item
        for item in research_store.list_prediction_manifests()
        if item.trade_date == date(2026, 3, 26)
    )
    predictions = research_store.load_predictions(date(2026, 3, 26), prediction_manifest.run_id)
    expected_top = predictions.sort_values(["score", "qlib_symbol"], ascending=[False, True]).head(2)
    assert frame["instrument_key"].tolist() == expected_top["instrument_key"].tolist()
    assert frame["target_weight"].sum() <= Decimal("0.90")
    assert frame["target_weight"].max() <= Decimal("0.45")
    assert (frame["target_weight"] >= Decimal("0")).all()


def test_target_weight_idempotency_and_force(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace = prepare_m6_workspace(tmp_path)

    first = build_target_weights(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        approved_by="local_smoke",
    )
    second = build_target_weights(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        approved_by="local_smoke",
    )
    forced = build_target_weights(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        approved_by="local_smoke",
        force=True,
    )
    assert second["strategy_run_id"] == first["strategy_run_id"]
    assert second["reused"] is True
    assert forced["strategy_run_id"] == first["strategy_run_id"]
    assert forced["reused"] is False

    lineage = resolve_target_weight_lineage(
        PlanningArtifactStore(workspace),
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        strategy_run_id=str(first["strategy_run_id"]),
    )
    assert lineage.prediction_run_id.startswith("model_")
    assert lineage.source_standard_build_run_id == "research_sample_standard_v1"
