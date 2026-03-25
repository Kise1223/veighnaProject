from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from apps.research_qlib.workflow import run_daily_inference, train_baseline_workflow
from libs.research.artifacts import ResearchArtifactStore
from libs.research.lineage import resolve_prediction_lineage
from scripts.build_research_sample import build_research_sample
from tests.research_helpers import prepare_research_workspace


def test_train_and_inference_are_idempotent_and_lineage_is_traceable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace = prepare_research_workspace(tmp_path)
    build_research_sample(project_root=workspace)

    first_train = train_baseline_workflow(project_root=workspace)
    second_train = train_baseline_workflow(project_root=workspace)
    assert first_train["run_id"] == second_train["run_id"]
    assert second_train["reused"] is True

    first_infer = run_daily_inference(project_root=workspace, trade_date=date(2026, 3, 26))
    second_infer = run_daily_inference(project_root=workspace, trade_date=date(2026, 3, 26))
    assert first_infer["run_id"] == second_infer["run_id"]
    assert second_infer["reused"] is True

    store = ResearchArtifactStore(workspace, workspace / "data" / "research")
    lineage = resolve_prediction_lineage(
        store,
        trade_date=date(2026, 3, 26),
        run_id=str(first_train["run_id"]),
    )
    assert lineage.source_standard_build_run_id == "research_sample_standard_v1"
    assert lineage.source_qlib_export_run_id == "research_sample_qlib_day_v1"
