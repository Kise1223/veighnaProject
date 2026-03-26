from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from apps.research_qlib import workflow as research_workflow
from libs.research.artifacts import ResearchArtifactStore
from libs.research.lineage import resolve_prediction_lineage
from libs.research.schemas import ResearchRunStatus
from scripts.build_research_sample import build_research_sample
from tests.research_helpers import prepare_research_workspace


def test_successful_run_is_reused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace = prepare_research_workspace(tmp_path)
    build_research_sample(project_root=workspace)

    first = research_workflow.train_baseline_workflow(project_root=workspace)
    second = research_workflow.train_baseline_workflow(project_root=workspace)

    assert first["run_id"] == second["run_id"]
    assert second["reused"] is True


def test_failed_run_can_be_retrained_and_lineage_is_preserved(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace = prepare_research_workspace(tmp_path)
    build_research_sample(project_root=workspace)

    original_fit = research_workflow.fit_baseline_model

    def fail_once(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("boom")

    monkeypatch.setattr(research_workflow, "fit_baseline_model", fail_once)
    with pytest.raises(RuntimeError, match="boom"):
        research_workflow.train_baseline_workflow(project_root=workspace)

    store = ResearchArtifactStore(workspace, workspace / "data" / "research")
    failed_run = next(iter(store.list_runs()))
    assert failed_run.status == ResearchRunStatus.FAILED

    monkeypatch.setattr(research_workflow, "fit_baseline_model", original_fit)
    rerun = research_workflow.train_baseline_workflow(project_root=workspace)
    assert rerun["status"] == "success"
    assert rerun["reused"] is False

    run = store.load_run(str(rerun["run_id"]))
    assert run.status == ResearchRunStatus.SUCCESS
    assert (store.run_dir(run.run_id) / "model.json").exists()

    infer = research_workflow.run_daily_inference(project_root=workspace, trade_date=date(2026, 3, 26))
    assert infer["run_id"] == rerun["run_id"]
    lineage = resolve_prediction_lineage(store, trade_date=date(2026, 3, 26), run_id=run.run_id)
    assert lineage.source_standard_build_run_id == "research_sample_standard_v1"
    assert lineage.source_qlib_export_run_id == "research_sample_qlib_day_v1"
