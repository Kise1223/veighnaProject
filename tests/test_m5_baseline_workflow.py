from __future__ import annotations

from pathlib import Path

import pytest

from apps.research_qlib.bootstrap import init_qlib, load_runtime_config
from apps.research_qlib.workflow import (
    check_symbol_and_calendar_consistency,
    run_daily_inference,
    train_baseline_workflow,
)
from libs.research.artifacts import ResearchArtifactStore
from scripts.build_research_sample import build_research_sample
from tests.research_helpers import prepare_research_workspace


def test_qlib_init_dataset_train_and_inference_smoke(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace = prepare_research_workspace(tmp_path)
    build_research_sample(project_root=workspace)

    runtime_config = load_runtime_config(workspace / "configs" / "qlib" / "base.yaml")
    init_qlib(workspace, runtime_config)

    consistency = check_symbol_and_calendar_consistency(project_root=workspace)
    assert consistency["status"] == "passed"
    assert consistency["instrument_count"] >= 3

    train_result = train_baseline_workflow(project_root=workspace)
    assert train_result["status"] == "success"
    assert train_result["reused"] is False

    inference_result = run_daily_inference(project_root=workspace, trade_date=__import__("datetime").date(2026, 3, 26))
    assert inference_result["row_count"] >= 3
    assert inference_result["reused"] is False

    store = ResearchArtifactStore(workspace, workspace / "data" / "research")
    run = store.load_run(str(train_result["run_id"]))
    assert run.metrics_json["train_rows"] >= 40
    assert run.source_qlib_export_run_id == "research_sample_qlib_day_v1"
