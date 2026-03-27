from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from apps.trade_server.app.paper.runner import run_paper_execution
from libs.execution.artifacts import ExecutionArtifactStore
from tests.execution_helpers import prepare_m7_workspace


def test_explicit_position_cost_basis_path_overrides_companion_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    default_result = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )

    positions_path = workspace / "data" / "bootstrap" / "execution_sample" / "positions_demo.json"
    positions_payload = json.loads(positions_path.read_text(encoding="utf-8"))
    custom_cost_basis_path = workspace / "custom_position_cost_basis.json"
    custom_cost_basis_path.write_text(
        json.dumps(
            {
                "positions": [
                    {"instrument_key": item["instrument_key"], "avg_price": "1.00"}
                    for item in positions_payload["positions"]
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    explicit_result = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        account_snapshot_path=workspace / "data" / "bootstrap" / "execution_sample" / "account_demo.json",
        positions_path=positions_path,
        market_snapshot_path=workspace
        / "data"
        / "bootstrap"
        / "execution_sample"
        / "market_snapshot_2026-03-26.json",
        position_cost_basis_path=custom_cost_basis_path,
    )

    store = ExecutionArtifactStore(workspace)
    default_account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=default_result.paper_run_id,
    )
    explicit_account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=explicit_result.paper_run_id,
    )

    assert explicit_result.paper_run_id != default_result.paper_run_id
    assert explicit_account.realized_pnl != default_account.realized_pnl


def test_companion_position_cost_basis_still_applies_without_explicit_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    default_result = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    explicit_same_inputs = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        account_snapshot_path=workspace / "data" / "bootstrap" / "execution_sample" / "account_demo.json",
        positions_path=workspace / "data" / "bootstrap" / "execution_sample" / "positions_demo.json",
        market_snapshot_path=workspace
        / "data"
        / "bootstrap"
        / "execution_sample"
        / "market_snapshot_2026-03-26.json",
    )
    store = ExecutionArtifactStore(workspace)
    default_account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=default_result.paper_run_id,
    )
    explicit_account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=explicit_same_inputs.paper_run_id,
    )

    assert explicit_same_inputs.paper_run_id == default_result.paper_run_id
    assert explicit_same_inputs.reused is True
    assert explicit_account.realized_pnl == default_account.realized_pnl
