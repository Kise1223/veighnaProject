from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from apps.trade_server.app.paper.runner import run_paper_execution
from scripts import reconcile_paper_run
from tests.execution_helpers import prepare_m7_workspace


def test_reconcile_cli_allows_single_matching_run_without_selector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    result = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    monkeypatch.setattr(reconcile_paper_run, "ROOT", workspace)

    exit_code = reconcile_paper_run.main(
        [
            "--trade-date",
            "2026-03-26",
            "--account-id",
            "demo_equity",
            "--basket-id",
            "baseline_long_only",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["paper_run_id"] == result.paper_run_id


def test_reconcile_cli_requires_selector_when_multiple_runs_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    first = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    custom_market_path = workspace / "selector_market_snapshot.json"
    custom_market_path.write_text(
        (
            workspace
            / "data"
            / "bootstrap"
            / "execution_sample"
            / "market_snapshot_2026-03-26.json"
        ).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    payload = json.loads(custom_market_path.read_text(encoding="utf-8"))
    for item in payload["market_snapshots"]:
        item["previous_close"] = "88.88"
        item["last_price"] = "88.88"
    custom_market_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    second = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_snapshot_path=custom_market_path,
    )
    monkeypatch.setattr(reconcile_paper_run, "ROOT", workspace)

    exit_code = reconcile_paper_run.main(
        [
            "--trade-date",
            "2026-03-26",
            "--account-id",
            "demo_equity",
            "--basket-id",
            "baseline_long_only",
        ]
    )
    captured = capsys.readouterr()

    assert first.paper_run_id != second.paper_run_id
    assert exit_code == 2
    assert "pass --paper-run-id or --latest" in captured.err


def test_reconcile_cli_supports_paper_run_id_and_execution_task_id_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    first = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    custom_market_path = workspace / "latest_market_snapshot.json"
    payload = json.loads(
        (
            workspace
            / "data"
            / "bootstrap"
            / "execution_sample"
            / "market_snapshot_2026-03-26.json"
        ).read_text(encoding="utf-8")
    )
    for item in payload["market_snapshots"]:
        item["previous_close"] = "66.66"
        item["last_price"] = "66.66"
    custom_market_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    second = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_snapshot_path=custom_market_path,
    )
    monkeypatch.setattr(reconcile_paper_run, "ROOT", workspace)

    first_exit = reconcile_paper_run.main(
        [
            "--trade-date",
            "2026-03-26",
            "--account-id",
            "demo_equity",
            "--basket-id",
            "baseline_long_only",
            "--paper-run-id",
            first.paper_run_id,
        ]
    )
    first_payload = json.loads(capsys.readouterr().out)
    latest_exit = reconcile_paper_run.main(
        [
            "--trade-date",
            "2026-03-26",
            "--account-id",
            "demo_equity",
            "--basket-id",
            "baseline_long_only",
            "--execution-task-id",
            execution_task_id,
            "--latest",
        ]
    )
    latest_payload = json.loads(capsys.readouterr().out)

    assert first_exit == 0
    assert latest_exit == 0
    assert first_payload["paper_run_id"] == first.paper_run_id
    assert latest_payload["paper_run_id"] == second.paper_run_id
