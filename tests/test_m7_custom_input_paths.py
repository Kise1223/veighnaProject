from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from apps.trade_server.app.paper.runner import run_paper_execution
from libs.execution.artifacts import ExecutionArtifactStore
from tests.execution_helpers import prepare_m7_workspace


def test_run_paper_execution_defaults_to_demo_sample_when_paths_are_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    account_path = workspace / "data" / "bootstrap" / "execution_sample" / "account_demo.json"
    payload = json.loads(account_path.read_text(encoding="utf-8"))
    payload["available_cash"] = "1.00"
    account_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    result = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )

    store = ExecutionArtifactStore(workspace)
    account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=result.paper_run_id,
    )
    assert account.cash_start == Decimal("1.00")


def test_run_paper_execution_prefers_explicit_custom_input_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)

    custom_account_path = workspace / "custom_account.json"
    custom_account_path.write_text(
        json.dumps(
            {
                "account_id": "demo_equity",
                "available_cash": "12345.67",
                "frozen_cash": "0",
                "nav": None,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    custom_positions_path = workspace / "custom_positions.json"
    custom_positions_path.write_text(
        json.dumps({"positions": []}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    default_market_path = (
        workspace
        / "data"
        / "bootstrap"
        / "execution_sample"
        / "market_snapshot_2026-03-26.json"
    )
    market_payload = json.loads(default_market_path.read_text(encoding="utf-8"))
    for item in market_payload["market_snapshots"]:
        item["previous_close"] = "77.77"
        item["last_price"] = "77.77"
    custom_market_path = workspace / "custom_market_snapshot.json"
    custom_market_path.write_text(
        json.dumps(market_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    result = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        account_snapshot_path=custom_account_path,
        positions_path=custom_positions_path,
        market_snapshot_path=custom_market_path,
    )

    store = ExecutionArtifactStore(workspace)
    account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=result.paper_run_id,
    )
    orders = store.load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )

    assert account.cash_start == Decimal("12345.67")
    assert Decimal("77.77") in set(orders["previous_close"].dropna())
    assert "sell_quantity_exceeds_sellable" in set(orders["status_reason"].dropna())
