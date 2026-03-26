from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from apps.trade_server.app.paper.runner import run_paper_execution
from libs.execution.artifacts import ExecutionArtifactStore
from tests.execution_helpers import prepare_m7_workspace


def test_reconcile_report_matches_paper_orders_trades_and_snapshots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    del execution_task_id
    result = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
    )
    store = ExecutionArtifactStore(workspace)
    orders = store.load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    trades = store.load_paper_trades(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    account = store.load_account_snapshot(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=result.paper_run_id,
    )
    positions = store.load_position_snapshots(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        paper_run_id=result.paper_run_id,
    )
    report = store.load_reconcile_report(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )

    trade_cost_total = sum(
        (Decimal(str(item["cost_breakdown_json"]["total"])) for item in trades.to_dict(orient="records")),
        Decimal("0"),
    ).quantize(Decimal("0.01"))
    market_value_total = (
        positions["market_value"].sum().quantize(Decimal("0.01"))
        if not positions.empty
        else Decimal("0.00")
    )

    assert report.planned_order_count == len(orders)
    assert report.filled_order_count == len(set(trades["order_id"].tolist()))
    assert report.realized_cost_total == trade_cost_total
    assert account.net_liquidation_end == (account.cash_end + account.market_value_end).quantize(
        Decimal("0.01")
    )
    assert account.market_value_end == market_value_total
    assert report.estimated_cost_total == sum(
        (Decimal(str(item)) for item in orders["estimated_cost"].tolist()),
        Decimal("0"),
    ).quantize(Decimal("0.01"))
