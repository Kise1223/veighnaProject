from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.shadow_artifacts import ShadowArtifactStore
from tests.shadow_helpers import make_preview, prepare_shadow_workspace


def test_shadow_session_prefers_market_snapshot_previous_close_for_limit_price(tmp_path: Path) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.20",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={
            "EQ_SZ_000001": [
                {
                    "bar_dt": datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                    "open": 9.90,
                    "high": 10.30,
                    "low": 9.70,
                    "close": 9.85,
                }
            ]
        },
        market_payload=[
            {
                "instrument_key": "EQ_SZ_000001",
                "last_price": "9.85",
                "previous_close": "9.80",
                "upper_limit": None,
                "lower_limit": None,
                "is_paused": False,
                "exchange_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
                "received_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
            }
        ],
    )
    config_path = workspace / "configs" / "execution" / "shadow_previous_close.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("limit_price_source: previous_close\n", encoding="utf-8")

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        config_path=Path("configs/execution/shadow_previous_close.yaml"),
    )

    shadow_store = ShadowArtifactStore(workspace)
    execution_store = ExecutionArtifactStore(workspace)
    order_events = shadow_store.load_order_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    paper_orders = execution_store.load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    paper_trades = execution_store.load_paper_trades(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )

    created_event = order_events[order_events["event_type"] == "created"].iloc[0]
    order = paper_orders.iloc[0]
    trade = paper_trades.iloc[0]

    assert created_event["limit_price"] == Decimal("9.80")
    assert order["limit_price"] == Decimal("9.80")
    assert order["previous_close"] == Decimal("9.80")
    assert trade["price"] == Decimal("9.80")


def test_shadow_session_default_reference_price_behavior_is_unchanged(tmp_path: Path) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.20",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={
            "EQ_SZ_000001": [
                {
                    "bar_dt": datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                    "open": 9.90,
                    "high": 10.30,
                    "low": 9.70,
                    "close": 9.85,
                }
            ]
        },
        market_payload=[
            {
                "instrument_key": "EQ_SZ_000001",
                "last_price": "9.85",
                "previous_close": "9.80",
                "upper_limit": None,
                "lower_limit": None,
                "is_paused": False,
                "exchange_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
                "received_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
            }
        ],
    )

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )

    shadow_store = ShadowArtifactStore(workspace)
    execution_store = ExecutionArtifactStore(workspace)
    order_events = shadow_store.load_order_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    paper_orders = execution_store.load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )
    paper_trades = execution_store.load_paper_trades(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    )

    created_event = order_events[order_events["event_type"] == "created"].iloc[0]
    order = paper_orders.iloc[0]
    trade = paper_trades.iloc[0]

    assert created_event["limit_price"] == Decimal("10.20")
    assert order["limit_price"] == Decimal("10.20")
    assert order["previous_close"] == Decimal("9.80")
    assert trade["price"] == Decimal("10.20")
