from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.analytics.artifacts import ExecutionAnalyticsArtifactStore
from libs.analytics.tca import run_execution_tca
from libs.common.time import CN_TZ
from tests.m11_analytics_helpers import prepare_m11_workspace
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_execution_tca_supports_paper_and_shadow_runs(tmp_path: Path) -> None:
    workspace, ids = prepare_m11_workspace(tmp_path)

    paper_result = run_execution_tca(
        project_root=workspace,
        paper_run_id=ids["paper_run_id"],
    )
    shadow_result = run_execution_tca(
        project_root=workspace,
        shadow_run_id=ids["ticks_partial_day_run_id"],
    )

    store = ExecutionAnalyticsArtifactStore(workspace)
    paper_summary = store.load_analytics_summary(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        analytics_run_id=str(paper_result["analytics_run_id"]),
    )
    shadow_rows = store.load_analytics_rows(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        analytics_run_id=str(shadow_result["analytics_run_id"]),
    )
    shadow_row = shadow_rows.iloc[0]

    assert paper_summary.order_count == 1
    assert paper_summary.filled_order_count == 1
    assert shadow_row["partial_fill_count"] == 1
    assert shadow_row["fill_rate"] == 1.0
    assert shadow_row["replay_mode"] == "ticks_l1"
    assert shadow_row["fill_model_name"] == "l1_partial_fill_v1"


def test_execution_tca_handles_expired_no_fill_runs(tmp_path: Path) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.00",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={},
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="m11_no_fill_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="10.10",
                bid_price_1="10.09",
                ask_price_1="10.11",
                bid_volume_1="100",
                ask_volume_1="100",
            )
        ],
    )
    shadow = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=tick_path,
    )

    analytics = run_execution_tca(
        project_root=workspace,
        shadow_run_id=shadow.shadow_run_id,
    )
    rows = ExecutionAnalyticsArtifactStore(workspace).load_analytics_rows(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        analytics_run_id=str(analytics["analytics_run_id"]),
    )
    row = rows.iloc[0]

    assert row["session_end_status"] == "expired_end_of_session"
    assert row["filled_quantity"] == 0
    assert row["implementation_shortfall"] == Decimal("0.00")


def test_execution_tca_requires_latest_when_multiple_sources_match(tmp_path: Path) -> None:
    workspace, ids = prepare_m11_workspace(tmp_path)

    try:
        run_execution_tca(
            project_root=workspace,
            trade_date=date(2026, 3, 26),
            account_id="demo_equity",
            basket_id="baseline_long_only",
        )
    except ValueError as exc:
        assert "multiple execution sources match" in str(exc)
    else:
        raise AssertionError("expected multi-source selection to require --latest")

    latest_shadow = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=ids["execution_task_id"],
        market_replay_mode="ticks_l1",
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="IOC",
        force=True,
    )
    latest = run_execution_tca(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        latest=True,
    )

    assert latest["source_type"] in {"paper_run", "shadow_run"}
    assert {
        latest_shadow.paper_run_id,
        latest_shadow.shadow_run_id,
    } & set(latest["source_run_ids"])
