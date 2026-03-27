from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.shadow_artifacts import ShadowArtifactStore
from tests.shadow_helpers import make_preview, prepare_shadow_workspace


def test_shadow_session_progresses_working_fill_and_expiry(tmp_path: Path) -> None:
    previews = [
        make_preview(
            instrument_key="EQ_SZ_000001",
            symbol="000001",
            exchange="SZSE",
            side="BUY",
            quantity=100,
            reference_price="10.00",
            previous_close="10.00",
            created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
        ),
        make_preview(
            instrument_key="EQ_SH_600000",
            symbol="600000",
            exchange="SSE",
            side="BUY",
            quantity=100,
            reference_price="9.00",
            previous_close="10.00",
            created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
        ),
    ]
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=previews,
        bars_by_instrument={
            "EQ_SZ_000001": [
                {
                    "bar_dt": datetime(2026, 3, 26, 11, 31, tzinfo=CN_TZ),
                    "open": 10.00,
                    "high": 10.05,
                    "low": 9.95,
                    "close": 10.01,
                },
                {
                    "bar_dt": datetime(2026, 3, 26, 13, 1, tzinfo=CN_TZ),
                    "open": 10.01,
                    "high": 10.10,
                    "low": 9.90,
                    "close": 10.02,
                },
            ],
            "EQ_SH_600000": [
                {
                    "bar_dt": datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                    "open": 10.10,
                    "high": 10.30,
                    "low": 10.05,
                    "close": 10.20,
                },
                {
                    "bar_dt": datetime(2026, 3, 26, 14, 55, tzinfo=CN_TZ),
                    "open": 10.20,
                    "high": 10.40,
                    "low": 10.10,
                    "close": 10.30,
                },
            ],
        },
    )

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )

    store = ShadowArtifactStore(workspace)
    events = store.load_order_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    fills = store.load_fill_events(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )
    report = store.load_report(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        shadow_run_id=result.shadow_run_id,
    )

    assert set(events["event_type"]) >= {"created", "working", "filled", "expired_end_of_session"}
    assert fills.iloc[0]["symbol"] == "000001"
    assert fills.iloc[0]["fill_dt"] == datetime(2026, 3, 26, 13, 1, tzinfo=CN_TZ)
    assert report.filled_order_count == 1
    assert report.expired_order_count == 1
    assert report.execution_task_id == execution_task_id

