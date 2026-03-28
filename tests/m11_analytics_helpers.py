from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.paper.runner import run_paper_execution
from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def prepare_m11_workspace(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=700,
        reference_price="12.00",
        previous_close="12.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={
            "EQ_SZ_000001": [
                {
                    "bar_dt": datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                    "open": "12.00",
                    "high": "12.02",
                    "low": "11.99",
                    "close": "12.00",
                    "volume": 5000,
                    "turnover": 60000,
                    "trade_count": 15,
                    "vwap": "12.00",
                }
            ]
        },
    )
    tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="m11_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="12.00",
                bid_price_1="11.99",
                ask_price_1="12.00",
                bid_volume_1="300",
                ask_volume_1="300",
            ),
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 32, tzinfo=CN_TZ),
                last_price="12.00",
                bid_price_1="11.99",
                ask_price_1="12.00",
                bid_volume_1="400",
                ask_volume_1="400",
            ),
        ],
    )
    paper = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )
    bars = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="bars_1m",
    )
    ticks_full = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=tick_path,
    )
    ticks_partial_day = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="DAY",
        tick_input_path=tick_path,
    )
    ticks_partial_ioc = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="l1_partial_fill_v1",
        time_in_force="IOC",
        tick_input_path=tick_path,
    )
    return workspace, {
        "execution_task_id": execution_task_id,
        "paper_run_id": paper.paper_run_id,
        "bars_shadow_run_id": bars.shadow_run_id,
        "ticks_crossing_run_id": ticks_full.shadow_run_id,
        "ticks_partial_day_run_id": ticks_partial_day.shadow_run_id,
        "ticks_partial_ioc_run_id": ticks_partial_ioc.shadow_run_id,
    }
