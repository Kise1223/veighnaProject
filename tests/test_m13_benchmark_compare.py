from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.analytics.attribution_artifacts import BenchmarkAttributionArtifactStore
from libs.analytics.benchmark_attribution import build_benchmark_reference, run_benchmark_analytics
from libs.analytics.benchmark_compare import compare_benchmark_analytics
from libs.common.time import CN_TZ
from tests.m13_benchmark_helpers import prepare_m13_workspace
from tests.shadow_helpers import make_preview, prepare_shadow_workspace
from tests.tick_shadow_helpers import make_tick_row, write_tick_shadow_source


def test_benchmark_compare_supports_standard_compare_bases(tmp_path: Path) -> None:
    workspace, ids = prepare_m13_workspace(tmp_path)

    bars_vs_ticks = compare_benchmark_analytics(
        project_root=workspace,
        left_benchmark_analytics_run_id=ids["bars_benchmark_analytics_run_id"],
        right_benchmark_analytics_run_id=ids["ticks_crossing_benchmark_analytics_run_id"],
        compare_basis="bars_vs_ticks",
    )
    full_vs_partial = compare_benchmark_analytics(
        project_root=workspace,
        left_benchmark_analytics_run_id=ids["ticks_crossing_benchmark_analytics_run_id"],
        right_benchmark_analytics_run_id=ids["ticks_partial_day_benchmark_analytics_run_id"],
        compare_basis="full_vs_partial",
    )
    day_vs_ioc = compare_benchmark_analytics(
        project_root=workspace,
        left_benchmark_analytics_run_id=ids["ticks_partial_day_benchmark_analytics_run_id"],
        right_benchmark_analytics_run_id=ids["ticks_partial_ioc_benchmark_analytics_run_id"],
        compare_basis="day_vs_ioc",
    )
    paper_vs_shadow = compare_benchmark_analytics(
        project_root=workspace,
        left_benchmark_analytics_run_id=ids["paper_benchmark_analytics_run_id"],
        right_benchmark_analytics_run_id=ids["bars_benchmark_analytics_run_id"],
        compare_basis="paper_vs_shadow",
    )

    assert bars_vs_ticks["row_count"] >= 4
    assert full_vs_partial["row_count"] >= 4
    assert day_vs_ioc["row_count"] >= 4
    assert paper_vs_shadow["row_count"] >= 4


def test_benchmark_compare_records_unmatched_instruments(tmp_path: Path) -> None:
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
    workspace, left_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={},
        positions_payload=[{"instrument_key": "EQ_SH_600000", "total_quantity": 100, "sellable_quantity": 100}],
        strategy_run_id="left_strategy",
        execution_task_id="left_task",
    )
    left_tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="left_benchmark_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="10.00",
                bid_price_1="9.99",
                ask_price_1="10.00",
                bid_volume_1="100",
                ask_volume_1="100",
            )
        ],
    )
    left_shadow = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=left_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=left_tick_path,
    )
    left_benchmark = build_benchmark_reference(
        project_root=workspace,
        shadow_run_id=left_shadow.shadow_run_id,
        source_type="equal_weight_target_universe",
    )
    left_analytics = run_benchmark_analytics(
        project_root=workspace,
        shadow_run_id=left_shadow.shadow_run_id,
        benchmark_run_id=str(left_benchmark["benchmark_run_id"]),
    )

    _, right_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview.model_copy(update={"strategy_run_id": "right_strategy", "execution_task_id": "right_task"})],
        bars_by_instrument={},
        positions_payload=[{"instrument_key": "ETF_SH_510300", "total_quantity": 100, "sellable_quantity": 100}],
        strategy_run_id="right_strategy",
        execution_task_id="right_task",
    )
    right_tick_path = write_tick_shadow_source(
        workspace,
        trade_date=date(2026, 3, 26),
        filename="right_benchmark_ticks.json",
        ticks=[
            make_tick_row(
                instrument_key="EQ_SZ_000001",
                symbol="000001",
                exchange="SZSE",
                exchange_ts=datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                last_price="10.00",
                bid_price_1="9.99",
                ask_price_1="10.00",
                bid_volume_1="100",
                ask_volume_1="100",
            )
        ],
    )
    right_shadow = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=right_task_id,
        market_replay_mode="ticks_l1",
        tick_fill_model="crossing_full_fill_v1",
        tick_input_path=right_tick_path,
    )
    right_analytics = run_benchmark_analytics(
        project_root=workspace,
        shadow_run_id=right_shadow.shadow_run_id,
        benchmark_run_id=str(left_benchmark["benchmark_run_id"]),
    )
    compare = compare_benchmark_analytics(
        project_root=workspace,
        left_benchmark_analytics_run_id=str(left_analytics["benchmark_analytics_run_id"]),
        right_benchmark_analytics_run_id=str(right_analytics["benchmark_analytics_run_id"]),
        compare_basis="bars_vs_ticks",
    )
    summary = BenchmarkAttributionArtifactStore(workspace).load_benchmark_compare_summary(
        benchmark_compare_run_id=str(compare["benchmark_compare_run_id"])
    )

    assert summary.comparable_count >= 1
    assert summary.summary_json["left_only_count"] >= 1
    assert summary.summary_json["right_only_count"] >= 1
