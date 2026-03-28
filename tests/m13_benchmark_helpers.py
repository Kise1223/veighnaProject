from __future__ import annotations

from pathlib import Path

from libs.analytics.benchmark_attribution import build_benchmark_reference, run_benchmark_analytics
from libs.analytics.portfolio import run_portfolio_analytics
from tests.m12_portfolio_helpers import prepare_m12_workspace


def prepare_m13_workspace(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    workspace, ids = prepare_m12_workspace(tmp_path)
    ids["paper_portfolio_run_id"] = str(
        run_portfolio_analytics(project_root=workspace, paper_run_id=ids["paper_run_id"])[
            "portfolio_analytics_run_id"
        ]
    )
    ids["bars_portfolio_run_id"] = str(
        run_portfolio_analytics(project_root=workspace, shadow_run_id=ids["bars_shadow_run_id"])[
            "portfolio_analytics_run_id"
        ]
    )
    ids["ticks_crossing_portfolio_run_id"] = str(
        run_portfolio_analytics(
            project_root=workspace,
            shadow_run_id=ids["ticks_crossing_run_id"],
        )["portfolio_analytics_run_id"]
    )
    ids["ticks_partial_day_portfolio_run_id"] = str(
        run_portfolio_analytics(
            project_root=workspace,
            shadow_run_id=ids["ticks_partial_day_run_id"],
        )["portfolio_analytics_run_id"]
    )
    ids["ticks_partial_ioc_portfolio_run_id"] = str(
        run_portfolio_analytics(
            project_root=workspace,
            shadow_run_id=ids["ticks_partial_ioc_run_id"],
        )["portfolio_analytics_run_id"]
    )
    ids["benchmark_run_id"] = str(
        build_benchmark_reference(
            project_root=workspace,
            portfolio_analytics_run_id=ids["ticks_partial_day_portfolio_run_id"],
            source_type="equal_weight_target_universe",
        )["benchmark_run_id"]
    )
    ids["paper_benchmark_analytics_run_id"] = str(
        run_benchmark_analytics(
            project_root=workspace,
            paper_run_id=ids["paper_run_id"],
            benchmark_run_id=ids["benchmark_run_id"],
        )["benchmark_analytics_run_id"]
    )
    ids["bars_benchmark_analytics_run_id"] = str(
        run_benchmark_analytics(
            project_root=workspace,
            shadow_run_id=ids["bars_shadow_run_id"],
            benchmark_run_id=ids["benchmark_run_id"],
        )["benchmark_analytics_run_id"]
    )
    ids["ticks_crossing_benchmark_analytics_run_id"] = str(
        run_benchmark_analytics(
            project_root=workspace,
            shadow_run_id=ids["ticks_crossing_run_id"],
            benchmark_run_id=ids["benchmark_run_id"],
        )["benchmark_analytics_run_id"]
    )
    ids["ticks_partial_day_benchmark_analytics_run_id"] = str(
        run_benchmark_analytics(
            project_root=workspace,
            shadow_run_id=ids["ticks_partial_day_run_id"],
            benchmark_run_id=ids["benchmark_run_id"],
        )["benchmark_analytics_run_id"]
    )
    ids["ticks_partial_ioc_benchmark_analytics_run_id"] = str(
        run_benchmark_analytics(
            project_root=workspace,
            shadow_run_id=ids["ticks_partial_ioc_run_id"],
            benchmark_run_id=ids["benchmark_run_id"],
        )["benchmark_analytics_run_id"]
    )
    return workspace, ids
