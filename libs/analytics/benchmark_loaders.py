"""Load and normalize M12 portfolio analytics plus M13 benchmark artifacts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from libs.analytics.benchmark_artifacts import BenchmarkReferenceArtifactStore
from libs.analytics.benchmark_schemas import (
    BenchmarkReferenceManifest,
    BenchmarkReferenceRunRecord,
    BenchmarkSummaryRecord,
)
from libs.analytics.portfolio import run_portfolio_analytics
from libs.analytics.portfolio_artifacts import PortfolioAnalyticsArtifactStore
from libs.analytics.portfolio_loaders import LoadedPortfolioSource, select_portfolio_source
from libs.analytics.portfolio_schemas import (
    PortfolioAnalyticsManifest,
    PortfolioAnalyticsRunRecord,
    PortfolioSourceType,
    PortfolioSummaryRecord,
)
from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.shadow_artifacts import ShadowArtifactStore


@dataclass(frozen=True)
class LoadedPortfolioAnalyticsSource:
    manifest: PortfolioAnalyticsManifest
    run: PortfolioAnalyticsRunRecord
    positions_frame: Any
    groups_frame: Any
    summary: PortfolioSummaryRecord
    portfolio_source: LoadedPortfolioSource


@dataclass(frozen=True)
class LoadedBenchmarkReference:
    manifest: BenchmarkReferenceManifest
    run: BenchmarkReferenceRunRecord
    weight_frame: Any
    summary: BenchmarkSummaryRecord


def select_portfolio_analytics_source(
    *,
    project_root: Path,
    portfolio_analytics_run_id: str | None = None,
    trade_date: date | None = None,
    account_id: str | None = None,
    basket_id: str | None = None,
    paper_run_id: str | None = None,
    shadow_run_id: str | None = None,
    latest: bool = False,
) -> LoadedPortfolioAnalyticsSource:
    store = PortfolioAnalyticsArtifactStore(project_root)
    if portfolio_analytics_run_id is None:
        result = run_portfolio_analytics(
            project_root=project_root,
            paper_run_id=paper_run_id,
            shadow_run_id=shadow_run_id,
            trade_date=trade_date,
            account_id=account_id,
            basket_id=basket_id,
            latest=latest,
        )
        portfolio_analytics_run_id = str(result["portfolio_analytics_run_id"])
    manifest = _find_portfolio_manifest_by_id(
        store=store,
        portfolio_analytics_run_id=portfolio_analytics_run_id,
    )
    run = store.load_portfolio_run(
        trade_date=manifest.trade_date,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        portfolio_analytics_run_id=manifest.portfolio_analytics_run_id,
    )
    positions_frame = store.load_position_rows(
        trade_date=manifest.trade_date,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        portfolio_analytics_run_id=manifest.portfolio_analytics_run_id,
    )
    groups_frame = store.load_group_rows(
        trade_date=manifest.trade_date,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        portfolio_analytics_run_id=manifest.portfolio_analytics_run_id,
    )
    summary = store.load_portfolio_summary(
        trade_date=manifest.trade_date,
        account_id=manifest.account_id,
        basket_id=manifest.basket_id,
        portfolio_analytics_run_id=manifest.portfolio_analytics_run_id,
    )
    portfolio_source = _resolve_portfolio_source_for_manifest(
        project_root=project_root,
        manifest=manifest,
    )
    return LoadedPortfolioAnalyticsSource(
        manifest=manifest,
        run=run,
        positions_frame=positions_frame,
        groups_frame=groups_frame,
        summary=summary,
        portfolio_source=portfolio_source,
    )


def load_benchmark_reference(
    *,
    project_root: Path,
    benchmark_run_id: str,
) -> LoadedBenchmarkReference:
    store = BenchmarkReferenceArtifactStore(project_root)
    manifest = _find_benchmark_manifest_by_id(store=store, benchmark_run_id=benchmark_run_id)
    run = store.load_benchmark_run(trade_date=manifest.trade_date, benchmark_run_id=manifest.benchmark_run_id)
    weight_frame = store.load_weight_rows(trade_date=manifest.trade_date, benchmark_run_id=manifest.benchmark_run_id)
    summary = store.load_benchmark_summary(trade_date=manifest.trade_date, benchmark_run_id=manifest.benchmark_run_id)
    return LoadedBenchmarkReference(
        manifest=manifest,
        run=run,
        weight_frame=weight_frame,
        summary=summary,
    )


def _find_portfolio_manifest_by_id(
    *,
    store: PortfolioAnalyticsArtifactStore,
    portfolio_analytics_run_id: str,
) -> PortfolioAnalyticsManifest:
    for manifest in store.list_portfolio_manifests():
        if manifest.portfolio_analytics_run_id == portfolio_analytics_run_id:
            return manifest
    raise FileNotFoundError(f"no portfolio analytics manifest found for {portfolio_analytics_run_id}")


def _find_benchmark_manifest_by_id(
    *,
    store: BenchmarkReferenceArtifactStore,
    benchmark_run_id: str,
) -> BenchmarkReferenceManifest:
    for manifest in store.list_benchmark_manifests():
        if manifest.benchmark_run_id == benchmark_run_id:
            return manifest
    raise FileNotFoundError(f"no benchmark reference manifest found for {benchmark_run_id}")


def _resolve_portfolio_source_for_manifest(
    *,
    project_root: Path,
    manifest: PortfolioAnalyticsManifest,
) -> LoadedPortfolioSource:
    paper_store = ExecutionArtifactStore(project_root)
    shadow_store = ShadowArtifactStore(project_root)
    paper_ids = {run.paper_run_id for run in paper_store.list_runs()}
    shadow_ids = {run.shadow_run_id for run in shadow_store.list_runs()}
    if manifest.source_type == PortfolioSourceType.PAPER_RUN:
        for candidate in manifest.source_run_ids:
            if candidate in paper_ids:
                return select_portfolio_source(project_root=project_root, paper_run_id=candidate)
    if manifest.source_type == PortfolioSourceType.SHADOW_RUN:
        for candidate in manifest.source_run_ids:
            if candidate in shadow_ids:
                return select_portfolio_source(project_root=project_root, shadow_run_id=candidate)
    for candidate in manifest.source_run_ids:
        if candidate in shadow_ids:
            return select_portfolio_source(project_root=project_root, shadow_run_id=candidate)
        if candidate in paper_ids:
            return select_portfolio_source(project_root=project_root, paper_run_id=candidate)
    raise FileNotFoundError(
        f"unable to resolve upstream execution source for portfolio analytics run {manifest.portfolio_analytics_run_id}"
    )
