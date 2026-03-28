"""Lineage helpers for M13 benchmark reference artifacts."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from libs.analytics.benchmark_artifacts import BenchmarkReferenceArtifactStore
from libs.analytics.benchmark_schemas import BenchmarkReferenceLineage


def resolve_benchmark_reference_lineage(
    *,
    project_root: Path,
    trade_date: date,
    benchmark_run_id: str,
) -> BenchmarkReferenceLineage:
    store = BenchmarkReferenceArtifactStore(project_root)
    manifest = store.load_benchmark_manifest(trade_date=trade_date, benchmark_run_id=benchmark_run_id)
    return BenchmarkReferenceLineage(
        benchmark_run_id=manifest.benchmark_run_id,
        source_portfolio_analytics_run_id=manifest.source_portfolio_analytics_run_id,
        source_strategy_run_id=manifest.source_strategy_run_id,
        source_prediction_run_id=manifest.source_prediction_run_id,
        source_qlib_export_run_id=manifest.source_qlib_export_run_id,
        source_standard_build_run_id=manifest.source_standard_build_run_id,
        run_file_path=manifest.run_file_path,
        run_file_hash=manifest.run_file_hash,
        summary_file_path=manifest.summary_file_path,
        summary_file_hash=manifest.summary_file_hash,
        status=manifest.status,
    )
