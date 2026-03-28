"""Configuration helpers for M13 benchmark reference construction."""

from __future__ import annotations

from libs.analytics.benchmark_schemas import BenchmarkReferenceConfig


def default_benchmark_reference_config() -> BenchmarkReferenceConfig:
    return BenchmarkReferenceConfig()
