"""Configuration helpers for M13 benchmark / attribution analytics."""

from __future__ import annotations

from libs.analytics.attribution_schemas import BenchmarkAnalyticsConfig


def default_benchmark_analytics_config() -> BenchmarkAnalyticsConfig:
    return BenchmarkAnalyticsConfig()
