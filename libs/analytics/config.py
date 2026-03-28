"""Configuration helpers for M11 execution analytics."""

from __future__ import annotations

from libs.analytics.schemas import ExecutionAnalyticsConfig


def default_execution_analytics_config() -> ExecutionAnalyticsConfig:
    return ExecutionAnalyticsConfig()
