"""Configuration helpers for M12 portfolio analytics."""

from __future__ import annotations

from libs.analytics.portfolio_schemas import PortfolioAnalyticsConfig


def default_portfolio_analytics_config() -> PortfolioAnalyticsConfig:
    return PortfolioAnalyticsConfig()
