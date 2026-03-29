"""Configuration helpers for M14 walk-forward campaigns."""

from __future__ import annotations

from libs.analytics.campaign_schemas import CampaignCompareConfig, CampaignConfig


def default_campaign_config() -> CampaignConfig:
    return CampaignConfig()


def default_campaign_compare_config() -> CampaignCompareConfig:
    return CampaignCompareConfig()
