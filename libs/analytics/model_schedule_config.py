"""Configuration helpers for M15 rolling model schedules."""

from __future__ import annotations

from libs.analytics.model_schedule_schemas import ModelScheduleConfig


def default_model_schedule_config() -> ModelScheduleConfig:
    return ModelScheduleConfig()
