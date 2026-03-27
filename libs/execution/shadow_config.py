"""Configuration loading for M8 replay-driven shadow sessions."""

from __future__ import annotations

from pathlib import Path

from libs.execution.config import load_yaml
from libs.execution.shadow_schemas import ShadowSessionConfig


def load_shadow_session_config(path: Path) -> ShadowSessionConfig:
    return ShadowSessionConfig.model_validate(load_yaml(path))
