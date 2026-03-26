"""Configuration loading for M7 paper execution."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from libs.execution.schemas import PaperFillModelConfig


def load_yaml(path: Path) -> dict[str, Any]:
    try:
        yaml = importlib.import_module("yaml")
    except ImportError as exc:  # pragma: no cover - optional dependency handled in runtime
        raise RuntimeError(
            "PyYAML is required to load paper execution configs; install with "
            '".\\.venv\\Scripts\\python.exe -m pip install -e \\".[research]\\""'
        ) from exc
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected mapping config in {path}")
    return payload


def load_fill_model_config(path: Path) -> PaperFillModelConfig:
    return PaperFillModelConfig.model_validate(load_yaml(path))
