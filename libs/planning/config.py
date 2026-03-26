"""Configuration loading for M6 planning workflows."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from libs.planning.schemas import RebalancePlannerConfigModel, TargetWeightConfigModel


def _require_yaml() -> Any:
    try:
        return importlib.import_module("yaml")
    except ImportError as exc:
        raise RuntimeError(
            "PyYAML is required for M6 planning configs. Install dependencies with "
            r'".\.venv\Scripts\python.exe -m pip install -e "".[research]"""'
        ) from exc


def load_yaml(path: Path) -> dict[str, Any]:
    yaml = _require_yaml()
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return dict(payload or {})


def load_target_weight_config(path: Path) -> TargetWeightConfigModel:
    return TargetWeightConfigModel.model_validate(load_yaml(path))


def load_rebalance_planner_config(path: Path) -> RebalancePlannerConfigModel:
    return RebalancePlannerConfigModel.model_validate(load_yaml(path))
