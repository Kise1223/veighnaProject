"""Research runtime bootstrap and optional dependency guards."""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from libs.research.schemas import BaselineDatasetConfig, BaselineModelConfig, ResearchRuntimeConfig


def require_dependency(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except ImportError as exc:  # pragma: no cover - exercised through monkeypatch tests
        raise RuntimeError(
            f"{module_name} is required for M5 research workflows. Install dependencies with "
            r'".\.venv\Scripts\python.exe -m pip install -e "".[research]"""'
        ) from exc


def load_yaml(path: Path) -> dict[str, Any]:
    yaml = require_dependency("yaml")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    return dict(payload or {})


def load_runtime_config(path: Path) -> ResearchRuntimeConfig:
    return ResearchRuntimeConfig.model_validate(load_yaml(path))


def load_dataset_config(path: Path) -> BaselineDatasetConfig:
    return BaselineDatasetConfig.model_validate(load_yaml(path))


def load_model_config(path: Path) -> BaselineModelConfig:
    return BaselineModelConfig.model_validate(load_yaml(path))


def init_qlib(project_root: Path, runtime_config: ResearchRuntimeConfig) -> Any:
    qlib = require_dependency("qlib")
    from qlib.config import C  # type: ignore[import-untyped]

    provider_uri = str((project_root / runtime_config.provider_uri).resolve())
    qlib.init(provider_uri=provider_uri, region=runtime_config.region, expression_cache=None, dataset_cache=None)
    C["kernels"] = 1
    C["joblib_backend"] = "threading"
    return qlib


def recorder_uri(project_root: Path, runtime_config: ResearchRuntimeConfig) -> str:
    return (project_root / runtime_config.recorder_uri).resolve().as_uri()
