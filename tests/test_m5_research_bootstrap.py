from __future__ import annotations

from pathlib import Path

import pytest

from apps.research_qlib.bootstrap import load_runtime_config, require_dependency

ROOT = Path(__file__).resolve().parents[1]


def test_load_runtime_config_smoke() -> None:
    config = load_runtime_config(ROOT / "configs" / "qlib" / "base.yaml")
    assert config.provider_uri == "data/qlib_bin"
    assert config.experiment_name == "baseline_linear_v1"


def test_missing_research_dependency_has_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_import(module_name: str):
        raise ImportError(module_name)

    monkeypatch.setattr("apps.research_qlib.bootstrap.importlib.import_module", fail_import)
    with pytest.raises(RuntimeError, match=r"\.\[research\]"):
        require_dependency("qlib")
