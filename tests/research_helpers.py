from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def prepare_research_workspace(tmp_path: Path) -> Path:
    shutil.copytree(ROOT / "data" / "master" / "bootstrap", tmp_path / "data" / "master" / "bootstrap")
    shutil.copytree(
        ROOT / "data" / "bootstrap" / "research_sample",
        tmp_path / "data" / "bootstrap" / "research_sample",
    )
    shutil.copytree(
        ROOT / "data" / "bootstrap" / "execution_sample",
        tmp_path / "data" / "bootstrap" / "execution_sample",
    )
    shutil.copytree(ROOT / "configs" / "qlib", tmp_path / "configs" / "qlib")
    shutil.copytree(ROOT / "configs" / "planning", tmp_path / "configs" / "planning")
    shutil.copytree(ROOT / "configs" / "execution", tmp_path / "configs" / "execution")
    return tmp_path
