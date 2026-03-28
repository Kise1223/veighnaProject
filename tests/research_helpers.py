from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def prepare_research_workspace(tmp_path: Path) -> Path:
    shutil.copytree(
        ROOT / "data" / "master" / "bootstrap",
        tmp_path / "data" / "master" / "bootstrap",
        dirs_exist_ok=True,
    )
    shutil.copytree(
        ROOT / "data" / "bootstrap" / "research_sample",
        tmp_path / "data" / "bootstrap" / "research_sample",
        dirs_exist_ok=True,
    )
    shutil.copytree(
        ROOT / "data" / "bootstrap" / "execution_sample",
        tmp_path / "data" / "bootstrap" / "execution_sample",
        dirs_exist_ok=True,
    )
    shadow_tick_sample = ROOT / "data" / "bootstrap" / "shadow_tick_sample"
    if shadow_tick_sample.exists():
        shutil.copytree(
            shadow_tick_sample,
            tmp_path / "data" / "bootstrap" / "shadow_tick_sample",
            dirs_exist_ok=True,
        )
    shutil.copytree(ROOT / "configs" / "qlib", tmp_path / "configs" / "qlib", dirs_exist_ok=True)
    shutil.copytree(
        ROOT / "configs" / "planning", tmp_path / "configs" / "planning", dirs_exist_ok=True
    )
    shutil.copytree(
        ROOT / "configs" / "execution", tmp_path / "configs" / "execution", dirs_exist_ok=True
    )
    return tmp_path
