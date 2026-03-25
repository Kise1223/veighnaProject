from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.load_master_data import load_bootstrap, validate_bootstrap
from tests.bootstrap_helpers import BOOTSTRAP_DIR


def test_validate_only_bootstrap_passes() -> None:
    payload = load_bootstrap(BOOTSTRAP_DIR)
    validate_bootstrap(payload, BOOTSTRAP_DIR)


def test_duplicate_instrument_key_is_rejected(tmp_path: Path) -> None:
    for source in BOOTSTRAP_DIR.iterdir():
        (tmp_path / source.name).write_bytes(source.read_bytes())
    with (tmp_path / "instrument_keys.csv").open("a", encoding="utf-8", newline="") as handle:
        handle.write(
            "EQ_SH_600000,600000,600000,600000,600000.SSE,SH600000,600000,SSE,bootstrap_manual,2026-03-26,2025-01-01,\n"
        )
    payload = load_bootstrap(tmp_path)
    with pytest.raises(ValueError, match="duplicate instrument_key mapping"):
        validate_bootstrap(payload, tmp_path)


def test_missing_manifest_entry_is_rejected(tmp_path: Path) -> None:
    for source in BOOTSTRAP_DIR.iterdir():
        (tmp_path / source.name).write_bytes(source.read_bytes())
    manifest_path = tmp_path / "bootstrap_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["entries"] = [
        entry for entry in payload["entries"] if entry["file"] != "cost_profiles.csv"
    ]
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    bootstrap = load_bootstrap(tmp_path)
    with pytest.raises(ValueError, match="missing manifest entries"):
        validate_bootstrap(bootstrap, tmp_path)
