from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from apps.trade_server.app.paper.runner import run_paper_execution
from libs.marketdata.manifest_store import ManifestStore
from tests.execution_helpers import prepare_m7_workspace


def test_market_data_hash_uses_content_when_manifest_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("qlib")
    monkeypatch.setenv("MPLCONFIGDIR", str(tmp_path / ".mpl"))
    workspace, execution_task_id = prepare_m7_workspace(tmp_path)
    manifest_store = ManifestStore(workspace / "data" / "manifests")
    manifest_store.delete_standard_file_manifests(layer="bars_1m", trade_date=date(2026, 3, 26))

    default_market_path = (
        workspace
        / "data"
        / "bootstrap"
        / "execution_sample"
        / "market_snapshot_2026-03-26.json"
    )
    custom_market_path = workspace / "custom_market_snapshot.json"
    custom_market_path.write_text(default_market_path.read_text(encoding="utf-8"), encoding="utf-8")

    first = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_snapshot_path=custom_market_path,
    )

    changed_payload = json.loads(custom_market_path.read_text(encoding="utf-8"))
    for item in changed_payload["market_snapshots"]:
        item["previous_close"] = "88.88"
        item["last_price"] = "88.88"
    custom_market_path.write_text(
        json.dumps(changed_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    second = run_paper_execution(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        market_snapshot_path=custom_market_path,
    )

    assert first.reused is False
    assert second.reused is False
    assert first.paper_run_id != second.paper_run_id
