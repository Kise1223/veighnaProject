from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_bootstrap_files_exist() -> None:
    expected = [
        ROOT / "pyproject.toml",
        ROOT / ".env.example",
        ROOT / "compose.yaml",
        ROOT / "scripts" / "dev.ps1",
        ROOT / "docs" / "adr" / "0001_m0_m2_contracts.md",
        ROOT / "docs" / "adr" / "0003_trade_server_runtime.md",
        ROOT / "docs" / "adr" / "0004_m4_data_foundation.md",
        ROOT / "docs" / "runbooks" / "recorder_and_qlib_export.md",
        ROOT / "docs" / "plans" / "m4_data_foundation_plan.md",
        ROOT / "docs" / "backlog" / "m4_data_foundation_backlog.yaml",
        ROOT / "infra" / "sql" / "postgres" / "002_market_data.sql",
        ROOT / "apps" / "trade_server" / "config.example.json",
        ROOT / "scripts" / "run_trade_server.py",
        ROOT / "scripts" / "run_recorder_smoke.py",
        ROOT / "scripts" / "build_standard_data.py",
        ROOT / "scripts" / "export_qlib_provider.py",
        ROOT / "scripts" / "run_data_dq.py",
        ROOT / "scripts" / "replay_sample.py",
    ]
    for path in expected:
        assert path.exists(), f"missing required bootstrap file: {path}"


def test_dev_script_supports_required_commands() -> None:
    script = (ROOT / "scripts" / "dev.ps1").read_text(encoding="utf-8")
    for command in ("bootstrap", "lint", "test", "up"):
        assert command in script
    assert "mypy apps gateways libs scripts" in script


def test_common_logging_smoke() -> None:
    from libs.common.logging import configure_logging

    logger = configure_logging(logger_name="tests.bootstrap")
    assert logger.name == "tests.bootstrap"
