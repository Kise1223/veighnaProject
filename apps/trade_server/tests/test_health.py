from __future__ import annotations

import json
from pathlib import Path
from urllib.request import urlopen

from apps.trade_server.app.bootstrap import TradeServerBootstrap
from apps.trade_server.app.config import GatewayRuntimeConfig, HealthServerConfig, TradeServerConfig

ROOT = Path(__file__).resolve().parents[3]


def test_health_endpoints_return_json() -> None:
    config = TradeServerConfig(
        env="test",
        project_root=ROOT,
        gateway=GatewayRuntimeConfig(gateway_name="OPENCTPSEC"),
        health_server=HealthServerConfig(enabled=True, host="127.0.0.1", port=0),
    )
    runtime = TradeServerBootstrap(config).bootstrap()
    try:
        runtime.start_health_server()
        host, port = runtime.health_server.address  # type: ignore[union-attr]
        with urlopen(f"http://{host}:{port}/healthz") as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert payload["gateway"]["name"] == "OPENCTPSEC"

        with urlopen(f"http://{host}:{port}/readyz") as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert payload["ready"] is True
    finally:
        runtime.stop()
