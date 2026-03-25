"""Configuration models for the trade server bootstrap."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OptionalAppConfig(BaseModel):
    """One optional VeighNa app integration."""

    name: str
    module: str
    class_name: str
    enabled: bool = True
    required: bool = False

    model_config = ConfigDict(extra="forbid")


class HealthServerConfig(BaseModel):
    """HTTP health endpoint settings."""

    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = Field(default=18080, ge=0, le=65535)

    model_config = ConfigDict(extra="forbid")


class GatewayRuntimeConfig(BaseModel):
    """Gateway bootstrap settings."""

    gateway_name: str = "OPENCTPSEC"
    auto_connect: bool = False
    settings: dict[str, str] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid")


class TradeServerConfig(BaseModel):
    """Top-level trade server configuration."""

    env: str = "local"
    project_root: Path
    gateway: GatewayRuntimeConfig = Field(default_factory=GatewayRuntimeConfig)
    health_server: HealthServerConfig = Field(default_factory=HealthServerConfig)
    optional_apps: list[OptionalAppConfig] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def normalize_project_root(self) -> Self:
        self.project_root = self.project_root.resolve()
        return self

    @classmethod
    def from_json_file(cls, path: Path) -> Self:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        project_root = Path(payload.get("project_root", "."))
        if not project_root.is_absolute():
            payload["project_root"] = (path.resolve().parent / project_root).resolve()
        return cls.model_validate(payload)


def default_optional_apps() -> list[OptionalAppConfig]:
    """Return the M3 optional app registry."""

    return [
        OptionalAppConfig(
            name="risk_manager",
            module="vnpy_riskmanager",
            class_name="RiskManagerApp",
        ),
        OptionalAppConfig(
            name="algo_trading",
            module="vnpy_algotrading",
            class_name="AlgoTradingApp",
        ),
        OptionalAppConfig(
            name="data_recorder",
            module="vnpy_datarecorder",
            class_name="DataRecorderApp",
        ),
        OptionalAppConfig(
            name="rpc_service",
            module="vnpy_rpcservice",
            class_name="RpcServiceApp",
        ),
        OptionalAppConfig(
            name="portfolio_manager",
            module="vnpy_portfoliomanager",
            class_name="PortfolioManagerApp",
        ),
    ]
