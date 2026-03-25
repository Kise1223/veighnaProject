"""Trade server bootstrap around MainEngine and the OpenCTP gateway."""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from apps.trade_server.app.config import TradeServerConfig, default_optional_apps
from apps.trade_server.app.health import (
    GatewayHealth,
    HealthHttpServer,
    ModuleHealth,
    TradeServerHealth,
)
from libs.common.logging import configure_logging

LOGGER = configure_logging(logger_name="apps.trade_server.bootstrap")


def prepare_vntrader_environment(project_root: Path) -> Path:
    """Ensure vn.py uses a writable `.vntrader` under the project root."""

    project_root = project_root.resolve()
    trader_dir = project_root / ".vntrader"
    trader_dir.mkdir(exist_ok=True)
    os.chdir(project_root)
    return trader_dir


@dataclass
class TradeServerRuntime:
    """Live runtime objects for the M3 bootstrap."""

    config: TradeServerConfig
    event_engine: Any
    main_engine: Any
    gateway: Any
    modules: list[ModuleHealth]
    health_server: HealthHttpServer | None = None

    def start_health_server(self) -> None:
        if not self.health_server:
            return
        self.health_server.start()

    def stop(self) -> None:
        if self.health_server:
            self.health_server.stop()
        self.main_engine.close()

    def snapshot_health(self) -> TradeServerHealth:
        gateway_state = self.gateway.get_status_snapshot()
        ready = gateway_state["registered"] and not any(
            module.required and module.status not in {"loaded", "missing_optional"}
            for module in self.modules
        )
        return TradeServerHealth(
            env=self.config.env,
            ready=ready,
            engines=sorted(self.main_engine.engines.keys()),
            gateway=GatewayHealth(**gateway_state),
            modules=self.modules,
        )


class TradeServerBootstrap:
    """Create the M3 trade server runtime."""

    def __init__(self, config: TradeServerConfig) -> None:
        self.config = config

    def bootstrap(self) -> TradeServerRuntime:
        prepare_vntrader_environment(self.config.project_root)
        from gateways.vnpy_openctpsec.compat import EventEngine, MainEngine
        from gateways.vnpy_openctpsec.gateway import OpenCTPSecGateway

        event_engine = EventEngine()
        main_engine = MainEngine(event_engine)
        gateway = main_engine.add_gateway(
            OpenCTPSecGateway, gateway_name=self.config.gateway.gateway_name
        )
        gateway = self._cast_gateway(gateway, OpenCTPSecGateway)
        modules = self._load_optional_apps(main_engine)

        health_server = None
        if self.config.health_server.enabled:
            health_server = HealthHttpServer(
                host=self.config.health_server.host,
                port=self.config.health_server.port,
                snapshot_provider=lambda: runtime.snapshot_health(),
            )

        runtime = TradeServerRuntime(
            config=self.config,
            event_engine=event_engine,
            main_engine=main_engine,
            gateway=gateway,
            modules=modules,
            health_server=health_server,
        )
        if self.config.gateway.auto_connect:
            gateway.connect(self.config.gateway.settings)
        return runtime

    def _load_optional_apps(self, main_engine: Any) -> list[ModuleHealth]:
        app_configs = self.config.optional_apps or default_optional_apps()
        modules: list[ModuleHealth] = []
        for app in app_configs:
            if not app.enabled:
                modules.append(ModuleHealth(name=app.name, status="disabled"))
                continue
            try:
                module = importlib.import_module(app.module)
            except ImportError:
                status = "missing_optional" if not app.required else "missing_required"
                modules.append(
                    ModuleHealth(
                        name=app.name,
                        status=status,
                        detail=f"{app.module}.{app.class_name} is not installed",
                        required=app.required,
                    )
                )
                continue

            try:
                app_class = getattr(module, app.class_name)
                main_engine.add_app(app_class)
            except Exception as exc:  # pragma: no cover - depends on optional external packages
                modules.append(
                    ModuleHealth(
                        name=app.name,
                        status="error",
                        detail=str(exc),
                        required=app.required,
                    )
                )
                continue

            modules.append(ModuleHealth(name=app.name, status="loaded", required=app.required))
        return modules

    def _cast_gateway(self, gateway: Any, gateway_class: type[Any]) -> Any:
        if not isinstance(gateway, gateway_class):
            raise TypeError("expected OpenCTPSecGateway instance from MainEngine.add_gateway")
        return gateway


def build_runtime(config_path: Path) -> TradeServerRuntime:
    """Load config from disk and bootstrap the runtime."""

    config = TradeServerConfig.from_json_file(config_path)
    bootstrap = TradeServerBootstrap(config)
    return bootstrap.bootstrap()
