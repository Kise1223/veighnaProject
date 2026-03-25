"""Health snapshot models and a tiny HTTP server."""

from __future__ import annotations

import json
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

from pydantic import BaseModel, ConfigDict, Field


class ModuleHealth(BaseModel):
    """One module or app load result."""

    name: str
    status: str
    detail: str | None = None
    required: bool = False

    model_config = ConfigDict(extra="forbid")


class GatewayHealth(BaseModel):
    """Gateway runtime state used by readiness checks."""

    name: str
    registered: bool
    adapters_bound: bool
    connected: bool
    last_error: str | None = None
    last_sync_at: str | None = None
    unfinished_orders: int = 0
    subscriptions: int = 0

    model_config = ConfigDict(extra="forbid")


class TradeServerHealth(BaseModel):
    """Aggregated health snapshot for the trade server."""

    env: str
    ready: bool
    engines: list[str] = Field(default_factory=list)
    gateway: GatewayHealth
    modules: list[ModuleHealth] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class HealthHttpServer:
    """Serve `/healthz` and `/readyz` from a background thread."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        snapshot_provider: Callable[[], TradeServerHealth],
    ) -> None:
        self._host = host
        self._port = port
        self._snapshot_provider = snapshot_provider
        self._server = ThreadingHTTPServer((host, port), self._build_handler())
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._started = False

    @property
    def address(self) -> tuple[str, int]:
        host = self._server.server_address[0]
        return str(host), self._server.server_address[1]

    def start(self) -> None:
        if self._started:
            return
        self._thread.start()
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)
        self._started = False

    def _build_handler(self) -> type[BaseHTTPRequestHandler]:
        provider = self._snapshot_provider

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                snapshot = provider()
                if self.path == "/healthz":
                    self._write_json(200, snapshot.model_dump())
                    return
                if self.path == "/readyz":
                    status = 200 if snapshot.ready else 503
                    self._write_json(status, {"ready": snapshot.ready})
                    return
                self._write_json(404, {"error": "not_found"})

            def log_message(self, message_format: str, *args: object) -> None:
                return

            def _write_json(self, status: int, payload: dict) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler
