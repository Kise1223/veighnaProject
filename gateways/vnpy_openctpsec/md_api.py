"""Market data adapter contract and wrapper."""

from __future__ import annotations

from typing import Protocol

from gateways.vnpy_openctpsec.compat import SubscribeRequest
from gateways.vnpy_openctpsec.mapper import AdapterTickEvent


class MarketDataListener(Protocol):
    def on_md_tick(self, event: AdapterTickEvent) -> None: ...

    def on_md_log(self, message: str) -> None: ...


class OpenCtpMdAdapter(Protocol):
    def set_listener(self, listener: MarketDataListener) -> None: ...

    def connect(self, settings: dict[str, str]) -> None: ...

    def subscribe(self, request: SubscribeRequest) -> None: ...

    def close(self) -> None: ...


class MarketDataApi:
    def __init__(self, adapter: OpenCtpMdAdapter) -> None:
        self.adapter = adapter

    def set_listener(self, listener: MarketDataListener) -> None:
        self.adapter.set_listener(listener)

    def connect(self, settings: dict[str, str]) -> None:
        self.adapter.connect(settings)

    def subscribe(self, request: SubscribeRequest) -> None:
        self.adapter.subscribe(request)

    def close(self) -> None:
        self.adapter.close()
