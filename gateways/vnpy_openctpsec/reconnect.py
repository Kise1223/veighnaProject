"""Reconnect bookkeeping for subscriptions and unfinished orders."""

from __future__ import annotations

from dataclasses import dataclass, field

from gateways.vnpy_openctpsec.compat import SubscribeRequest


@dataclass
class ReconnectState:
    subscriptions: dict[str, SubscribeRequest] = field(default_factory=dict)
    unfinished_local_orderids: set[str] = field(default_factory=set)

    def remember_subscription(self, request: SubscribeRequest) -> None:
        self.subscriptions[f"{request.symbol}.{request.exchange}"] = request

    def remember_unfinished_order(self, local_orderid: str) -> None:
        self.unfinished_local_orderids.add(local_orderid)

    def forget_unfinished_order(self, local_orderid: str) -> None:
        self.unfinished_local_orderids.discard(local_orderid)

    def resubscribe_requests(self) -> list[SubscribeRequest]:
        return list(self.subscriptions.values())
