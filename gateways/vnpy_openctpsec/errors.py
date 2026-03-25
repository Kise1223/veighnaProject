"""Gateway-specific errors."""

from __future__ import annotations


class GatewayConfigurationError(RuntimeError):
    """Raised when the gateway configuration is incomplete."""


class StateTransitionError(RuntimeError):
    """Raised when an invalid state transition is attempted."""


class DuplicateEventIgnoredError(RuntimeError):
    """Raised internally when a duplicate callback is ignored."""
