"""Minimal project logging setup used by M0-M2 components."""

from __future__ import annotations

import logging

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def configure_logging(level: int = logging.INFO, logger_name: str | None = None) -> logging.Logger:
    """Configure and return a project logger.

    The helper is intentionally small and avoids global reconfiguration when handlers
    already exist, so tests and adapters can call it safely.
    """

    logger = logging.getLogger(logger_name)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=level, format=DEFAULT_LOG_FORMAT)
    logger.setLevel(level)
    return logger
