"""Time utilities with explicit Asia/Shanghai handling."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

CN_TZ = ZoneInfo("Asia/Shanghai")


def ensure_cn_aware(value: datetime) -> datetime:
    """Return an Asia/Shanghai-aware datetime."""

    if value.tzinfo is None:
        return value.replace(tzinfo=CN_TZ)
    return value.astimezone(CN_TZ)
