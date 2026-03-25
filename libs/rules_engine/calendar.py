"""Trading calendar helpers backed by versioned bootstrap files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from libs.schemas.master_data import ExchangeCode


@dataclass(frozen=True)
class ExchangeCalendar:
    exchange: ExchangeCode
    closed_dates: frozenset[date]
    special_open_dates: frozenset[date]

    def is_trade_day(self, value: date) -> bool:
        if value in self.special_open_dates:
            return True
        if value.weekday() >= 5:
            return False
        return value not in self.closed_dates


def load_calendars(path: Path) -> dict[ExchangeCode, ExchangeCalendar]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    calendars: dict[ExchangeCode, ExchangeCalendar] = {}
    for item in payload["calendars"]:
        exchange = ExchangeCode(item["exchange"])
        calendars[exchange] = ExchangeCalendar(
            exchange=exchange,
            closed_dates=frozenset(date.fromisoformat(raw) for raw in item.get("closed_dates", [])),
            special_open_dates=frozenset(
                date.fromisoformat(raw) for raw in item.get("special_open_dates", [])
            ),
        )
    return calendars


def is_trade_day(
    value: date, exchange: ExchangeCode, calendars: dict[ExchangeCode, ExchangeCalendar]
) -> bool:
    """Return whether the date is tradable for the given exchange."""

    return calendars[exchange].is_trade_day(value)
