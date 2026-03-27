"""Trading-session clock helpers for M8 shadow execution."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from libs.common.time import CN_TZ, ensure_cn_aware
from libs.rules_engine.market_rules import RulesRepository
from libs.schemas.master_data import Instrument


def resolve_activation_dt(
    *, created_at: datetime, instrument: Instrument, rules_repo: RulesRepository
) -> datetime | None:
    aware_created_at = ensure_cn_aware(created_at)
    if rules_repo.is_order_accepting(aware_created_at, instrument):
        return aware_created_at
    return rules_repo.next_actionable_time(aware_created_at, instrument)


def resolve_session_end_dt(
    *, trade_date: date, instrument: Instrument, rules_repo: RulesRepository
) -> datetime:
    sessions = rules_repo.get_sessions(trade_date, instrument)
    if not sessions:
        raise ValueError(
            f"no trading session found for {instrument.instrument_key} on {trade_date.isoformat()}"
        )
    last_session = max(sessions, key=lambda item: item.end)
    return datetime.combine(trade_date, last_session.end, tzinfo=CN_TZ)


def collect_replay_datetimes(frames_by_instrument: dict[str, Any]) -> list[datetime]:
    timestamps: set[datetime] = set()
    for frame in frames_by_instrument.values():
        if frame is None or frame.empty:
            continue
        timestamps.update(ensure_cn_aware(value) for value in frame["bar_dt"].tolist())
    return sorted(timestamps)
