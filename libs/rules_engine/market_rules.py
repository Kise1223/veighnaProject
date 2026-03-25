"""Market rule lookup and trading phase helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

from libs.common.time import CN_TZ, ensure_cn_aware
from libs.rules_engine.calendar import ExchangeCalendar, is_trade_day
from libs.schemas.master_data import ExchangeCode, Instrument, MarketRuleSnapshot, TradingWindow
from libs.schemas.trading import TradingPhase

STATUS_FLAGS_NO_LIMIT = {"NO_LIMIT", "IPO_FREE_LIMIT"}


@dataclass(frozen=True)
class PriceLimit:
    upper_limit: Decimal | None
    lower_limit: Decimal | None


def _load_market_rules(path: Path) -> list[MarketRuleSnapshot]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [MarketRuleSnapshot.model_validate(item) for item in payload["market_rules"]]


class RulesRepository:
    """Loads bootstrap rule snapshots and serves effective-date queries."""

    def __init__(
        self,
        rules: list[MarketRuleSnapshot],
        calendars: dict[ExchangeCode, ExchangeCalendar],
    ) -> None:
        self._rules = rules
        self._calendars: dict[ExchangeCode, ExchangeCalendar] = calendars
        self._validate_non_overlap()

    @classmethod
    def from_bootstrap_dir(
        cls,
        bootstrap_dir: Path,
        calendars: dict[ExchangeCode, ExchangeCalendar],
    ) -> RulesRepository:
        rules = _load_market_rules(bootstrap_dir / "market_rules.json")
        return cls(rules=rules, calendars=calendars)

    def _validate_non_overlap(self) -> None:
        grouped: dict[tuple[str, str, str], list[MarketRuleSnapshot]] = {}
        for rule in self._rules:
            key = (rule.exchange.value, rule.instrument_type.value, rule.board.value)
            grouped.setdefault(key, []).append(rule)
        for snapshots in grouped.values():
            ordered = sorted(snapshots, key=lambda item: item.effective_from)
            for current, next_item in zip(ordered, ordered[1:], strict=False):
                current_end = current.effective_to or date.max
                if current_end >= next_item.effective_from:
                    raise ValueError(
                        f"overlapping market rule snapshots: {current.rule_id} and {next_item.rule_id}"
                    )

    def get_rule(self, trade_date: date, instrument: Instrument) -> MarketRuleSnapshot:
        candidates = [
            rule
            for rule in self._rules
            if rule.exchange == instrument.exchange
            and rule.instrument_type == instrument.instrument_type
            and rule.board == instrument.board
            and rule.effective_from <= trade_date
            and (rule.effective_to is None or rule.effective_to >= trade_date)
        ]
        if not candidates:
            raise KeyError(
                f"no rule snapshot for {instrument.instrument_key} on {trade_date.isoformat()}"
            )
        return max(candidates, key=lambda item: item.effective_from)

    def get_sessions(self, trade_date: date, instrument: Instrument) -> list[TradingWindow]:
        if not is_trade_day(trade_date, instrument.exchange, self._calendars):
            return []
        return self.get_rule(trade_date, instrument).trading_sessions

    def get_trading_phase(self, value: datetime, instrument: Instrument) -> TradingPhase:
        aware_value = ensure_cn_aware(value)
        sessions = self.get_sessions(aware_value.date(), instrument)
        current_time = aware_value.timetz().replace(tzinfo=None)
        for session in sessions:
            if session.start <= current_time < session.end:
                return TradingPhase(session.phase)
        return TradingPhase.CLOSED

    def is_order_accepting(self, value: datetime, instrument: Instrument) -> bool:
        aware_value = ensure_cn_aware(value)
        current_time = aware_value.timetz().replace(tzinfo=None)
        for session in self.get_sessions(aware_value.date(), instrument):
            if session.start <= current_time < session.end:
                return session.order_accepting
        return False

    def is_match_phase(self, value: datetime, instrument: Instrument) -> bool:
        aware_value = ensure_cn_aware(value)
        current_time = aware_value.timetz().replace(tzinfo=None)
        for session in self.get_sessions(aware_value.date(), instrument):
            if session.start <= current_time < session.end:
                return session.match_phase
        return False

    def next_actionable_time(self, value: datetime, instrument: Instrument) -> datetime | None:
        aware_value = ensure_cn_aware(value)
        current_time = aware_value.timetz().replace(tzinfo=None)
        sessions = self.get_sessions(aware_value.date(), instrument)
        for session in sessions:
            if current_time < session.start and session.order_accepting:
                return datetime.combine(aware_value.date(), session.start, tzinfo=CN_TZ)
            if session.start <= current_time < session.end and session.order_accepting:
                return aware_value
        for offset in range(1, 8):
            next_day = aware_value.date() + timedelta(days=offset)
            if not is_trade_day(next_day, instrument.exchange, self._calendars):
                continue
            for session in self.get_sessions(next_day, instrument):
                if session.order_accepting:
                    return datetime.combine(next_day, session.start, tzinfo=CN_TZ)
        return None

    def is_cancel_allowed(self, value: datetime, instrument: Instrument) -> bool:
        aware_value = ensure_cn_aware(value)
        rule = self.get_rule(aware_value.date(), instrument)
        current_time = aware_value.timetz().replace(tzinfo=None)
        for window in rule.cancel_restricted_windows:
            if window.action == "CANCEL" and window.start <= current_time < window.end:
                return False
        for session in self.get_sessions(aware_value.date(), instrument):
            if session.start <= current_time < session.end:
                return session.cancel_allowed
        return False

    def supports_after_hours_fixed_price(self, trade_date: date, instrument: Instrument) -> bool:
        return (
            self.get_rule(trade_date, instrument).after_hours_supported
            and instrument.after_hours_fixed_price_supported
        )

    def is_t0_allowed(self, instrument: Instrument) -> bool:
        return instrument.settlement_type.value == "T0"

    def get_lot_size(self, instrument: Instrument, side: str) -> int:
        if side == "BUY":
            return instrument.min_buy_lot
        if instrument.odd_lot_sell_only:
            return 1
        return instrument.min_buy_lot

    def get_price_limit(
        self,
        trade_date: date,
        instrument: Instrument,
        last_close: Decimal,
        open_price: Decimal | None,
        status_flags: set[str] | None = None,
    ) -> PriceLimit:
        status_flags = status_flags or set()
        rule = self.get_rule(trade_date, instrument)
        if status_flags.intersection(STATUS_FLAGS_NO_LIMIT):
            return PriceLimit(upper_limit=None, lower_limit=None)
        free_limit_deadline = instrument.list_date + timedelta(days=rule.ipo_free_limit_days)
        if instrument.list_date <= trade_date < free_limit_deadline:
            return PriceLimit(upper_limit=None, lower_limit=None)
        limit_ratio = (
            instrument.limit_pct if instrument.limit_pct is not None else rule.price_limit_ratio
        )
        if limit_ratio is None:
            return PriceLimit(upper_limit=None, lower_limit=None)
        upper = _round_to_tick(last_close * (Decimal("1") + limit_ratio), instrument.pricetick)
        lower = _round_to_tick(last_close * (Decimal("1") - limit_ratio), instrument.pricetick)
        return PriceLimit(upper_limit=upper, lower_limit=lower)


def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    if tick == 0:
        return value
    steps = (value / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return (steps * tick).quantize(tick)


def get_sessions(
    repo: RulesRepository,
    trade_date: date,
    instrument: Instrument,
) -> list[TradingWindow]:
    return repo.get_sessions(trade_date, instrument)


def get_trading_phase(
    repo: RulesRepository, value: datetime, instrument: Instrument
) -> TradingPhase:
    return repo.get_trading_phase(value, instrument)


def is_order_accepting(repo: RulesRepository, value: datetime, instrument: Instrument) -> bool:
    return repo.is_order_accepting(value, instrument)


def is_match_phase(repo: RulesRepository, value: datetime, instrument: Instrument) -> bool:
    return repo.is_match_phase(value, instrument)


def next_actionable_time(
    repo: RulesRepository, value: datetime, instrument: Instrument
) -> datetime | None:
    return repo.next_actionable_time(value, instrument)


def is_cancel_allowed(repo: RulesRepository, value: datetime, instrument: Instrument) -> bool:
    return repo.is_cancel_allowed(value, instrument)


def supports_after_hours_fixed_price(
    repo: RulesRepository, trade_date: date, instrument: Instrument
) -> bool:
    return repo.supports_after_hours_fixed_price(trade_date, instrument)


def is_t0_allowed(repo: RulesRepository, instrument: Instrument) -> bool:
    return repo.is_t0_allowed(instrument)


def get_lot_size(repo: RulesRepository, instrument: Instrument, side: str) -> int:
    return repo.get_lot_size(instrument, side)


def get_price_limit(
    repo: RulesRepository,
    trade_date: date,
    instrument: Instrument,
    last_close: Decimal,
    open_price: Decimal | None,
    status_flags: set[str] | None = None,
) -> PriceLimit:
    return repo.get_price_limit(trade_date, instrument, last_close, open_price, status_flags)
