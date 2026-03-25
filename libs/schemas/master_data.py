"""Canonical master data contracts for M1."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class ExchangeCode(StrEnum):
    SSE = "SSE"
    SZSE = "SZSE"


class InstrumentType(StrEnum):
    EQUITY = "EQUITY"
    ETF = "ETF"


class Board(StrEnum):
    MAIN = "MAIN"
    GEM = "GEM"
    STAR = "STAR"
    ETF = "ETF"


class SettlementType(StrEnum):
    T0 = "T0"
    T1 = "T1"


class Provenance(BaseModel):
    """Source tracing metadata for bootstrap snapshots."""

    source: str
    source_version: str
    source_url: str | None = None
    source_hash: str | None = None
    reviewed_at: datetime | None = None

    model_config = ConfigDict(extra="forbid")


class InstrumentKeyMapping(BaseModel):
    """Stable symbol mapping across internal and vendor systems."""

    instrument_key: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    vendor_symbol: str = Field(min_length=1)
    broker_symbol: str = Field(min_length=1)
    vt_symbol: str = Field(min_length=1)
    qlib_symbol: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: ExchangeCode
    source: str
    source_version: str
    effective_from: date
    effective_to: date | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("vt_symbol")
    @classmethod
    def validate_vt_symbol(cls, value: str) -> str:
        if "." not in value:
            raise ValueError("vt_symbol must be formatted as symbol.exchange")
        return value

    @model_validator(mode="after")
    def validate_effective_window(self) -> Self:
        if self.effective_to and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not be earlier than effective_from")
        return self


class Instrument(BaseModel):
    """Instrument attributes used by rules, routing, and storage."""

    instrument_key: str
    exchange: ExchangeCode
    symbol: str = Field(min_length=1)
    instrument_type: InstrumentType
    board: Board
    list_date: date
    delist_date: date | None = None
    settlement_type: SettlementType
    pricetick: Decimal = Field(gt=Decimal("0"))
    min_buy_lot: int = Field(gt=0)
    odd_lot_sell_only: bool
    limit_pct: Decimal | None = Field(default=None, ge=Decimal("0"))
    ipo_free_limit_days: int = Field(default=0, ge=0)
    after_hours_fixed_price_supported: bool
    source: str
    source_version: str
    effective_from: date
    effective_to: date | None = None

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_effective_window(self) -> Self:
        if self.effective_to and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not be earlier than effective_from")
        return self


class TradingWindow(BaseModel):
    """One time window inside a trade day."""

    phase: str
    start: time
    end: time
    order_accepting: bool
    match_phase: bool
    cancel_allowed: bool

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.end <= self.start:
            raise ValueError("window end must be later than start")
        return self


class RestrictedWindow(BaseModel):
    """An explicit window where a given action is not allowed."""

    start: time
    end: time
    action: str
    reason: str

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.end <= self.start:
            raise ValueError("restricted window end must be later than start")
        return self


class MarketRuleSnapshot(BaseModel):
    """Effective-date rule snapshot for exchange, board, and product."""

    rule_id: str
    exchange: ExchangeCode
    instrument_type: InstrumentType
    board: Board
    effective_from: date
    effective_to: date | None = None
    trading_sessions: list[TradingWindow]
    cancel_restricted_windows: list[RestrictedWindow]
    price_limit_ratio: Decimal | None = Field(default=None, ge=Decimal("0"))
    ipo_free_limit_days: int = Field(default=0, ge=0)
    after_hours_supported: bool = False
    source: str
    source_version: str

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_effective_window(self) -> Self:
        if self.effective_to and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not be earlier than effective_from")
        return self


class CostProfile(BaseModel):
    """Product-specific cost template used by backtest and live execution."""

    cost_profile_id: str
    broker: str
    instrument_type: InstrumentType
    exchange: ExchangeCode | None = None
    effective_from: date
    effective_to: date | None = None
    commission_rate: Decimal = Field(ge=Decimal("0"))
    commission_min: Decimal = Field(ge=Decimal("0"))
    tax_sell_rate: Decimal = Field(ge=Decimal("0"))
    handling_fee_rate: Decimal = Field(ge=Decimal("0"))
    transfer_fee_rate: Decimal = Field(ge=Decimal("0"))
    reg_fee_rate: Decimal = Field(ge=Decimal("0"))
    source: str
    source_version: str

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_effective_window(self) -> Self:
        if self.effective_to and self.effective_to < self.effective_from:
            raise ValueError("effective_to must not be earlier than effective_from")
        return self


class BootstrapManifestEntry(BaseModel):
    """One bootstrap file tracked by source provenance."""

    file: str
    source_url: str
    source_hash: str
    reviewed_at: datetime

    model_config = ConfigDict(extra="forbid")


class BootstrapManifest(BaseModel):
    """Repository-level source manifest for M1 seed files."""

    source: str
    source_version: str
    entries: list[BootstrapManifestEntry]

    model_config = ConfigDict(extra="forbid")
