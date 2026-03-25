"""Trading-side schemas shared by rules and gateway state handling."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from libs.schemas.master_data import ExchangeCode, InstrumentType


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class TradingPhase(StrEnum):
    CLOSED = "CLOSED"
    OPEN_CALL = "OPEN_CALL"
    CONTINUOUS_AM = "CONTINUOUS_AM"
    CONTINUOUS_PM = "CONTINUOUS_PM"
    CLOSE_CALL = "CLOSE_CALL"
    AFTER_HOURS_FIXED = "AFTER_HOURS_FIXED"


class OrderStatus(StrEnum):
    SUBMITTING = "SUBMITTING"
    NOTTRADED = "NOTTRADED"
    PARTTRADED = "PARTTRADED"
    ALLTRADED = "ALLTRADED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class PositionSnapshot(BaseModel):
    instrument_key: str
    total_quantity: int = Field(ge=0)
    sellable_quantity: int = Field(ge=0)

    model_config = ConfigDict(extra="forbid")


class AccountSnapshot(BaseModel):
    account_id: str
    available_cash: Decimal
    frozen_cash: Decimal = Decimal("0")
    nav: Decimal | None = None

    model_config = ConfigDict(extra="forbid")


class MarketSnapshot(BaseModel):
    instrument_key: str
    last_price: Decimal
    upper_limit: Decimal | None = None
    lower_limit: Decimal | None = None
    is_paused: bool = False
    exchange_ts: datetime
    received_ts: datetime

    model_config = ConfigDict(extra="forbid")


class OrderRequest(BaseModel):
    account_id: str
    instrument_key: str
    exchange: ExchangeCode
    symbol: str
    side: OrderSide
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=Decimal("0"))
    reference: str
    strategy_run_id: str
    order_ts: datetime
    exchange_ts: datetime | None = None
    received_ts: datetime | None = None

    model_config = ConfigDict(extra="forbid")


class ValidationResult(BaseModel):
    accepted: bool
    phase: TradingPhase
    reasons: list[str] = Field(default_factory=list)
    next_actionable_time: datetime | None = None

    model_config = ConfigDict(extra="forbid")


class CostBreakdown(BaseModel):
    instrument_key: str
    instrument_type: InstrumentType
    exchange: ExchangeCode
    side: OrderSide
    quantity: int = Field(gt=0)
    notional: Decimal
    commission: Decimal
    stamp_duty: Decimal
    handling_fee: Decimal
    transfer_fee: Decimal
    reg_fee: Decimal
    total: Decimal
    effective_date: date

    model_config = ConfigDict(extra="forbid")
