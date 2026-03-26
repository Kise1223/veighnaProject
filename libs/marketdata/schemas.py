"""Market data schemas for recording, standardization, and export."""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class SessionTag(StrEnum):
    AUCTION = "auction"
    AM = "am"
    PM = "pm"
    AFTER_HOURS = "after_hours"


class RawTickRecord(BaseModel):
    instrument_key: str
    symbol: str
    exchange: str
    vt_symbol: str
    gateway_name: str
    exchange_ts: datetime
    received_ts: datetime
    last_price: float
    volume: float = 0.0
    turnover: float = 0.0
    open_interest: float | None = None
    bid_price_1: float | None = None
    bid_price_2: float | None = None
    bid_price_3: float | None = None
    bid_price_4: float | None = None
    bid_price_5: float | None = None
    ask_price_1: float | None = None
    ask_price_2: float | None = None
    ask_price_3: float | None = None
    ask_price_4: float | None = None
    ask_price_5: float | None = None
    bid_volume_1: float | None = None
    bid_volume_2: float | None = None
    bid_volume_3: float | None = None
    bid_volume_4: float | None = None
    bid_volume_5: float | None = None
    ask_volume_1: float | None = None
    ask_volume_2: float | None = None
    ask_volume_3: float | None = None
    ask_volume_4: float | None = None
    ask_volume_5: float | None = None
    limit_up: float | None = None
    limit_down: float | None = None
    source_seq: str | None = None
    ingest_seq: int | None = None
    raw_hash: str
    recorded_at: datetime

    model_config = ConfigDict(extra="forbid")


class StandardizedTickRecord(BaseModel):
    instrument_key: str
    symbol: str
    exchange: str
    vt_symbol: str
    exchange_ts: datetime
    received_ts: datetime
    last_price: float
    volume: float
    turnover: float
    volume_delta: float
    turnover_delta: float
    phase: str
    session_tag: SessionTag
    raw_hash: str
    build_run_id: str

    model_config = ConfigDict(extra="forbid")


class Bar1mRecord(BaseModel):
    instrument_key: str
    symbol: str
    exchange: str
    vt_symbol: str
    bar_dt: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    trade_count: int = Field(ge=0)
    vwap: float
    session_tag: SessionTag
    is_synthetic: bool = False
    build_run_id: str

    model_config = ConfigDict(extra="forbid")


class Bar1dRecord(BaseModel):
    instrument_key: str
    symbol: str
    exchange: str
    vt_symbol: str
    trade_date: date
    bar_dt: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    trade_count: int = Field(ge=0)
    vwap: float
    build_run_id: str

    model_config = ConfigDict(extra="forbid")


class RecordingRun(BaseModel):
    run_id: str
    source_gateway: str
    mode: str
    started_at: datetime
    finished_at: datetime | None = None
    status: str
    notes: str | None = None

    model_config = ConfigDict(extra="forbid")


class RawFileManifest(BaseModel):
    file_id: str
    run_id: str
    trade_date: date
    instrument_key: str
    symbol: str
    exchange: str
    gateway_name: str
    row_count: int = Field(ge=0)
    file_path: str
    file_hash: str
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class StandardFileManifest(BaseModel):
    file_id: str
    build_run_id: str
    layer: str
    trade_date: date | None = None
    instrument_key: str | None = None
    symbol: str | None = None
    exchange: str | None = None
    row_count: int = Field(ge=0)
    file_path: str
    file_hash: str
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class CorporateActionType(StrEnum):
    CASH_DIVIDEND = "cash_dividend"
    STOCK_SPLIT = "stock_split"
    REVERSE_SPLIT = "reverse_split"
    RIGHTS_ISSUE = "rights_issue"
    BONUS_SHARE = "bonus_share"


class CorporateActionRecord(BaseModel):
    action_id: str
    instrument_key: str
    symbol: str
    exchange: str
    action_type: CorporateActionType
    ex_date: date
    effective_date: date
    cash_per_share: float | None = None
    share_ratio: float | None = None
    rights_price: float | None = None
    source: str
    source_hash: str
    loaded_at: datetime

    model_config = ConfigDict(extra="forbid")


class AdjustmentFactorRecord(BaseModel):
    instrument_key: str
    trade_date: date
    adj_factor: float
    adj_mode: str
    source_run_id: str

    model_config = ConfigDict(extra="forbid")


class DQIssue(BaseModel):
    code: str
    severity: str
    message: str
    symbol: str | None = None
    exchange: str | None = None
    instrument_key: str | None = None
    exchange_ts: datetime | None = None

    model_config = ConfigDict(extra="forbid")


class DQReport(BaseModel):
    report_id: str
    layer: str
    trade_date: date | None
    scope: str
    status: str
    issue_count: int = Field(ge=0)
    report_path: str
    created_at: datetime
    issues: list[DQIssue] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")
