"""Trade-server-side dry-run ingestion contracts for M6."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from libs.planning.schemas import ValidationStatus


class DryRunIngestionResult(BaseModel):
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    dry_run: bool
    preview_count: int = Field(ge=0)
    accepted_count: int = Field(ge=0)
    send_order_called: bool = False
    file_path: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class OrderRequestPreviewPayload(BaseModel):
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    side: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    price: Decimal = Field(gt=Decimal("0"))
    reference: str = Field(min_length=1)
    validation_status: ValidationStatus
    validation_reason: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")
