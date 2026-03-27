"""Trade-server-side contracts for M8 shadow sessions."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field

from libs.execution.shadow_schemas import ShadowRunStatus


class ShadowSessionResult(BaseModel):
    shadow_run_id: str = Field(min_length=1)
    paper_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    status: ShadowRunStatus
    order_count: int = Field(ge=0)
    fill_count: int = Field(ge=0)
    report_path: str | None = None
    paper_report_path: str | None = None
    send_order_called: bool = False
    reused: bool = False

    model_config = ConfigDict(extra="forbid")

