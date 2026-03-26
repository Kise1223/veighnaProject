"""Trade-server-side contracts for M7 paper execution."""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, ConfigDict, Field

from libs.execution.schemas import PaperRunStatus


class PaperExecutionResult(BaseModel):
    paper_run_id: str = Field(min_length=1)
    execution_task_id: str = Field(min_length=1)
    strategy_run_id: str = Field(min_length=1)
    account_id: str = Field(min_length=1)
    basket_id: str = Field(min_length=1)
    trade_date: date
    status: PaperRunStatus
    order_count: int = Field(ge=0)
    trade_count: int = Field(ge=0)
    report_path: str | None = None
    send_order_called: bool = False
    reused: bool = False

    model_config = ConfigDict(extra="forbid")
