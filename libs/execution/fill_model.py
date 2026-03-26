"""Deterministic bar-driven fill model for M7 paper execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from libs.common.time import ensure_cn_aware
from libs.execution.schemas import PaperFillModelConfig, PaperOrderStatus
from libs.marketdata.raw_store import read_partitioned_frame


@dataclass(frozen=True)
class FillDecision:
    status: PaperOrderStatus
    fill_bar_dt: datetime | None = None
    fill_price: Decimal | None = None
    reason: str | None = None


def load_bars_for_order(
    *,
    project_root: Path,
    trade_date: date,
    exchange: str,
    symbol: str,
    source_standard_build_run_id: str | None,
) -> Any:
    frame = read_partitioned_frame(
        project_root / "data" / "standard" / "bars_1m",
        trade_date=trade_date,
        exchange=exchange,
        symbol=symbol,
    )
    if frame.empty:
        return frame
    if source_standard_build_run_id is not None and "build_run_id" in frame.columns:
        frame = frame[frame["build_run_id"] == source_standard_build_run_id].copy()
    if frame.empty:
        return frame
    frame["bar_dt"] = frame["bar_dt"].map(_as_datetime)
    return frame.sort_values(["bar_dt", "symbol"]).reset_index(drop=True)


def simulate_limit_fill(
    *,
    side: str,
    limit_price: Decimal,
    bars: Any,
    config: PaperFillModelConfig,
) -> FillDecision:
    if bars.empty:
        if config.missing_bar_behavior == "unfilled":
            return FillDecision(status=PaperOrderStatus.UNFILLED, reason="missing_bars")
        return FillDecision(status=PaperOrderStatus.REJECTED, reason="missing_bars")

    for row in bars.to_dict(orient="records"):
        low = Decimal(str(row["low"]))
        high = Decimal(str(row["high"]))
        crossed = low <= limit_price if side == "BUY" else high >= limit_price
        if crossed:
            return FillDecision(
                status=PaperOrderStatus.FILLED,
                fill_bar_dt=_as_datetime(row["bar_dt"]),
                fill_price=limit_price,
            )
    return FillDecision(status=PaperOrderStatus.UNFILLED, reason="limit_not_crossed")


def _as_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return ensure_cn_aware(value)
    return ensure_cn_aware(datetime.fromisoformat(str(value)))
