"""Minimal adjustment factor builder for M4 provider exports."""

from __future__ import annotations

from collections import defaultdict

from libs.marketdata.raw_store import require_parquet_support
from libs.marketdata.schemas import CorporateActionRecord, CorporateActionType


def build_adjustment_factors(daily_bars, actions: list[CorporateActionRecord], *, source_run_id: str):  # type: ignore[no-untyped-def]
    pd = require_parquet_support()
    if daily_bars.empty:
        return pd.DataFrame(
            columns=["instrument_key", "trade_date", "adj_factor", "adj_mode", "source_run_id"]
        )

    action_map: dict[str, dict[object, list[CorporateActionRecord]]] = defaultdict(lambda: defaultdict(list))
    for action in actions:
        action_map[action.instrument_key][action.effective_date].append(action)

    results: list[dict[str, object]] = []
    for instrument_key, frame in daily_bars.groupby("instrument_key"):
        ordered = frame.sort_values("trade_date").reset_index(drop=True)
        cumulative = 1.0
        factors_by_date: dict[object, float] = {}
        rows = ordered.to_dict(orient="records")
        for index in range(len(rows) - 1, -1, -1):
            row = rows[index]
            trade_date = row["trade_date"]
            factors_by_date[trade_date] = cumulative
            for action in action_map[instrument_key].get(trade_date, []):
                if index == 0:
                    continue
                previous_close = float(rows[index - 1]["close"])
                cumulative *= _event_factor(action, previous_close)

        for trade_date, factor in sorted(factors_by_date.items()):
            results.append(
                {
                    "instrument_key": instrument_key,
                    "trade_date": trade_date,
                    "adj_factor": round(factor, 8),
                    "adj_mode": "forward",
                    "source_run_id": source_run_id,
                }
            )
    return pd.DataFrame(results).sort_values(["instrument_key", "trade_date"]).reset_index(drop=True)


def _event_factor(action: CorporateActionRecord, previous_close: float) -> float:
    if previous_close <= 0:
        return 1.0
    if action.action_type == CorporateActionType.CASH_DIVIDEND:
        cash = float(action.cash_per_share or 0.0)
        return max((previous_close - cash) / previous_close, 0.0)
    if action.action_type in {CorporateActionType.STOCK_SPLIT, CorporateActionType.BONUS_SHARE}:
        ratio = float(action.share_ratio or 0.0)
        return 1.0 / (1.0 + ratio) if ratio >= 0 else 1.0
    if action.action_type == CorporateActionType.REVERSE_SPLIT:
        ratio = float(action.share_ratio or 1.0)
        return 1.0 / ratio if ratio > 0 else 1.0
    if action.action_type == CorporateActionType.RIGHTS_ISSUE:
        ratio = float(action.share_ratio or 0.0)
        rights_price = float(action.rights_price or 0.0)
        denominator = previous_close * (1.0 + ratio)
        if denominator <= 0:
            return 1.0
        return (previous_close + rights_price * ratio) / denominator
    return 1.0
