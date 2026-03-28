"""In-memory order state used by the M8 shadow session engine."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from libs.execution.shadow_schemas import ShadowOrderState
from libs.schemas.master_data import Instrument
from libs.schemas.trading import OrderSide


@dataclass
class ShadowWorkingOrder:
    order_id: str
    instrument: Instrument
    side: OrderSide
    quantity: int
    remaining_quantity: int
    filled_quantity: int
    reference_price: Decimal
    limit_price: Decimal
    previous_close: Decimal
    estimated_cost: Decimal
    cumulative_notional: Decimal
    activation_dt: datetime
    expiry_dt: datetime
    state: ShadowOrderState
    status_reason: str | None
    source_order_intent_hash: str
    created_at: datetime
    creation_seq: int
    last_fill_dt: datetime | None
    source_prediction_run_id: str
    source_qlib_export_run_id: str | None
    source_standard_build_run_id: str | None


def shadow_order_sort_key(order: ShadowWorkingOrder) -> tuple[int, str, str]:
    side_rank = 0 if order.side == OrderSide.SELL else 1
    return (side_rank, order.instrument.symbol, order.instrument.instrument_key)


def shadow_order_fifo_key(order: ShadowWorkingOrder) -> tuple[datetime, int, str]:
    return (order.created_at, order.creation_seq, order.order_id)
