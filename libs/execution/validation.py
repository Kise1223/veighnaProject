"""Validation helpers for M7 paper execution."""

from __future__ import annotations

from decimal import Decimal

from libs.schemas.master_data import Instrument


def validate_static_order_inputs(
    *,
    instrument: Instrument | None,
    previous_close: Decimal | None,
) -> str | None:
    if instrument is None:
        return "symbol_mapping_missing"
    if previous_close is None:
        return "previous_close_missing"
    return None


def validate_sellable_quantity(*, requested: int, sellable_quantity: int) -> str | None:
    if requested > sellable_quantity:
        return "sell_quantity_exceeds_sellable"
    return None


def validate_cash_available(
    *,
    available_cash: Decimal,
    required_cash: Decimal,
    policy: str,
) -> str | None:
    if required_cash <= available_cash:
        return None
    if policy == "reject":
        return "insufficient_cash_for_paper_buy"
    return "unsupported_cash_policy"
