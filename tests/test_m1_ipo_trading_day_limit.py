from __future__ import annotations

from datetime import date
from decimal import Decimal

from tests.bootstrap_helpers import bootstrap_payload, bootstrap_rules


def _instrument(instrument_key: str):
    payload = bootstrap_payload()
    return next(item for item in payload.instruments if item.instrument_key == instrument_key)


def test_ipo_free_limit_counts_trading_days_in_normal_week() -> None:
    repo = bootstrap_rules()
    stock = _instrument("EQ_SH_600000").model_copy(update={"list_date": date(2026, 3, 23)})
    assert repo.get_price_limit(date(2026, 3, 27), stock, None, None).upper_limit is None
    assert repo.get_price_limit(date(2026, 3, 30), stock, Decimal("10"), None).upper_limit == Decimal("11.00")


def test_ipo_free_limit_skips_weekend_for_szse() -> None:
    repo = bootstrap_rules()
    stock = _instrument("EQ_SZ_000001").model_copy(update={"list_date": date(2026, 3, 26)})
    assert repo.get_price_limit(date(2026, 4, 1), stock, None, None).upper_limit is None
    assert repo.get_price_limit(date(2026, 4, 2), stock, Decimal("10"), None).upper_limit == Decimal("11.00")


def test_ipo_free_limit_skips_exchange_holiday_for_sse() -> None:
    repo = bootstrap_rules()
    stock = _instrument("EQ_SH_600000").model_copy(update={"list_date": date(2026, 2, 12)})
    assert repo.get_price_limit(date(2026, 2, 19), stock, None, None).upper_limit is None
    assert repo.get_price_limit(date(2026, 2, 20), stock, Decimal("10"), None).upper_limit == Decimal("11.00")
