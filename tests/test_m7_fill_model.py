from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from libs.common.time import CN_TZ
from libs.execution.fill_model import simulate_limit_fill
from libs.execution.schemas import PaperFillModelConfig, PaperOrderStatus
from libs.marketdata.raw_store import require_parquet_support


def test_fill_model_buy_sell_cross_and_no_cross_are_deterministic() -> None:
    pd = require_parquet_support()
    bars = pd.DataFrame(
        [
            {
                "bar_dt": datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                "low": 9.80,
                "high": 10.20,
            },
            {
                "bar_dt": datetime(2026, 3, 26, 9, 32, tzinfo=CN_TZ),
                "low": 10.10,
                "high": 10.60,
            },
        ]
    )
    config = PaperFillModelConfig()

    buy = simulate_limit_fill(
        side="BUY",
        limit_price=Decimal("10.00"),
        bars=bars,
        config=config,
    )
    sell = simulate_limit_fill(
        side="SELL",
        limit_price=Decimal("10.50"),
        bars=bars,
        config=config,
    )
    no_cross = simulate_limit_fill(
        side="BUY",
        limit_price=Decimal("9.00"),
        bars=bars,
        config=config,
    )
    buy_again = simulate_limit_fill(
        side="BUY",
        limit_price=Decimal("10.00"),
        bars=bars,
        config=config,
    )

    assert buy.status == PaperOrderStatus.FILLED
    assert buy.fill_price == Decimal("10.00")
    assert buy.fill_bar_dt == datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ)
    assert sell.status == PaperOrderStatus.FILLED
    assert sell.fill_price == Decimal("10.50")
    assert sell.fill_bar_dt == datetime(2026, 3, 26, 9, 32, tzinfo=CN_TZ)
    assert no_cross.status == PaperOrderStatus.UNFILLED
    assert no_cross.reason == "limit_not_crossed"
    assert buy_again == buy
