from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from libs.execution.ledger import PaperLedger
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.rules_engine.calendar import load_calendars
from libs.rules_engine.market_rules import RulesRepository
from libs.schemas.trading import AccountSnapshot, OrderSide, PositionSnapshot
from scripts.load_master_data import load_bootstrap

ROOT = Path(__file__).resolve().parents[1]


def test_paper_ledger_updates_cash_and_t1_sellable_rules() -> None:
    payload = load_bootstrap(ROOT / "data" / "master" / "bootstrap")
    catalog = InstrumentCatalog(payload)
    rules_repo = RulesRepository(
        payload.market_rules,
        load_calendars(ROOT / "data" / "master" / "bootstrap" / "trading_calendar.json"),
    )
    equity = catalog.resolve(instrument_key="EQ_SH_600000").instrument
    account = AccountSnapshot(account_id="demo", available_cash=Decimal("10000"))
    positions = {"EQ_SH_600000": PositionSnapshot(instrument_key="EQ_SH_600000", total_quantity=100, sellable_quantity=100)}
    ledger = PaperLedger.from_snapshots(
        trade_date=date(2026, 3, 26),
        account_snapshot=account,
        positions=positions,
        avg_price_by_instrument={"EQ_SH_600000": Decimal("9.80")},
        instruments={"EQ_SH_600000": equity},
        rules_repo=rules_repo,
        payload=payload,
        broker="DEFAULT",
    )

    sell = ledger.apply_fill(
        instrument=equity,
        side=OrderSide.SELL,
        quantity=100,
        price=Decimal("10.00"),
        previous_close=Decimal("10.00"),
    )
    buy = ledger.apply_fill(
        instrument=equity,
        side=OrderSide.BUY,
        quantity=100,
        price=Decimal("10.00"),
        previous_close=Decimal("10.00"),
    )

    assert sell.accepted is True
    assert buy.accepted is True
    assert ledger.available_cash < Decimal("10990")
    position = ledger.positions["EQ_SH_600000"]
    assert position.quantity == 100
    assert position.sellable_quantity == 0
    assert ledger.fees_total > 0


def test_paper_ledger_handles_t0_cash_and_super_sell_rejections() -> None:
    payload = load_bootstrap(ROOT / "data" / "master" / "bootstrap")
    catalog = InstrumentCatalog(payload)
    rules_repo = RulesRepository(
        payload.market_rules,
        load_calendars(ROOT / "data" / "master" / "bootstrap" / "trading_calendar.json"),
    )
    t0_etf = catalog.resolve(instrument_key="ETF_SH_513100").instrument
    equity = catalog.resolve(instrument_key="EQ_SH_600000").instrument
    account = AccountSnapshot(account_id="demo", available_cash=Decimal("1000"))
    ledger = PaperLedger.from_snapshots(
        trade_date=date(2026, 3, 26),
        account_snapshot=account,
        positions={"EQ_SH_600000": PositionSnapshot(instrument_key="EQ_SH_600000", total_quantity=50, sellable_quantity=50)},
        avg_price_by_instrument={"EQ_SH_600000": Decimal("9.80")},
        instruments={"EQ_SH_600000": equity},
        rules_repo=rules_repo,
        payload=payload,
        broker="DEFAULT",
    )

    t0_buy = ledger.apply_fill(
        instrument=t0_etf,
        side=OrderSide.BUY,
        quantity=100,
        price=Decimal("1.00"),
        previous_close=Decimal("1.00"),
    )
    super_sell = ledger.apply_fill(
        instrument=equity,
        side=OrderSide.SELL,
        quantity=60,
        price=Decimal("10.00"),
        previous_close=Decimal("10.00"),
    )
    cash_fail = ledger.apply_fill(
        instrument=equity,
        side=OrderSide.BUY,
        quantity=1000,
        price=Decimal("10.00"),
        previous_close=Decimal("10.00"),
    )

    assert t0_buy.accepted is True
    assert ledger.positions["ETF_SH_513100"].sellable_quantity == 100
    assert super_sell.accepted is False
    assert super_sell.reason == "sell_quantity_exceeds_sellable"
    assert cash_fail.accepted is False
    assert cash_fail.reason == "insufficient_cash_for_paper_buy"
