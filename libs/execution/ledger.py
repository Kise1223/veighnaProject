"""Local cash and position ledger for M7 paper execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from libs.planning.pretrade import select_cost_profile
from libs.rules_engine import RulesRepository, calc_cost
from libs.schemas.master_data import Instrument
from libs.schemas.trading import AccountSnapshot, CostBreakdown, OrderSide, PositionSnapshot
from scripts.load_master_data import BootstrapPayload


@dataclass
class LedgerPosition:
    instrument_key: str
    symbol: str
    exchange: str
    quantity: int
    sellable_quantity: int
    avg_price: Decimal


@dataclass
class LedgerFillResult:
    accepted: bool
    reason: str | None
    cost: CostBreakdown | None


@dataclass
class PaperLedger:
    account_id: str
    trade_date: date
    cash_start: Decimal
    available_cash: Decimal
    rules_repo: RulesRepository
    payload: BootstrapPayload
    broker: str
    positions: dict[str, LedgerPosition] = field(default_factory=dict)
    fees_total: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")

    @classmethod
    def from_snapshots(
        cls,
        *,
        trade_date: date,
        account_snapshot: AccountSnapshot,
        positions: dict[str, PositionSnapshot],
        avg_price_by_instrument: dict[str, Decimal],
        instruments: dict[str, Instrument],
        rules_repo: RulesRepository,
        payload: BootstrapPayload,
        broker: str,
    ) -> PaperLedger:
        ledger_positions = {
            instrument_key: LedgerPosition(
                instrument_key=instrument_key,
                symbol=instruments[instrument_key].symbol,
                exchange=instruments[instrument_key].exchange.value,
                quantity=position.total_quantity,
                sellable_quantity=position.sellable_quantity,
                avg_price=avg_price_by_instrument[instrument_key],
            )
            for instrument_key, position in positions.items()
        }
        return cls(
            account_id=account_snapshot.account_id,
            trade_date=trade_date,
            cash_start=account_snapshot.available_cash,
            available_cash=account_snapshot.available_cash,
            rules_repo=rules_repo,
            payload=payload,
            broker=broker,
            positions=ledger_positions,
        )

    def get_position(self, instrument: Instrument, previous_close: Decimal) -> LedgerPosition:
        position = self.positions.get(instrument.instrument_key)
        if position is not None:
            return position
        position = LedgerPosition(
            instrument_key=instrument.instrument_key,
            symbol=instrument.symbol,
            exchange=instrument.exchange.value,
            quantity=0,
            sellable_quantity=0,
            avg_price=previous_close,
        )
        self.positions[instrument.instrument_key] = position
        return position

    def apply_fill(
        self,
        *,
        instrument: Instrument,
        side: OrderSide,
        quantity: int,
        price: Decimal,
        previous_close: Decimal,
    ) -> LedgerFillResult:
        position = self.get_position(instrument, previous_close)
        cost_profile = select_cost_profile(
            self.payload,
            trade_date=self.trade_date,
            instrument=instrument,
            broker=self.broker,
        )
        cost = calc_cost(
            trade_date=self.trade_date,
            instrument=instrument,
            cost_profile=cost_profile,
            side=side,
            quantity=quantity,
            price=price,
        )
        if side == OrderSide.SELL:
            if quantity > position.sellable_quantity:
                return LedgerFillResult(
                    accepted=False,
                    reason="sell_quantity_exceeds_sellable",
                    cost=None,
                )
            position.quantity -= quantity
            position.sellable_quantity -= quantity
            self.available_cash += cost.notional - cost.total
            self.fees_total += cost.total
            self.realized_pnl += (price - position.avg_price) * Decimal(quantity) - cost.total
            if position.quantity == 0:
                position.avg_price = Decimal("0")
            return LedgerFillResult(accepted=True, reason=None, cost=cost)

        cash_required = cost.notional + cost.total
        if cash_required > self.available_cash:
            return LedgerFillResult(
                accepted=False,
                reason="insufficient_cash_for_paper_buy",
                cost=None,
            )
        prior_quantity = position.quantity
        self.available_cash -= cash_required
        self.fees_total += cost.total
        position.quantity += quantity
        if self.rules_repo.is_t0_allowed(instrument):
            position.sellable_quantity += quantity
        if prior_quantity == 0:
            position.avg_price = price
        else:
            position.avg_price = (
                (position.avg_price * Decimal(prior_quantity)) + (price * Decimal(quantity))
            ) / Decimal(prior_quantity + quantity)
        return LedgerFillResult(accepted=True, reason=None, cost=cost)
