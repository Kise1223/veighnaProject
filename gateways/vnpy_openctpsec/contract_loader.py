"""Contract helpers for bootstrap-backed mock environments."""

from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

from gateways.vnpy_openctpsec.mapper import AdapterContractEvent


class ContractLoader:
    """Builds adapter contract payloads from bootstrap master data."""

    def __init__(self, bootstrap_dir: Path) -> None:
        self.bootstrap_dir = bootstrap_dir

    def load_contracts(self) -> list[AdapterContractEvent]:
        instruments_path = self.bootstrap_dir / "instruments.csv"
        contracts: list[AdapterContractEvent] = []
        with instruments_path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                contracts.append(
                    AdapterContractEvent(
                        symbol=row["symbol"],
                        exchange=row["exchange"],
                        name=row["symbol"],
                        product=row["instrument_type"],
                        size=1,
                        pricetick=Decimal(row["pricetick"]),
                        min_volume=int(row["min_buy_lot"]),
                    )
                )
        return contracts
