"""Instrument, vt_symbol, and qlib symbol lookup helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from libs.schemas.master_data import Instrument, InstrumentKeyMapping
from scripts.load_master_data import BootstrapPayload, load_bootstrap


@dataclass(frozen=True)
class ResolvedInstrument:
    mapping: InstrumentKeyMapping
    instrument: Instrument


class InstrumentCatalog:
    """Bootstrap-backed symbol registry used across M4."""

    def __init__(self, payload: BootstrapPayload) -> None:
        self._mappings_by_key = {item.instrument_key: item for item in payload.instrument_keys}
        self._mappings_by_vt = {item.vt_symbol: item for item in payload.instrument_keys}
        self._mappings_by_symbol = {
            (item.symbol, item.exchange.value): item for item in payload.instrument_keys
        }
        self._mappings_by_qlib = {item.qlib_symbol: item for item in payload.instrument_keys}
        self._instruments_by_key = {item.instrument_key: item for item in payload.instruments}

    @classmethod
    def from_bootstrap_dir(cls, bootstrap_dir: Path) -> InstrumentCatalog:
        return cls(load_bootstrap(bootstrap_dir))

    def resolve(
        self,
        *,
        instrument_key: str | None = None,
        vt_symbol: str | None = None,
        symbol: str | None = None,
        exchange: str | None = None,
    ) -> ResolvedInstrument:
        mapping: InstrumentKeyMapping | None = None
        if instrument_key:
            mapping = self._mappings_by_key.get(instrument_key)
        elif vt_symbol:
            mapping = self._mappings_by_vt.get(vt_symbol)
        elif symbol and exchange:
            mapping = self._mappings_by_symbol.get((symbol, exchange))
        if mapping is None:
            raise KeyError(
                f"instrument mapping not found for instrument_key={instrument_key}, vt_symbol={vt_symbol}, symbol={symbol}, exchange={exchange}"
            )
        return ResolvedInstrument(mapping=mapping, instrument=self._instruments_by_key[mapping.instrument_key])

    def get_instrument(self, instrument_key: str) -> Instrument:
        return self._instruments_by_key[instrument_key]

    def get_mapping(self, instrument_key: str) -> InstrumentKeyMapping:
        return self._mappings_by_key[instrument_key]

    def all_instrument_keys(self) -> list[str]:
        return sorted(self._instruments_by_key)

    def to_qlib_symbol(self, instrument_key: str) -> str:
        return self._mappings_by_key[instrument_key].qlib_symbol

    def from_qlib_symbol(self, qlib_symbol: str) -> ResolvedInstrument:
        mapping = self._mappings_by_qlib[qlib_symbol]
        return ResolvedInstrument(mapping=mapping, instrument=self._instruments_by_key[mapping.instrument_key])
