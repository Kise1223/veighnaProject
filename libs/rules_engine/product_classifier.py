"""Instrument scope and product classification helpers."""

from __future__ import annotations

from libs.schemas.master_data import Board, Instrument, InstrumentType

SUPPORTED_BOARDS = {Board.MAIN, Board.GEM, Board.STAR, Board.ETF}
SUPPORTED_TYPES = {InstrumentType.EQUITY, InstrumentType.ETF}


def is_supported_scope(instrument: Instrument) -> bool:
    """Return whether the instrument is inside the M0-M2 scope freeze."""

    return instrument.instrument_type in SUPPORTED_TYPES and instrument.board in SUPPORTED_BOARDS


def classify_product(instrument: Instrument) -> str:
    """Return a coarse product label used by rules and cost templates."""

    if instrument.instrument_type == InstrumentType.ETF:
        return "ETF"
    if instrument.board == Board.GEM:
        return "GEM_EQUITY"
    if instrument.board == Board.STAR:
        return "STAR_EQUITY"
    return "MAIN_EQUITY"
