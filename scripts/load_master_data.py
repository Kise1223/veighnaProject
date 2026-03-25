"""Validate and optionally load bootstrap master data into PostgreSQL."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from libs.common.logging import configure_logging
from libs.rules_engine.calendar import load_calendars
from libs.rules_engine.market_rules import RulesRepository
from libs.schemas.master_data import (
    BootstrapManifest,
    CostProfile,
    Instrument,
    InstrumentKeyMapping,
    MarketRuleSnapshot,
)

LOGGER = configure_logging(logger_name="scripts.load_master_data")
ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass
class BootstrapPayload:
    instrument_keys: list[InstrumentKeyMapping]
    instruments: list[Instrument]
    market_rules: list[MarketRuleSnapshot]
    cost_profiles: list[CostProfile]
    manifest: BootstrapManifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "master" / "bootstrap",
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="validate files without writing to PostgreSQL"
    )
    parser.add_argument(
        "--apply", action="store_true", help="apply validated records to PostgreSQL"
    )
    parser.add_argument("--dsn", default=os.getenv("POSTGRES_DSN"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = load_bootstrap(args.bootstrap_dir)
    validate_bootstrap(payload, args.bootstrap_dir)
    LOGGER.info(
        "validated bootstrap payload",
        extra={
            "instrument_keys": len(payload.instrument_keys),
            "instruments": len(payload.instruments),
            "market_rules": len(payload.market_rules),
            "cost_profiles": len(payload.cost_profiles),
        },
    )
    if args.apply:
        if not args.dsn:
            raise SystemExit("--apply requires --dsn or POSTGRES_DSN")
        apply_to_postgres(payload, dsn=args.dsn)
    return 0


def load_bootstrap(bootstrap_dir: Path) -> BootstrapPayload:
    manifest = BootstrapManifest.model_validate_json(
        (bootstrap_dir / "bootstrap_manifest.json").read_text(encoding="utf-8")
    )
    instrument_keys = _load_csv_models(bootstrap_dir / "instrument_keys.csv", InstrumentKeyMapping)
    instruments = _load_csv_models(bootstrap_dir / "instruments.csv", Instrument)
    cost_profiles = _load_csv_models(bootstrap_dir / "cost_profiles.csv", CostProfile)
    rule_payload = json.loads((bootstrap_dir / "market_rules.json").read_text(encoding="utf-8"))
    market_rules = [
        MarketRuleSnapshot.model_validate(item) for item in rule_payload["market_rules"]
    ]
    return BootstrapPayload(
        instrument_keys=instrument_keys,
        instruments=instruments,
        market_rules=market_rules,
        cost_profiles=cost_profiles,
        manifest=manifest,
    )


def validate_bootstrap(payload: BootstrapPayload, bootstrap_dir: Path) -> None:
    manifest_files = {entry.file for entry in payload.manifest.entries}
    expected_files = {
        "cost_profiles.csv",
        "instrument_keys.csv",
        "instruments.csv",
        "market_rules.json",
        "trading_calendar.json",
    }
    missing_manifest_entries = expected_files.difference(manifest_files)
    if missing_manifest_entries:
        raise ValueError(f"missing manifest entries: {sorted(missing_manifest_entries)}")

    _validate_unique(payload.instrument_keys, "instrument_key", "duplicate instrument_key mapping")
    _validate_unique(payload.instrument_keys, "vt_symbol", "duplicate vt_symbol mapping")
    _validate_unique(payload.instruments, "instrument_key", "duplicate instrument rows")
    _validate_unique(payload.cost_profiles, "cost_profile_id", "duplicate cost profiles")

    instrument_keys = {item.instrument_key for item in payload.instrument_keys}
    for instrument in payload.instruments:
        if instrument.instrument_key not in instrument_keys:
            raise ValueError(
                f"instrument {instrument.instrument_key} missing instrument_key mapping"
            )

    calendars = load_calendars(bootstrap_dir / "trading_calendar.json")
    RulesRepository(payload.market_rules, calendars)
    _validate_cost_profile_overlaps(payload.cost_profiles)

    for entry in payload.manifest.entries:
        target = bootstrap_dir / entry.file
        if not target.exists():
            raise ValueError(f"manifest entry points to missing file: {entry.file}")
        actual_hash = hashlib.sha256(target.read_bytes()).hexdigest().upper()
        if actual_hash != entry.source_hash.upper():
            raise ValueError(f"hash mismatch for {entry.file}")


def apply_to_postgres(payload: BootstrapPayload, dsn: str) -> None:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - depends on optional package
        raise RuntimeError("psycopg is required for --apply") from exc

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            for mapping in payload.instrument_keys:
                cursor.execute(
                    """
                    INSERT INTO instrument_keys (
                        instrument_key, canonical_symbol, vendor_symbol, broker_symbol, vt_symbol, qlib_symbol,
                        symbol, exchange, source, source_version, effective_from, effective_to
                    ) VALUES (%(instrument_key)s, %(canonical_symbol)s, %(vendor_symbol)s, %(broker_symbol)s, %(vt_symbol)s,
                              %(qlib_symbol)s, %(symbol)s, %(exchange)s, %(source)s, %(source_version)s,
                              %(effective_from)s, %(effective_to)s)
                    ON CONFLICT (instrument_key) DO UPDATE SET
                        canonical_symbol = EXCLUDED.canonical_symbol,
                        vendor_symbol = EXCLUDED.vendor_symbol,
                        broker_symbol = EXCLUDED.broker_symbol,
                        vt_symbol = EXCLUDED.vt_symbol,
                        qlib_symbol = EXCLUDED.qlib_symbol,
                        symbol = EXCLUDED.symbol,
                        exchange = EXCLUDED.exchange,
                        source = EXCLUDED.source,
                        source_version = EXCLUDED.source_version,
                        effective_from = EXCLUDED.effective_from,
                        effective_to = EXCLUDED.effective_to
                    """,
                    mapping.model_dump(),
                )
            for instrument in payload.instruments:
                cursor.execute(
                    """
                    INSERT INTO instruments (
                        instrument_key, exchange, symbol, instrument_type, board, list_date, delist_date, settlement_type,
                        pricetick, min_buy_lot, odd_lot_sell_only, limit_pct, ipo_free_limit_days,
                        after_hours_fixed_price_supported, source, source_version, effective_from, effective_to
                    ) VALUES (%(instrument_key)s, %(exchange)s, %(symbol)s, %(instrument_type)s, %(board)s, %(list_date)s,
                              %(delist_date)s, %(settlement_type)s, %(pricetick)s, %(min_buy_lot)s, %(odd_lot_sell_only)s,
                              %(limit_pct)s, %(ipo_free_limit_days)s, %(after_hours_fixed_price_supported)s, %(source)s,
                              %(source_version)s, %(effective_from)s, %(effective_to)s)
                    ON CONFLICT (instrument_key) DO UPDATE SET
                        exchange = EXCLUDED.exchange,
                        symbol = EXCLUDED.symbol,
                        instrument_type = EXCLUDED.instrument_type,
                        board = EXCLUDED.board,
                        list_date = EXCLUDED.list_date,
                        delist_date = EXCLUDED.delist_date,
                        settlement_type = EXCLUDED.settlement_type,
                        pricetick = EXCLUDED.pricetick,
                        min_buy_lot = EXCLUDED.min_buy_lot,
                        odd_lot_sell_only = EXCLUDED.odd_lot_sell_only,
                        limit_pct = EXCLUDED.limit_pct,
                        ipo_free_limit_days = EXCLUDED.ipo_free_limit_days,
                        after_hours_fixed_price_supported = EXCLUDED.after_hours_fixed_price_supported,
                        source = EXCLUDED.source,
                        source_version = EXCLUDED.source_version,
                        effective_from = EXCLUDED.effective_from,
                        effective_to = EXCLUDED.effective_to
                    """,
                    instrument.model_dump(),
                )
            for rule in payload.market_rules:
                rule_data = rule.model_dump(mode="json")
                rule_data["trading_sessions"] = Jsonb(rule_data["trading_sessions"])
                rule_data["cancel_restricted_windows"] = Jsonb(
                    rule_data["cancel_restricted_windows"]
                )
                cursor.execute(
                    """
                    INSERT INTO market_rules (
                        rule_id, exchange, instrument_type, board, effective_from, effective_to, trading_sessions,
                        cancel_restricted_windows, price_limit_ratio, ipo_free_limit_days, after_hours_supported,
                        source, source_version
                    ) VALUES (%(rule_id)s, %(exchange)s, %(instrument_type)s, %(board)s, %(effective_from)s,
                              %(effective_to)s, %(trading_sessions)s, %(cancel_restricted_windows)s,
                              %(price_limit_ratio)s, %(ipo_free_limit_days)s, %(after_hours_supported)s,
                              %(source)s, %(source_version)s)
                    ON CONFLICT (rule_id) DO UPDATE SET
                        exchange = EXCLUDED.exchange,
                        instrument_type = EXCLUDED.instrument_type,
                        board = EXCLUDED.board,
                        effective_from = EXCLUDED.effective_from,
                        effective_to = EXCLUDED.effective_to,
                        trading_sessions = EXCLUDED.trading_sessions,
                        cancel_restricted_windows = EXCLUDED.cancel_restricted_windows,
                        price_limit_ratio = EXCLUDED.price_limit_ratio,
                        ipo_free_limit_days = EXCLUDED.ipo_free_limit_days,
                        after_hours_supported = EXCLUDED.after_hours_supported,
                        source = EXCLUDED.source,
                        source_version = EXCLUDED.source_version
                    """,
                    rule_data,
                )
            for profile in payload.cost_profiles:
                cursor.execute(
                    """
                    INSERT INTO cost_profiles (
                        cost_profile_id, broker, instrument_type, exchange, effective_from, effective_to, commission_rate,
                        commission_min, tax_sell_rate, handling_fee_rate, transfer_fee_rate, reg_fee_rate, source,
                        source_version
                    ) VALUES (%(cost_profile_id)s, %(broker)s, %(instrument_type)s, %(exchange)s, %(effective_from)s,
                              %(effective_to)s, %(commission_rate)s, %(commission_min)s, %(tax_sell_rate)s,
                              %(handling_fee_rate)s, %(transfer_fee_rate)s, %(reg_fee_rate)s, %(source)s,
                              %(source_version)s)
                    ON CONFLICT (cost_profile_id) DO UPDATE SET
                        broker = EXCLUDED.broker,
                        instrument_type = EXCLUDED.instrument_type,
                        exchange = EXCLUDED.exchange,
                        effective_from = EXCLUDED.effective_from,
                        effective_to = EXCLUDED.effective_to,
                        commission_rate = EXCLUDED.commission_rate,
                        commission_min = EXCLUDED.commission_min,
                        tax_sell_rate = EXCLUDED.tax_sell_rate,
                        handling_fee_rate = EXCLUDED.handling_fee_rate,
                        transfer_fee_rate = EXCLUDED.transfer_fee_rate,
                        reg_fee_rate = EXCLUDED.reg_fee_rate,
                        source = EXCLUDED.source,
                        source_version = EXCLUDED.source_version
                    """,
                    profile.model_dump(),
                )


def _load_csv_models(path: Path, model_type: type[ModelT]) -> list[ModelT]:
    results = []
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            cleaned = {key: (value if value != "" else None) for key, value in row.items()}
            try:
                results.append(model_type.model_validate(cleaned))
            except ValidationError as exc:
                raise ValueError(f"invalid row in {path.name}: {exc}") from exc
    return results


def _validate_unique(items: list[Any], field_name: str, error_message: str) -> None:
    seen: set[Any] = set()
    for item in items:
        value = getattr(item, field_name)
        if value in seen:
            raise ValueError(f"{error_message}: {value}")
        seen.add(value)


def _validate_cost_profile_overlaps(cost_profiles: list[CostProfile]) -> None:
    grouped: dict[tuple[str, str, str | None], list[CostProfile]] = {}
    for profile in cost_profiles:
        grouped.setdefault(
            (
                profile.broker,
                profile.instrument_type.value,
                profile.exchange.value if profile.exchange else None,
            ),
            [],
        ).append(profile)
    for snapshots in grouped.values():
        ordered = sorted(snapshots, key=lambda item: item.effective_from)
        for current, next_item in zip(ordered, ordered[1:], strict=False):
            current_end = current.effective_to or next_item.effective_from
            if current_end >= next_item.effective_from:
                raise ValueError(
                    f"overlapping cost profiles: {current.cost_profile_id} and {next_item.cost_profile_id}"
                )


if __name__ == "__main__":
    raise SystemExit(main())
