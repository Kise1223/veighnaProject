"""Build standardized ticks, minute bars, daily bars, and adjustment factors."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

from apps.trade_server.app.recording.manifests import make_run_id, make_standard_file_manifest
from libs.marketdata.adjustments import build_adjustment_factors
from libs.marketdata.bars import build_1m_bars, build_daily_bars_from_1m
from libs.marketdata.corporate_actions import load_corporate_actions
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.raw_store import (
    list_partition_files,
    read_partitioned_frame,
    write_partition_frame,
)
from libs.marketdata.schemas import AdjustmentFactorRecord
from libs.marketdata.standardize import load_raw_ticks, normalize_ticks
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.rules_engine.calendar import load_calendars
from libs.rules_engine.market_rules import RulesRepository
from scripts.load_master_data import load_bootstrap

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"
CORPORATE_ACTIONS_PATH = ROOT / "data" / "marketdata" / "bootstrap" / "corporate_actions.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--symbol")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--rebuild", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.set_defaults(with_adjustment=True)
    parser.add_argument("--without-adjustment", action="store_false", dest="with_adjustment")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    build_run_id = make_run_id("build", args.trade_date)
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    payload = load_bootstrap(BOOTSTRAP_DIR)
    rules_repo = RulesRepository(payload.market_rules, load_calendars(BOOTSTRAP_DIR / "trading_calendar.json"))
    manifest_store = ManifestStore(ROOT / "data" / "manifests")
    trade_date = args.trade_date
    targets = _resolve_targets(catalog, symbol=args.symbol)
    results: list[dict[str, object]] = []
    for resolved in targets:
        raw_frame = load_raw_ticks(
            ROOT / "data" / "raw" / "market_ticks",
            trade_date=_date(trade_date),
            symbol=resolved.mapping.symbol,
            exchange=resolved.mapping.exchange.value,
        )
        if raw_frame.empty:
            continue
        normalized = normalize_ticks(
            raw_frame,
            resolved.instrument,
            rules_repo,
            build_run_id=build_run_id,
        )
        if normalized.empty:
            continue
        if not args.dry_run:
            _write_standard_partition(
                manifest_store=manifest_store,
                build_run_id=build_run_id,
                layer="ticks",
                frame=normalized,
                trade_date=_date(trade_date),
                symbol=resolved.mapping.symbol,
                exchange=resolved.mapping.exchange.value,
                instrument_key=resolved.mapping.instrument_key,
                rebuild=args.rebuild,
            )
        bars_1m = build_1m_bars(normalized, build_run_id=build_run_id)
        if not args.dry_run:
            _write_standard_partition(
                manifest_store=manifest_store,
                build_run_id=build_run_id,
                layer="bars_1m",
                frame=bars_1m,
                trade_date=_date(trade_date),
                symbol=resolved.mapping.symbol,
                exchange=resolved.mapping.exchange.value,
                instrument_key=resolved.mapping.instrument_key,
                rebuild=args.rebuild,
            )
        bars_1d = build_daily_bars_from_1m(bars_1m, build_run_id=build_run_id)
        if not args.dry_run:
            _write_standard_partition(
                manifest_store=manifest_store,
                build_run_id=build_run_id,
                layer="bars_1d",
                frame=bars_1d,
                trade_date=_date(trade_date),
                symbol=resolved.mapping.symbol,
                exchange=resolved.mapping.exchange.value,
                instrument_key=resolved.mapping.instrument_key,
                rebuild=args.rebuild,
            )
        results.append(
            {
                "instrument_key": resolved.mapping.instrument_key,
                "ticks": len(normalized),
                "bars_1m": len(bars_1m),
                "bars_1d": len(bars_1d),
            }
        )

    if args.with_adjustment and not args.dry_run:
        actions = load_corporate_actions(CORPORATE_ACTIONS_PATH)
        for action in actions:
            manifest_store.upsert_corporate_action(action)
        daily_frame = read_partitioned_frame(ROOT / "data" / "standard" / "bars_1d")
        if not daily_frame.empty:
            factors = build_adjustment_factors(daily_frame, actions, source_run_id=build_run_id)
            for instrument_key, factor_frame in factors.groupby("instrument_key"):
                mapping = catalog.get_mapping(instrument_key)
                if factor_frame.empty:
                    continue
                latest_trade_date = factor_frame["trade_date"].max()
                file_path = write_partition_frame(
                    factor_frame.to_dict(orient="records"),
                    base_dir=ROOT / "data" / "standard" / "adjustment_factors",
                    trade_date=latest_trade_date,
                    exchange=mapping.exchange.value,
                    symbol=mapping.symbol,
                    file_stem=f"{build_run_id}_adjustment",
                )
                manifest_store.upsert_standard_file_manifest(
                    make_standard_file_manifest(
                        project_root=ROOT,
                        build_run_id=build_run_id,
                        layer="adjustment_factors",
                        row_count=len(factor_frame),
                        file_path=file_path,
                        trade_date=latest_trade_date,
                        instrument_key=instrument_key,
                        symbol=mapping.symbol,
                        exchange=mapping.exchange.value,
                    )
                )
                for row in factor_frame.to_dict(orient="records"):
                    manifest_store.upsert_adjustment_factor(
                        AdjustmentFactorRecord.model_validate(row)
                    )
    sys.stdout.write(
        json.dumps({"build_run_id": build_run_id, "results": results}, ensure_ascii=False, indent=2, default=str)
        + "\n"
    )
    return 0


def _resolve_targets(catalog: InstrumentCatalog, *, symbol: str | None) -> list[Any]:
    if symbol:
        raw_symbol, raw_exchange = symbol.split(".", 1) if "." in symbol else (symbol, None)
        if raw_exchange is None:
            raise SystemExit("--symbol must be formatted as SYMBOL.EXCHANGE")
        return [catalog.resolve(symbol=raw_symbol, exchange=raw_exchange)]
    return [
        catalog.resolve(instrument_key=instrument_key)
        for instrument_key in catalog.all_instrument_keys()
    ]


def _write_standard_partition(
    *,
    manifest_store: ManifestStore,
    build_run_id: str,
    layer: str,
    frame: Any,
    trade_date: date,
    symbol: str,
    exchange: str,
    instrument_key: str,
    rebuild: bool,
) -> None:
    base_dir = ROOT / "data" / "standard" / layer
    if not rebuild and list_partition_files(base_dir, trade_date=trade_date, symbol=symbol, exchange=exchange):
        return
    file_path = write_partition_frame(
        frame.to_dict(orient="records"),
        base_dir=base_dir,
        trade_date=trade_date,
        exchange=exchange,
        symbol=symbol,
        file_stem=build_run_id,
    )
    manifest_store.upsert_standard_file_manifest(
        make_standard_file_manifest(
            project_root=ROOT,
            build_run_id=build_run_id,
            layer=layer,
            row_count=len(frame),
            file_path=file_path,
            trade_date=trade_date,
            instrument_key=instrument_key,
            symbol=symbol,
            exchange=exchange,
        )
    )


def _date(value: str) -> date:
    return date.fromisoformat(value)


if __name__ == "__main__":
    raise SystemExit(main())
