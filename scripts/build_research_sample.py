"""Build a deterministic multi-day research sample and export qlib providers."""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from libs.common.time import CN_TZ
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.manifests import make_standard_file_manifest
from libs.marketdata.qlib_export import export_qlib_provider
from libs.marketdata.raw_store import write_partition_frame
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.rules_engine.calendar import is_trade_day, load_calendars

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SPEC_PATH = ROOT / "data" / "bootstrap" / "research_sample" / "sample_universe.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild", action="store_true")
    return parser.parse_args()


def build_research_sample(*, project_root: Path, rebuild: bool = False) -> dict[str, object]:
    spec_path = project_root / "data" / "bootstrap" / "research_sample" / "sample_universe.json"
    manifest_path = project_root / "data" / "bootstrap" / "research_sample" / "sample_manifest.json"
    if manifest_path.exists() and not rebuild and _sample_outputs_ready(project_root):
        return cast(dict[str, object], json.loads(manifest_path.read_text(encoding="utf-8")))

    spec = cast(dict[str, Any], json.loads(spec_path.read_text(encoding="utf-8")))
    catalog = InstrumentCatalog.from_bootstrap_dir(project_root / "data" / "master" / "bootstrap")
    calendars = load_calendars(project_root / "data" / "master" / "bootstrap" / "trading_calendar.json")
    trade_dates = _trade_dates(
        start=date.fromisoformat(spec["trade_date_start"]),
        end=date.fromisoformat(spec["trade_date_end"]),
        calendars=calendars,
    )
    manifest_store = ManifestStore(project_root / "data" / "manifests")
    standard_build_run_id = "research_sample_standard_v1"
    for instrument_offset, instrument_key in enumerate(spec["instrument_keys"]):
        mapping = catalog.get_mapping(instrument_key)
        for trade_day in trade_dates:
            minute_rows = _minute_rows(
                instrument_key=instrument_key,
                symbol=mapping.symbol,
                exchange=mapping.exchange.value,
                vt_symbol=mapping.vt_symbol,
                trade_date=trade_day,
                instrument_offset=instrument_offset,
                build_run_id=standard_build_run_id,
            )
            bars_1m_path = write_partition_frame(
                minute_rows,
                base_dir=project_root / "data" / "standard" / "bars_1m",
                trade_date=trade_day,
                exchange=mapping.exchange.value,
                symbol=mapping.symbol,
                file_stem=standard_build_run_id,
            )
            manifest_store.upsert_standard_file_manifest(
                make_standard_file_manifest(
                    project_root=project_root,
                    build_run_id=standard_build_run_id,
                    layer="bars_1m",
                    row_count=len(minute_rows),
                    file_path=bars_1m_path,
                    trade_date=trade_day,
                    instrument_key=instrument_key,
                    symbol=mapping.symbol,
                    exchange=mapping.exchange.value,
                )
            )
            daily_row = _daily_row(minute_rows)
            bars_1d_path = write_partition_frame(
                [daily_row],
                base_dir=project_root / "data" / "standard" / "bars_1d",
                trade_date=trade_day,
                exchange=mapping.exchange.value,
                symbol=mapping.symbol,
                file_stem=standard_build_run_id,
            )
            manifest_store.upsert_standard_file_manifest(
                make_standard_file_manifest(
                    project_root=project_root,
                    build_run_id=standard_build_run_id,
                    layer="bars_1d",
                    row_count=1,
                    file_path=bars_1d_path,
                    trade_date=trade_day,
                    instrument_key=instrument_key,
                    symbol=mapping.symbol,
                    exchange=mapping.exchange.value,
                )
            )
            factor_row = {
                "instrument_key": instrument_key,
                "trade_date": trade_day.isoformat(),
                "adj_factor": 1.0,
                "adj_mode": "forward",
                "source_run_id": standard_build_run_id,
            }
            factor_path = write_partition_frame(
                [factor_row],
                base_dir=project_root / "data" / "standard" / "adjustment_factors",
                trade_date=trade_day,
                exchange=mapping.exchange.value,
                symbol=mapping.symbol,
                file_stem=standard_build_run_id,
            )
            manifest_store.upsert_standard_file_manifest(
                make_standard_file_manifest(
                    project_root=project_root,
                    build_run_id=standard_build_run_id,
                    layer="adjustment_factors",
                    row_count=1,
                    file_path=factor_path,
                    trade_date=trade_day,
                    instrument_key=instrument_key,
                    symbol=mapping.symbol,
                    exchange=mapping.exchange.value,
                )
            )
    export_day_run_id = "research_sample_qlib_day_v1"
    export_minute_run_id = "research_sample_qlib_1min_v1"
    day_payload = export_qlib_provider(
        project_root=project_root,
        provider_root=project_root / "data" / "qlib_bin",
        catalog=catalog,
        manifest_store=manifest_store,
        freq="1d",
        build_run_id=export_day_run_id,
        source_build_run_ids=[standard_build_run_id],
    )
    minute_payload = export_qlib_provider(
        project_root=project_root,
        provider_root=project_root / "data" / "qlib_bin",
        catalog=catalog,
        manifest_store=manifest_store,
        freq="1min",
        build_run_id=export_minute_run_id,
        source_build_run_ids=[standard_build_run_id],
    )
    result = {
        "sample_name": str(spec["sample_name"]),
        "standard_build_run_id": standard_build_run_id,
        "qlib_export_run_ids": {"day": export_day_run_id, "1min": export_minute_run_id},
        "trade_dates": [item.isoformat() for item in trade_dates],
        "instrument_keys": [str(item) for item in spec["instrument_keys"]],
        "day_rows": day_payload["rows"],
        "minute_rows": minute_payload["rows"],
    }
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main() -> int:
    args = parse_args()
    result = build_research_sample(project_root=ROOT, rebuild=args.rebuild)
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


def _trade_dates(*, start: date, end: date, calendars) -> list[date]:  # type: ignore[no-untyped-def]
    current = start
    results: list[date] = []
    while current <= end:
        if is_trade_day(current, exchange=list(calendars)[0], calendars=calendars):
            results.append(current)
        current += timedelta(days=1)
    return results


def _minute_rows(
    *,
    instrument_key: str,
    symbol: str,
    exchange: str,
    vt_symbol: str,
    trade_date: date,
    instrument_offset: int,
    build_run_id: str,
) -> list[dict[str, object]]:
    base_level = 8.0 + instrument_offset * 4.0
    day_offset = (trade_date - date(2026, 2, 2)).days
    rows: list[dict[str, object]] = []
    for minute_index in range(5):
        bar_dt = datetime(
            trade_date.year,
            trade_date.month,
            trade_date.day,
            9,
            31 + minute_index,
            tzinfo=CN_TZ,
        )
        drift = 0.06 * day_offset
        pulse = math.sin((day_offset + minute_index + instrument_offset) / 4) * 0.12
        open_price = round(base_level + drift + minute_index * 0.03 + pulse, 3)
        close_price = round(open_price + 0.015 * ((minute_index % 3) - 1), 3)
        high_price = round(max(open_price, close_price) + 0.02, 3)
        low_price = round(min(open_price, close_price) - 0.02, 3)
        volume = float(800 + instrument_offset * 90 + day_offset * 8 + minute_index * 11)
        turnover = float(round(((open_price + close_price) / 2) * volume, 3))
        rows.append(
            {
                "instrument_key": instrument_key,
                "symbol": symbol,
                "exchange": exchange,
                "vt_symbol": vt_symbol,
                "bar_dt": bar_dt.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "turnover": turnover,
                "trade_count": 6 + minute_index,
                "vwap": round(turnover / volume, 6),
                "session_tag": "am",
                "is_synthetic": False,
                "build_run_id": build_run_id,
            }
        )
    return rows


def _daily_row(minute_rows: list[dict[str, object]]) -> dict[str, object]:
    first = minute_rows[0]
    last = minute_rows[-1]
    volume_values = [_as_float(item["volume"]) for item in minute_rows]
    turnover_values = [_as_float(item["turnover"]) for item in minute_rows]
    volume = float(sum(volume_values))
    turnover = float(sum(turnover_values))
    trade_date = str(first["bar_dt"])[:10]
    high_values = [_as_float(item["high"]) for item in minute_rows]
    low_values = [_as_float(item["low"]) for item in minute_rows]
    trade_counts = [_as_int(item["trade_count"]) for item in minute_rows]
    return {
        "instrument_key": first["instrument_key"],
        "symbol": first["symbol"],
        "exchange": first["exchange"],
        "vt_symbol": first["vt_symbol"],
        "trade_date": trade_date,
        "bar_dt": f"{trade_date}T15:00:00+08:00",
        "open": _as_float(first["open"]),
        "high": max(high_values),
        "low": min(low_values),
        "close": _as_float(last["close"]),
        "volume": volume,
        "turnover": turnover,
        "trade_count": sum(trade_counts),
        "vwap": round(turnover / volume, 6),
        "build_run_id": first["build_run_id"],
    }


def _as_float(value: object) -> float:
    return float(str(value))


def _as_int(value: object) -> int:
    return int(str(value))


def _sample_outputs_ready(project_root: Path) -> bool:
    required = [
        project_root / "data" / "qlib_bin" / "instruments" / "all.txt",
        project_root / "data" / "qlib_bin" / "export_manifest_day.json",
        project_root / "data" / "qlib_bin" / "export_manifest_1min.json",
    ]
    return all(path.exists() for path in required)


if __name__ == "__main__":
    raise SystemExit(main())
