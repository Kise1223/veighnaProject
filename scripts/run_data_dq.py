"""Run M4 data quality checks against raw tick parquet."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from libs.marketdata.dq import evaluate_raw_tick_dq, write_dq_report
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.standardize import load_raw_ticks
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.rules_engine.calendar import load_calendars
from libs.rules_engine.market_rules import RulesRepository
from scripts.load_master_data import load_bootstrap

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    trade_date = date.fromisoformat(args.trade_date)
    raw_frame = load_raw_ticks(ROOT / "data" / "raw" / "market_ticks", trade_date=trade_date)
    payload = load_bootstrap(BOOTSTRAP_DIR)
    issues = evaluate_raw_tick_dq(
        raw_frame,
        catalog=InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR),
        rules_repo=RulesRepository(payload.market_rules, load_calendars(BOOTSTRAP_DIR / "trading_calendar.json")),
    )
    report = write_dq_report(
        project_root=ROOT,
        report_root=ROOT / "data" / "dq_reports",
        manifest_store=ManifestStore(ROOT / "data" / "manifests"),
        layer="raw_ticks",
        trade_date=trade_date,
        scope=f"trade_date={trade_date.isoformat()}",
        issues=issues,
    )
    sys.stdout.write(json.dumps(report.model_dump(mode="json"), ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
