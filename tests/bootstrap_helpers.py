from __future__ import annotations

from pathlib import Path

from libs.rules_engine.calendar import load_calendars
from libs.rules_engine.market_rules import RulesRepository
from scripts.load_master_data import load_bootstrap

ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP_DIR = ROOT / "data" / "master" / "bootstrap"


def bootstrap_payload():
    return load_bootstrap(BOOTSTRAP_DIR)


def bootstrap_rules() -> RulesRepository:
    calendars = load_calendars(BOOTSTRAP_DIR / "trading_calendar.json")
    payload = bootstrap_payload()
    return RulesRepository(payload.market_rules, calendars)
