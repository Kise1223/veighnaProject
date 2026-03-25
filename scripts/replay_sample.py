"""Replay a raw tick or standardized bar parquet sample."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from apps.trade_server.app.replay.bar_replay import replay_bars
from apps.trade_server.app.replay.tick_replay import replay_ticks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    path = args.input.resolve()
    payload: dict[str, Any]
    if "market_ticks" in path.as_posix():
        events = replay_ticks(path)
        payload = {
            "kind": "ticks",
            "count": len(events),
            "first": getattr(events[0], "vt_symbol", None) if events else None,
        }
    else:
        bar_events = replay_bars(path)
        payload = {
            "kind": "bars",
            "count": len(bar_events),
            "first": getattr(bar_events[0], "vt_symbol", None) if bar_events else None,
        }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
