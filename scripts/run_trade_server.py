"""CLI bootstrap for the M3 trade server runtime."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path

from apps.trade_server.app.bootstrap import build_runtime


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--print-health", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    runtime = build_runtime(args.config)
    runtime.start_health_server()
    if args.print_health:
        sys.stdout.write(
            json.dumps(runtime.snapshot_health().model_dump(), ensure_ascii=False, indent=2) + "\n"
        )
        runtime.stop()
        return 0

    keep_running = True

    def stop_handler(signum, frame) -> None:  # type: ignore[no-untyped-def]
        nonlocal keep_running
        keep_running = False

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    try:
        while keep_running:
            time.sleep(0.5)
    finally:
        runtime.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
