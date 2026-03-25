"""Check qlib symbol mappings and calendar coverage for M5."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from apps.research_qlib.workflow import check_symbol_and_calendar_consistency

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-config", default="configs/qlib/base.yaml")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = check_symbol_and_calendar_consistency(
        project_root=ROOT,
        base_config_path=Path(args.base_config),
    )
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
