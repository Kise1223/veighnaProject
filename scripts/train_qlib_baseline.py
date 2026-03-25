"""Train the M5 qlib baseline model and persist model_run artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from apps.research_qlib.workflow import train_baseline_workflow

ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-config", default="configs/qlib/base.yaml")
    parser.add_argument("--dataset-config", default="configs/qlib/dataset_baseline.yaml")
    parser.add_argument("--model-config", default="configs/qlib/model_baseline.yaml")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = train_baseline_workflow(
        project_root=ROOT,
        base_config_path=Path(args.base_config),
        dataset_config_path=Path(args.dataset_config),
        model_config_path=Path(args.model_config),
    )
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
