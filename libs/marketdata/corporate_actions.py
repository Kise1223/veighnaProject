"""Corporate action bootstrap loading for M4."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash
from libs.marketdata.schemas import CorporateActionRecord


def load_corporate_actions(path: Path) -> list[CorporateActionRecord]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    actions: list[CorporateActionRecord] = []
    for item in payload["corporate_actions"]:
        enriched = dict(item)
        enriched.setdefault(
            "action_id",
            stable_hash(
                {
                    "instrument_key": item["instrument_key"],
                    "action_type": item["action_type"],
                    "effective_date": item["effective_date"],
                    "source": item["source"],
                }
            )[:24],
        )
        enriched["loaded_at"] = ensure_cn_aware(datetime.fromisoformat(enriched["loaded_at"]))
        actions.append(CorporateActionRecord.model_validate(enriched))
    return actions
