from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from apps.trade_server.app.shadow.session import run_shadow_session
from libs.common.time import CN_TZ
from libs.execution.artifacts import ExecutionArtifactStore
from libs.marketdata.raw_store import stable_hash
from tests.shadow_helpers import make_preview, prepare_shadow_workspace


def test_shadow_source_order_intent_hash_uses_resolved_previous_close(
    tmp_path: Path,
) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.20",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={
            "EQ_SZ_000001": [
                {
                    "bar_dt": datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                    "open": 9.90,
                    "high": 10.30,
                    "low": 9.70,
                    "close": 9.85,
                }
            ]
        },
        market_payload=[
            {
                "instrument_key": "EQ_SZ_000001",
                "last_price": "9.85",
                "previous_close": "9.80",
                "upper_limit": None,
                "lower_limit": None,
                "is_paused": False,
                "exchange_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
                "received_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
            }
        ],
    )
    config_path = workspace / "configs" / "execution" / "shadow_previous_close_hash.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text("limit_price_source: previous_close\n", encoding="utf-8")

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
        config_path=Path("configs/execution/shadow_previous_close_hash.yaml"),
    )

    order = ExecutionArtifactStore(workspace).load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    ).iloc[0]

    expected_hash = stable_hash(
        {
            "execution_task_id": execution_task_id,
            "instrument_key": preview.instrument_key,
            "side": preview.side,
            "quantity": abs(preview.delta_quantity),
            "reference_price": str(preview.reference_price),
            "previous_close": "9.80",
            "source_target_weight_hash": preview.source_target_weight_hash,
        }
    )
    preview_hash = stable_hash(
        {
            "execution_task_id": execution_task_id,
            "instrument_key": preview.instrument_key,
            "side": preview.side,
            "quantity": abs(preview.delta_quantity),
            "reference_price": str(preview.reference_price),
            "previous_close": "10.00",
            "source_target_weight_hash": preview.source_target_weight_hash,
        }
    )

    assert order["previous_close"] == Decimal("9.80")
    assert order["limit_price"] == Decimal("9.80")
    assert order["source_order_intent_hash"] == expected_hash
    assert order["source_order_intent_hash"] != preview_hash


def test_shadow_source_order_intent_hash_reference_price_behavior_is_unchanged(
    tmp_path: Path,
) -> None:
    preview = make_preview(
        instrument_key="EQ_SZ_000001",
        symbol="000001",
        exchange="SZSE",
        side="BUY",
        quantity=100,
        reference_price="10.20",
        previous_close="10.00",
        created_at=datetime(2026, 3, 26, 9, 29, tzinfo=CN_TZ),
    )
    workspace, execution_task_id = prepare_shadow_workspace(
        tmp_path,
        previews=[preview],
        bars_by_instrument={
            "EQ_SZ_000001": [
                {
                    "bar_dt": datetime(2026, 3, 26, 9, 31, tzinfo=CN_TZ),
                    "open": 9.90,
                    "high": 10.30,
                    "low": 9.70,
                    "close": 9.85,
                }
            ]
        },
        market_payload=[
            {
                "instrument_key": "EQ_SZ_000001",
                "last_price": "9.85",
                "previous_close": "9.80",
                "upper_limit": None,
                "lower_limit": None,
                "is_paused": False,
                "exchange_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
                "received_ts": datetime(2026, 3, 26, 9, 30, tzinfo=CN_TZ).isoformat(),
            }
        ],
    )

    result = run_shadow_session(
        project_root=workspace,
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        execution_task_id=execution_task_id,
    )

    order = ExecutionArtifactStore(workspace).load_paper_orders(
        trade_date=date(2026, 3, 26),
        account_id="demo_equity",
        basket_id="baseline_long_only",
        paper_run_id=result.paper_run_id,
    ).iloc[0]

    expected_hash = stable_hash(
        {
            "execution_task_id": execution_task_id,
            "instrument_key": preview.instrument_key,
            "side": preview.side,
            "quantity": abs(preview.delta_quantity),
            "reference_price": str(preview.reference_price),
            "previous_close": "10.00",
            "source_target_weight_hash": preview.source_target_weight_hash,
        }
    )

    assert order["limit_price"] == Decimal("10.20")
    assert order["previous_close"] == Decimal("9.80")
    assert order["source_order_intent_hash"] == expected_hash
