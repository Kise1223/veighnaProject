from __future__ import annotations

from datetime import datetime

from libs.common.time import CN_TZ
from libs.marketdata.dq import evaluate_raw_tick_dq
from libs.marketdata.symbol_mapping import InstrumentCatalog
from tests.bootstrap_helpers import BOOTSTRAP_DIR, bootstrap_rules


def _raw_frame(rows: list[dict[str, object]]):  # type: ignore[no-untyped-def]
    pd = __import__("pandas")
    return pd.DataFrame(rows)


def _base_row(*, exchange_ts: datetime, ingest_seq: int | None = None) -> dict[str, object]:
    return {
        "instrument_key": "EQ_SH_600000",
        "symbol": "600000",
        "exchange": "SSE",
        "vt_symbol": "600000.SSE",
        "gateway_name": "OPENCTPSEC",
        "exchange_ts": exchange_ts,
        "received_ts": exchange_ts,
        "last_price": 10.0,
        "volume": 100.0,
        "turnover": 1000.0,
        "ingest_seq": ingest_seq,
        "raw_hash": f"{ingest_seq}_{exchange_ts.isoformat()}",
        "recorded_at": exchange_ts,
    }


def test_dq_accepts_monotonic_raw_ingest_order() -> None:
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    frame = _raw_frame(
        [
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 1, tzinfo=CN_TZ), ingest_seq=1),
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 5, tzinfo=CN_TZ), ingest_seq=2),
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 8, tzinfo=CN_TZ), ingest_seq=3),
        ]
    )
    issues = evaluate_raw_tick_dq(frame, catalog=catalog, rules_repo=bootstrap_rules())
    assert not any(issue.code == "time_regression" for issue in issues)


def test_dq_detects_time_regression_in_original_row_order() -> None:
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    frame = _raw_frame(
        [
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 1, tzinfo=CN_TZ)),
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 6, tzinfo=CN_TZ)),
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 4, tzinfo=CN_TZ)),
        ]
    )
    issues = evaluate_raw_tick_dq(frame, catalog=catalog, rules_repo=bootstrap_rules())
    assert any(issue.code == "time_regression" for issue in issues)


def test_dq_uses_ingest_sequence_even_when_rows_look_sorted_by_timestamp() -> None:
    catalog = InstrumentCatalog.from_bootstrap_dir(BOOTSTRAP_DIR)
    frame = _raw_frame(
        [
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 1, tzinfo=CN_TZ), ingest_seq=1),
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 4, tzinfo=CN_TZ), ingest_seq=3),
            _base_row(exchange_ts=datetime(2026, 3, 25, 9, 30, 6, tzinfo=CN_TZ), ingest_seq=2),
        ]
    )
    issues = evaluate_raw_tick_dq(frame, catalog=catalog, rules_repo=bootstrap_rules())
    assert any(issue.code == "time_regression" for issue in issues)
