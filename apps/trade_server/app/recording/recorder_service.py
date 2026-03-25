"""Event-driven recorder service that listens for tick callbacks."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from apps.trade_server.app.recording.manifests import (
    finish_recording_run,
    make_recording_run,
    make_run_id,
)
from apps.trade_server.app.recording.sinks import ParquetTickSink
from libs.common.logging import configure_logging
from libs.common.time import ensure_cn_aware
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.raw_store import stable_hash
from libs.marketdata.schemas import RawTickRecord, RecordingRun
from libs.marketdata.symbol_mapping import InstrumentCatalog

LOGGER = configure_logging(logger_name="apps.trade_server.recording.recorder_service")

try:  # pragma: no cover - exercised only with real vnpy installed
    from vnpy.trader.event import EVENT_TICK  # type: ignore
except Exception:  # pragma: no cover - fallback path
    EVENT_TICK = "tick"


class RecorderService:
    """Subscribe to tick events and persist raw parquet batches."""

    def __init__(
        self,
        *,
        event_engine: Any,
        project_root: Path,
        instrument_catalog: InstrumentCatalog,
        sink: ParquetTickSink,
        manifest_store: ManifestStore,
        gateway_name: str,
        mode: str = "live",
    ) -> None:
        self.event_engine = event_engine
        self.project_root = project_root
        self.instrument_catalog = instrument_catalog
        self.sink = sink
        self.manifest_store = manifest_store
        self.gateway_name = gateway_name
        self.mode = mode
        self.run: RecordingRun | None = None
        self._started = False

    def start(self, run_id: str | None = None) -> RecordingRun:
        if self._started and self.run is not None:
            return self.run
        run_id = run_id or make_run_id("recording", self.gateway_name)
        self.run = make_recording_run(run_id=run_id, source_gateway=self.gateway_name, mode=self.mode)
        self.manifest_store.upsert_recording_run(self.run)
        self._register_handler(EVENT_TICK)
        if EVENT_TICK != "tick":
            self._register_handler("tick")
        self._started = True
        return self.run

    def stop(self, *, status: str = "completed", notes: str | None = None) -> None:
        if self.run is None:
            return
        self.flush()
        self.run = finish_recording_run(self.run, status=status, notes=notes)
        self.manifest_store.upsert_recording_run(self.run)
        self._started = False

    def flush(self) -> None:
        if self.run is None:
            return
        manifests = self.sink.flush(run_id=self.run.run_id)
        if manifests:
            LOGGER.info("flushed raw tick parquet", extra={"run_id": self.run.run_id, "files": len(manifests)})

    def handle_event(self, event_or_data: object) -> None:
        data = getattr(event_or_data, "data", event_or_data)
        if data is None:
            return
        self.record_tick(data)

    def record_tick(self, tick: Any) -> RawTickRecord | None:
        try:
            vt_symbol = getattr(tick, "vt_symbol", None) or self._make_vt_symbol(tick)
            resolved = self.instrument_catalog.resolve(vt_symbol=vt_symbol)
        except KeyError:
            LOGGER.warning("skip unmapped tick", extra={"symbol": getattr(tick, "symbol", None)})
            return None

        base_datetime = tick.datetime if hasattr(tick, "datetime") else datetime.now()
        exchange_ts = ensure_cn_aware(getattr(tick, "exchange_ts", None) or base_datetime)
        received_ts = ensure_cn_aware(getattr(tick, "received_ts", None) or base_datetime)
        recorded_at = ensure_cn_aware(datetime.now())
        instrument_key = resolved.mapping.instrument_key
        symbol = resolved.mapping.symbol
        exchange = resolved.mapping.exchange.value
        gateway_name = _coerce_str(getattr(tick, "gateway_name", self.gateway_name))
        last_price = _coerce_float(getattr(tick, "last_price", 0.0))
        volume = _coerce_float(getattr(tick, "volume", 0.0))
        turnover = _coerce_float(getattr(tick, "turnover", 0.0))
        open_interest = _optional_float(getattr(tick, "open_interest", None))
        limit_up = _optional_float(getattr(tick, "limit_up", None))
        limit_down = _optional_float(getattr(tick, "limit_down", None))
        source_seq = _optional_str(getattr(tick, "source_seq", None))
        vt_symbol = resolved.mapping.vt_symbol
        payload = {
            "instrument_key": instrument_key,
            "symbol": symbol,
            "exchange": exchange,
            "vt_symbol": vt_symbol,
            "gateway_name": gateway_name,
            "exchange_ts": exchange_ts.isoformat(),
            "received_ts": received_ts.isoformat(),
            "last_price": last_price,
            "volume": volume,
            "turnover": turnover,
            "open_interest": open_interest,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "source_seq": source_seq,
        }
        for side in ("bid", "ask"):
            for level in range(1, 6):
                payload[f"{side}_price_{level}"] = _optional_float(
                    getattr(tick, f"{side}_price_{level}", None)
                )
                payload[f"{side}_volume_{level}"] = _optional_float(
                    getattr(tick, f"{side}_volume_{level}", None)
                )
        record = RawTickRecord(
            instrument_key=instrument_key,
            symbol=symbol,
            exchange=exchange,
            vt_symbol=vt_symbol,
            gateway_name=gateway_name,
            exchange_ts=exchange_ts,
            received_ts=received_ts,
            last_price=last_price,
            volume=volume,
            turnover=turnover,
            open_interest=open_interest,
            bid_price_1=_optional_float(payload["bid_price_1"]),
            bid_price_2=_optional_float(payload["bid_price_2"]),
            bid_price_3=_optional_float(payload["bid_price_3"]),
            bid_price_4=_optional_float(payload["bid_price_4"]),
            bid_price_5=_optional_float(payload["bid_price_5"]),
            ask_price_1=_optional_float(payload["ask_price_1"]),
            ask_price_2=_optional_float(payload["ask_price_2"]),
            ask_price_3=_optional_float(payload["ask_price_3"]),
            ask_price_4=_optional_float(payload["ask_price_4"]),
            ask_price_5=_optional_float(payload["ask_price_5"]),
            bid_volume_1=_optional_float(payload["bid_volume_1"]),
            bid_volume_2=_optional_float(payload["bid_volume_2"]),
            bid_volume_3=_optional_float(payload["bid_volume_3"]),
            bid_volume_4=_optional_float(payload["bid_volume_4"]),
            bid_volume_5=_optional_float(payload["bid_volume_5"]),
            ask_volume_1=_optional_float(payload["ask_volume_1"]),
            ask_volume_2=_optional_float(payload["ask_volume_2"]),
            ask_volume_3=_optional_float(payload["ask_volume_3"]),
            ask_volume_4=_optional_float(payload["ask_volume_4"]),
            ask_volume_5=_optional_float(payload["ask_volume_5"]),
            limit_up=limit_up,
            limit_down=limit_down,
            source_seq=source_seq,
            raw_hash=stable_hash(payload),
            recorded_at=recorded_at,
        )
        self.sink.write_tick(record)
        return record

    def _register_handler(self, event_type: str) -> None:
        register = getattr(self.event_engine, "register", None)
        if callable(register):
            register(event_type, self.handle_event)

    @staticmethod
    def _make_vt_symbol(tick: Any) -> str:
        exchange = tick.exchange
        exchange_value = getattr(exchange, "value", str(exchange))
        return f"{tick.symbol}.{exchange_value}"


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(str(value))


def _coerce_float(value: object) -> float:
    return float(str(value))


def _coerce_str(value: object) -> str:
    return str(value)


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)
