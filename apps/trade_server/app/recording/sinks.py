"""Recorder sinks that flush captured ticks into parquet."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path

from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.manifests import make_raw_file_manifest
from libs.marketdata.raw_store import write_partition_frame
from libs.marketdata.schemas import RawFileManifest, RawTickRecord


class ParquetTickSink:
    """Buffer tick rows and flush them into partitioned raw parquet files."""

    def __init__(
        self,
        *,
        project_root: Path,
        raw_root: Path,
        manifest_store: ManifestStore,
    ) -> None:
        self.project_root = project_root
        self.raw_root = raw_root
        self.manifest_store = manifest_store
        self._buffers: dict[tuple[date, str, str], list[RawTickRecord]] = defaultdict(list)
        self._file_seq = 0

    def write_tick(self, tick: RawTickRecord) -> None:
        trade_date = tick.exchange_ts.date()
        self._buffers[(trade_date, tick.exchange, tick.symbol)].append(tick)

    def flush(self, *, run_id: str) -> list[RawFileManifest]:
        manifests: list[RawFileManifest] = []
        for (trade_date, exchange, symbol), records in list(self._buffers.items()):
            if not records:
                continue
            self._file_seq += 1
            frame = [record.model_dump() for record in records]
            file_path = write_partition_frame(
                frame,
                base_dir=self.raw_root,
                trade_date=trade_date,
                exchange=exchange,
                symbol=symbol,
                file_stem=f"{run_id}_{self._file_seq:04d}",
            )
            manifest = make_raw_file_manifest(
                project_root=self.project_root,
                run_id=run_id,
                trade_date=trade_date,
                instrument_key=records[0].instrument_key,
                symbol=symbol,
                exchange=exchange,
                gateway_name=records[0].gateway_name,
                row_count=len(records),
                file_path=file_path,
            )
            self.manifest_store.upsert_raw_file_manifest(manifest)
            manifests.append(manifest)
        self._buffers.clear()
        return manifests
