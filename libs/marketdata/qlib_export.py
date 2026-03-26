"""Export standardized parquet data into a minimal qlib file provider."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np

from libs.common.time import ensure_cn_aware
from libs.marketdata.manifest_store import ManifestStore
from libs.marketdata.manifests import make_standard_file_manifest
from libs.marketdata.raw_store import read_partitioned_frame, require_parquet_support
from libs.marketdata.symbol_mapping import InstrumentCatalog


def export_qlib_provider(
    *,
    project_root: Path,
    provider_root: Path,
    catalog: InstrumentCatalog,
    manifest_store: ManifestStore,
    freq: str,
    build_run_id: str,
    source_build_run_ids: list[str] | None = None,
 ) -> dict[str, object]:
    pd = require_parquet_support()
    qlib_freq = _normalize_freq(freq)
    layer = "bars_1d" if qlib_freq == "day" else "bars_1m"
    frame = read_partitioned_frame(project_root / "data" / "standard" / layer)
    if frame.empty:
        raise ValueError(f"no standardized data found in layer {layer}")

    factor_frame = read_partitioned_frame(project_root / "data" / "standard" / "adjustment_factors")
    provider_root.mkdir(parents=True, exist_ok=True)
    (provider_root / "calendars").mkdir(exist_ok=True)
    (provider_root / "instruments").mkdir(exist_ok=True)
    (provider_root / "features").mkdir(exist_ok=True)

    frame = frame.copy()
    factor_frame = factor_frame.copy()
    if source_build_run_ids is not None and "build_run_id" in frame.columns:
        frame = frame[frame["build_run_id"].isin(source_build_run_ids)].copy()
    if source_build_run_ids is not None and "source_run_id" in factor_frame.columns:
        factor_frame = factor_frame[factor_frame["source_run_id"].isin(source_build_run_ids)].copy()
    if frame.empty:
        raise ValueError(f"no standardized data found for source_build_run_ids={source_build_run_ids}")
    datetime_column = "trade_date" if qlib_freq == "day" else "bar_dt"
    frame[datetime_column] = frame[datetime_column].map(lambda value: _format_calendar_value(value, qlib_freq))
    if not factor_frame.empty and "trade_date" in factor_frame.columns:
        factor_frame["trade_date"] = factor_frame["trade_date"].map(lambda value: _format_calendar_value(value, "day"))
    calendar_values = sorted(pd.Index(frame[datetime_column]).unique().tolist())
    _write_calendar(provider_root, qlib_freq, calendar_values)
    _write_instruments(provider_root, frame, catalog, datetime_column)
    _write_features(provider_root, frame, factor_frame, catalog, qlib_freq, datetime_column)

    standard_build_run_ids = sorted({str(value) for value in frame["build_run_id"].dropna().unique()})
    manifest_payload = {
        "freq": qlib_freq,
        "rows": len(frame),
        "calendar_size": len(calendar_values),
        "build_run_id": build_run_id,
        "source_standard_build_run_id": standard_build_run_ids[0] if len(standard_build_run_ids) == 1 else None,
        "source_standard_build_run_ids": standard_build_run_ids,
        "created_at": ensure_cn_aware(datetime.now()).isoformat(),
    }
    marker_path = provider_root / "export_manifest.json"
    marker_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    freq_marker_path = provider_root / f"export_manifest_{qlib_freq}.json"
    freq_marker_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")
    manifest_store.upsert_standard_file_manifest(
        make_standard_file_manifest(
            project_root=project_root,
            build_run_id=build_run_id,
            layer=f"qlib_provider_{qlib_freq}",
            row_count=len(frame),
            file_path=freq_marker_path,
        )
    )
    return {
        "freq": qlib_freq,
        "rows": len(frame),
        "calendar_size": len(calendar_values),
        "source_standard_build_run_ids": standard_build_run_ids,
        "source_standard_build_run_id": manifest_payload["source_standard_build_run_id"],
    }


def qlib_smoke_read(*, provider_root: Path, qlib_symbol: str, freq: str) -> object:
    import qlib  # type: ignore[import-untyped]
    from qlib.data import D  # type: ignore[import-untyped]

    qlib_freq = _normalize_freq(freq)
    start_time, end_time = _smoke_window(provider_root, qlib_freq)
    qlib.init(provider_uri=str(provider_root), region="cn", expression_cache=None, dataset_cache=None)
    fields = ["$open", "$close"]
    data = D.features(
        instruments=[qlib_symbol],
        fields=fields,
        start_time=start_time,
        end_time=end_time,
        freq=qlib_freq,
    )
    return data


def _normalize_freq(freq: str) -> str:
    mapping = {"1d": "day", "day": "day", "1min": "1min"}
    if freq not in mapping:
        raise ValueError(f"unsupported qlib export freq: {freq}")
    return mapping[freq]


def _write_calendar(provider_root: Path, qlib_freq: str, calendar_values: list[str]) -> None:
    calendar_path = provider_root / "calendars" / f"{qlib_freq}.txt"
    calendar_path.write_text("\n".join(calendar_values) + ("\n" if calendar_values else ""), encoding="utf-8")


def _write_instruments(provider_root: Path, frame, catalog: InstrumentCatalog, datetime_column: str) -> None:  # type: ignore[no-untyped-def]
    lines: list[str] = []
    for instrument_key, subset in frame.groupby("instrument_key"):
        qlib_symbol = catalog.to_qlib_symbol(instrument_key)
        start_time = str(subset[datetime_column].min())
        end_time = str(subset[datetime_column].max())
        lines.append(f"{qlib_symbol}\t{start_time}\t{end_time}")
    instrument_path = provider_root / "instruments" / "all.txt"
    instrument_path.write_text("\n".join(sorted(lines)) + ("\n" if lines else ""), encoding="utf-8")


def _write_features(
    provider_root: Path,
    frame: Any,
    factor_frame: Any,
    catalog: InstrumentCatalog,
    qlib_freq: str,
    datetime_column: str,
 ) -> None:
    pd = require_parquet_support()
    calendar_index = pd.Index(sorted(pd.Index(frame[datetime_column]).astype(str).unique().tolist()))
    factor_lookup = {}
    if not factor_frame.empty:
        factor_lookup = {
            (row["instrument_key"], str(row["trade_date"])): float(row["adj_factor"])
            for row in factor_frame.to_dict(orient="records")
        }

    for instrument_key, subset in frame.groupby("instrument_key"):
        qlib_symbol = catalog.to_qlib_symbol(instrument_key)
        instrument_dir = provider_root / "features" / qlib_symbol.lower()
        instrument_dir.mkdir(parents=True, exist_ok=True)
        indexed = subset.copy()
        indexed["_calendar_key"] = indexed[datetime_column].astype(str)
        sort_columns = [datetime_column]
        if "build_run_id" in indexed.columns:
            sort_columns.append("build_run_id")
        indexed = indexed.sort_values(sort_columns).drop_duplicates(subset="_calendar_key", keep="last")
        indexed = indexed.set_index("_calendar_key").reindex(calendar_index)
        factor_values = []
        for calendar_value in calendar_index:
            lookup_date = calendar_value[:10]
            factor_values.append(factor_lookup.get((instrument_key, lookup_date), 1.0))
        field_map = {
            "open": indexed["open"].to_numpy(dtype=np.float32),
            "high": indexed["high"].to_numpy(dtype=np.float32),
            "low": indexed["low"].to_numpy(dtype=np.float32),
            "close": indexed["close"].to_numpy(dtype=np.float32),
            "volume": indexed["volume"].to_numpy(dtype=np.float32),
            "amount": indexed["turnover"].to_numpy(dtype=np.float32),
            "vwap": indexed["vwap"].to_numpy(dtype=np.float32),
            "factor": np.asarray(factor_values, dtype=np.float32),
        }
        for field, values in field_map.items():
            _write_feature_bin(instrument_dir / f"{field}.{qlib_freq}.bin", values)


def _write_feature_bin(path: Path, values: np.ndarray) -> None:
    normalized = np.nan_to_num(values.astype(np.float32), nan=np.nan)
    with path.open("wb") as handle:
        np.hstack([np.array([0], dtype=np.float32), normalized]).astype("<f4").tofile(handle)


def _format_calendar_value(value: object, freq: str) -> str:
    if freq == "day":
        return str(value)[:10]
    pd = require_parquet_support()
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("Asia/Shanghai").tz_localize(None)
    return str(timestamp.strftime("%Y-%m-%d %H:%M:%S"))


def _smoke_window(provider_root: Path, qlib_freq: str) -> tuple[str, str]:
    calendar_path = provider_root / "calendars" / f"{qlib_freq}.txt"
    if not calendar_path.exists():
        raise FileNotFoundError(f"qlib calendar not found: {calendar_path}")
    values = [line.strip() for line in calendar_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not values:
        raise ValueError(f"qlib calendar is empty: {calendar_path}")
    return values[0], values[-1]
