"""Baseline dataset materialization on top of the M4 qlib provider."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from libs.marketdata.raw_store import require_parquet_support
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.research.schemas import BaselineDatasetConfig


@dataclass(frozen=True)
class BaselineDatasetBundle:
    feature_frame: Any
    train_frame: Any
    inference_frame: Any
    feature_names: list[str]
    label_name: str
    calendar_start: date
    calendar_end: date


def provider_symbols(provider_root: Path) -> list[str]:
    instrument_path = provider_root / "instruments" / "all.txt"
    if not instrument_path.exists():
        raise FileNotFoundError(f"qlib instruments file not found: {instrument_path}")
    symbols = []
    for line in instrument_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        symbols.append(line.split("\t", 1)[0])
    return sorted(set(symbols))


def build_baseline_dataset(
    *,
    provider_root: Path,
    catalog: InstrumentCatalog,
    dataset_config: BaselineDatasetConfig,
) -> BaselineDatasetBundle:
    pd = require_parquet_support()
    from qlib.data import D  # type: ignore[import-untyped]

    qlib_symbols = _resolve_qlib_symbols(provider_root, catalog, dataset_config)
    feature_names = [feature.name for feature in dataset_config.features]
    expressions = [feature.expression for feature in dataset_config.features] + [dataset_config.label.expression]
    raw_frame = D.features(
        qlib_symbols,
        expressions,
        start_time=dataset_config.train_start.isoformat(),
        end_time=dataset_config.infer_trade_date.isoformat(),
        freq=dataset_config.freq,
    )
    if raw_frame.empty:
        raise ValueError("qlib provider returned no rows for the configured baseline dataset")

    label_name = dataset_config.label.name
    frame = raw_frame.copy()
    frame.columns = feature_names + [label_name]
    frame = frame.reset_index().rename(columns={"instrument": "qlib_symbol", "datetime": "trade_date"})
    frame["trade_date"] = frame["trade_date"].map(lambda value: pd.Timestamp(value).date())
    frame["instrument_key"] = frame["qlib_symbol"].map(
        lambda value: catalog.from_qlib_symbol(str(value)).mapping.instrument_key
    )
    calendar_start = frame["trade_date"].min()
    calendar_end = frame["trade_date"].max()
    train_frame = frame[
        (frame["trade_date"] >= dataset_config.train_start)
        & (frame["trade_date"] <= dataset_config.train_end)
    ].dropna(subset=feature_names + [label_name])
    inference_frame = frame[frame["trade_date"] == dataset_config.infer_trade_date].dropna(
        subset=feature_names
    )
    if train_frame["instrument_key"].nunique() < dataset_config.min_instruments:
        raise ValueError(
            "baseline dataset has too few instruments for training; run scripts.build_research_sample first"
        )
    if len(train_frame) < dataset_config.min_train_rows:
        raise ValueError(
            "baseline dataset has too few rows for training; run scripts.build_research_sample first"
        )
    return BaselineDatasetBundle(
        feature_frame=frame,
        train_frame=train_frame,
        inference_frame=inference_frame,
        feature_names=feature_names,
        label_name=label_name,
        calendar_start=calendar_start,
        calendar_end=calendar_end,
    )


def build_inference_frame(
    *,
    provider_root: Path,
    catalog: InstrumentCatalog,
    dataset_config: BaselineDatasetConfig,
    trade_date: date,
) -> Any:
    pd = require_parquet_support()
    from qlib.data import D  # type: ignore[import-untyped]

    qlib_symbols = _resolve_qlib_symbols(provider_root, catalog, dataset_config)
    feature_names = [feature.name for feature in dataset_config.features]
    expressions = [feature.expression for feature in dataset_config.features]
    raw_frame = D.features(
        qlib_symbols,
        expressions,
        start_time=trade_date.isoformat(),
        end_time=trade_date.isoformat(),
        freq=dataset_config.freq,
    )
    if raw_frame.empty:
        raise ValueError(f"no inference rows found for trade_date={trade_date.isoformat()}")
    frame = raw_frame.copy()
    frame.columns = feature_names
    frame = frame.reset_index().rename(columns={"instrument": "qlib_symbol", "datetime": "trade_date"})
    frame["trade_date"] = frame["trade_date"].map(lambda value: pd.Timestamp(value).date())
    frame["instrument_key"] = frame["qlib_symbol"].map(
        lambda value: catalog.from_qlib_symbol(str(value)).mapping.instrument_key
    )
    return frame.dropna(subset=feature_names)


def _resolve_qlib_symbols(
    provider_root: Path,
    catalog: InstrumentCatalog,
    dataset_config: BaselineDatasetConfig,
) -> list[str]:
    available = set(provider_symbols(provider_root))
    if dataset_config.instruments == "all":
        return sorted(available)
    resolved = []
    for item in dataset_config.instruments:
        if item in available:
            resolved.append(item)
            continue
        resolved.append(catalog.to_qlib_symbol(item))
    return sorted(set(resolved).intersection(available))
