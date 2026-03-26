"""Partitioned parquet helpers for raw and standardized market data."""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

from libs.common.time import ensure_cn_aware


def require_parquet_support() -> Any:
    try:
        import pandas as pd  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - dependency managed in runtime
        raise RuntimeError("pandas is required for M4 market data tasks") from exc
    try:
        import pyarrow  # type: ignore[import-untyped]  # noqa: F401
    except ImportError as exc:  # pragma: no cover - dependency managed in runtime
        raise RuntimeError("pyarrow is required for parquet market data tasks") from exc
    return pd


def stable_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def resolve_partition_dir(base_dir: Path, trade_date: date, exchange: str, symbol: str) -> Path:
    return (
        base_dir
        / f"trade_date={trade_date.isoformat()}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
    )


def clear_partition_dir(base_dir: Path, trade_date: date, exchange: str, symbol: str) -> None:
    target_dir = resolve_partition_dir(base_dir, trade_date, exchange, symbol)
    if target_dir.exists():
        shutil.rmtree(target_dir)


def clear_symbol_partitions(base_dir: Path, *, exchange: str, symbol: str) -> None:
    if not base_dir.exists():
        return
    pattern = f"trade_date=*/exchange={exchange}/symbol={symbol}"
    for target_dir in sorted(base_dir.glob(pattern)):
        if target_dir.is_dir():
            shutil.rmtree(target_dir)


def relative_path(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def write_partition_frame(
    frame: object,
    *,
    base_dir: Path,
    trade_date: date,
    exchange: str,
    symbol: str,
    file_stem: str,
) -> Path:
    pd = require_parquet_support()
    target_dir = resolve_partition_dir(base_dir, trade_date, exchange, symbol)
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{file_stem}.parquet"
    normalized = pd.DataFrame(frame).copy()
    normalized.to_parquet(target_path, index=False)
    return target_path


def list_partition_files(
    base_dir: Path,
    *,
    trade_date: date | None = None,
    symbol: str | None = None,
    exchange: str | None = None,
) -> list[Path]:
    if not base_dir.exists():
        return []

    if trade_date and exchange and symbol:
        pattern = (
            f"trade_date={trade_date.isoformat()}/exchange={exchange}/symbol={symbol}/*.parquet"
        )
    else:
        trade_pattern = (
            f"trade_date={trade_date.isoformat()}" if trade_date else "trade_date=*"
        )
        exchange_pattern = f"exchange={exchange}" if exchange else "exchange=*"
        symbol_pattern = f"symbol={symbol}" if symbol else "symbol=*"
        pattern = f"{trade_pattern}/{exchange_pattern}/{symbol_pattern}/*.parquet"

    return [path for path in sorted(base_dir.glob(pattern)) if path.is_file()]


def read_partitioned_frame(
    base_dir: Path,
    *,
    trade_date: date | None = None,
    symbol: str | None = None,
    exchange: str | None = None,
) -> Any:
    pd = require_parquet_support()
    files = list_partition_files(base_dir, trade_date=trade_date, symbol=symbol, exchange=exchange)
    if not files:
        return pd.DataFrame()
    frames = [pd.read_parquet(path) for path in files]
    return pd.concat(frames, ignore_index=True)


def ensure_datetime_columns(frame: Any, columns: Iterable[str]) -> Any:
    for column in columns:
        if column not in frame.columns:
            continue
        frame[column] = frame[column].map(ensure_cn_aware)
    return frame
