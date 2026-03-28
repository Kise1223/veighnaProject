"""Canonical contracts for M13 benchmark reference artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class BenchmarkRunStatus(StrEnum):
    CREATED = "created"
    SUCCESS = "success"
    FAILED = "failed"


class BenchmarkSourceType(StrEnum):
    CUSTOM_WEIGHTS = "custom_weights"
    EQUAL_WEIGHT_TARGET_UNIVERSE = "equal_weight_target_universe"
    EQUAL_WEIGHT_UNION = "equal_weight_union"


class BenchmarkReferenceConfig(BaseModel):
    normalize_mode: str = Field(default="scale_if_overweight", min_length=1)
    allow_cash_weight: bool = True

    model_config = ConfigDict(extra="forbid")


class BenchmarkReferenceRunRecord(BaseModel):
    benchmark_run_id: str = Field(min_length=1)
    trade_date: date
    benchmark_name: str = Field(min_length=1)
    benchmark_source_type: BenchmarkSourceType
    source_portfolio_analytics_run_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    benchmark_config_hash: str = Field(min_length=1)
    status: BenchmarkRunStatus
    created_at: datetime
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class BenchmarkReferenceManifest(BaseModel):
    benchmark_run_id: str = Field(min_length=1)
    trade_date: date
    benchmark_name: str = Field(min_length=1)
    benchmark_source_type: BenchmarkSourceType
    source_portfolio_analytics_run_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    benchmark_config_hash: str = Field(min_length=1)
    status: BenchmarkRunStatus
    created_at: datetime
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    weights_file_path: str | None = None
    weights_file_hash: str | None = None
    weight_row_count: int = Field(default=0, ge=0)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    error_message: str | None = None
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")


class BenchmarkWeightRowRecord(BaseModel):
    benchmark_run_id: str = Field(min_length=1)
    instrument_key: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    exchange: str = Field(min_length=1)
    benchmark_weight: Decimal = Field(ge=Decimal("0"))
    benchmark_rank: int = Field(ge=1)
    group_key_optional: str | None = None
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class BenchmarkSummaryRecord(BaseModel):
    benchmark_run_id: str = Field(min_length=1)
    holdings_count: int = Field(ge=0)
    benchmark_cash_weight: Decimal = Field(ge=Decimal("0"))
    top1_concentration: Decimal = Field(ge=Decimal("0"))
    top3_concentration: Decimal = Field(ge=Decimal("0"))
    top5_concentration: Decimal = Field(ge=Decimal("0"))
    hhi_concentration: Decimal = Field(ge=Decimal("0"))
    summary_json: dict[str, JsonScalar | Sequence[JsonScalar]] = Field(default_factory=dict)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class BenchmarkReferenceLineage(BaseModel):
    benchmark_run_id: str = Field(min_length=1)
    source_portfolio_analytics_run_id: str = Field(min_length=1)
    source_strategy_run_id: str = Field(min_length=1)
    source_prediction_run_id: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None
    run_file_path: str = Field(min_length=1)
    run_file_hash: str = Field(min_length=1)
    summary_file_path: str | None = None
    summary_file_hash: str | None = None
    status: BenchmarkRunStatus

    model_config = ConfigDict(extra="forbid")
