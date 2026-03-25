"""Canonical contracts for M5 research workflows."""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

JsonScalar = str | int | float | bool | None


class ResearchRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class FeatureSpec(BaseModel):
    name: str = Field(min_length=1)
    expression: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class LabelSpec(BaseModel):
    name: str = Field(min_length=1)
    expression: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class ResearchRuntimeConfig(BaseModel):
    provider_uri: str = Field(default="data/qlib_bin", min_length=1)
    region: str = Field(default="cn", min_length=1)
    experiment_name: str = Field(default="baseline_linear_v1", min_length=1)
    artifacts_root: str = Field(default="data/research", min_length=1)
    recorder_uri: str = Field(default="data/research/mlruns", min_length=1)
    qlib_export_manifest_path: str = Field(
        default="data/qlib_bin/export_manifest_day.json",
        min_length=1,
    )

    model_config = ConfigDict(extra="forbid")


class BaselineDatasetConfig(BaseModel):
    dataset_name: str = Field(min_length=1)
    feature_set_name: str = Field(min_length=1)
    feature_set_version: str = Field(min_length=1)
    freq: str = Field(default="day", min_length=1)
    instruments: list[str] | str = Field(default="all")
    train_start: date
    train_end: date
    infer_trade_date: date
    min_train_rows: int = Field(default=20, ge=1)
    min_instruments: int = Field(default=2, ge=1)
    features: list[FeatureSpec]
    label: LabelSpec

    model_config = ConfigDict(extra="forbid")


class BaselineModelConfig(BaseModel):
    model_name: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    regularization: float = Field(default=1.0, ge=0.0)

    model_config = ConfigDict(extra="forbid")


class ArtifactFile(BaseModel):
    name: str = Field(min_length=1)
    relative_path: str = Field(min_length=1)
    file_hash: str = Field(min_length=1)

    model_config = ConfigDict(extra="forbid")


class ModelRunRecord(BaseModel):
    run_id: str = Field(min_length=1)
    experiment_name: str = Field(min_length=1)
    model_name: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    feature_set_name: str = Field(min_length=1)
    feature_set_version: str = Field(min_length=1)
    provider_uri: str = Field(min_length=1)
    calendar_start: date
    calendar_end: date
    train_start: date
    train_end: date
    infer_trade_date: date
    status: ResearchRunStatus
    artifact_path: str = Field(min_length=1)
    metrics_json: dict[str, JsonScalar] = Field(default_factory=dict)
    source_standard_build_run_id: str | None = None
    source_qlib_export_run_id: str | None = None
    created_at: datetime
    artifact_hash: str = Field(min_length=1)
    config_hash: str = Field(min_length=1)
    recorder_id: str | None = None
    recorder_uri: str | None = None

    model_config = ConfigDict(extra="forbid")


class PredictionRecord(BaseModel):
    trade_date: date
    instrument_key: str = Field(min_length=1)
    qlib_symbol: str = Field(min_length=1)
    score: float
    run_id: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    feature_set_version: str = Field(min_length=1)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class PredictionManifest(BaseModel):
    trade_date: date
    run_id: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    feature_set_version: str = Field(min_length=1)
    row_count: int = Field(ge=0)
    file_path: str = Field(min_length=1)
    file_hash: str = Field(min_length=1)
    created_at: datetime

    model_config = ConfigDict(extra="forbid")


class QlibExportLineage(BaseModel):
    freq: str = Field(min_length=1)
    build_run_id: str = Field(min_length=1)
    rows: int | None = None
    calendar_size: int | None = None
    source_standard_build_run_id: str | None = None
    source_standard_build_run_ids: list[str] = Field(default_factory=list)
    created_at: datetime | None = None

    model_config = ConfigDict(extra="forbid")


class PredictionLineage(BaseModel):
    trade_date: date
    run_id: str = Field(min_length=1)
    prediction_path: str = Field(min_length=1)
    prediction_file_hash: str = Field(min_length=1)
    model_name: str = Field(min_length=1)
    model_version: str = Field(min_length=1)
    feature_set_name: str = Field(min_length=1)
    feature_set_version: str = Field(min_length=1)
    source_qlib_export_run_id: str | None = None
    source_standard_build_run_id: str | None = None

    model_config = ConfigDict(extra="forbid")
