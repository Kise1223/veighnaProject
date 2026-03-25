"""Deterministic baseline model used by M5."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import numpy as np

from libs.research.schemas import BaselineModelConfig


@dataclass(frozen=True)
class BaselineLinearModel:
    model_name: str
    model_version: str
    feature_names: list[str]
    regularization: float
    feature_mean: list[float]
    feature_scale: list[float]
    coefficients: list[float]
    intercept: float

    def predict(self, frame: Any) -> Any:
        matrix = np.asarray(frame[self.feature_names], dtype=float)
        means = np.asarray(self.feature_mean, dtype=float)
        scales = np.asarray(self.feature_scale, dtype=float)
        scales = np.where(scales == 0, 1.0, scales)
        normalized = (matrix - means) / scales
        coefficients = np.asarray(self.coefficients, dtype=float)
        return self.intercept + normalized @ coefficients

    def to_payload(self) -> dict[str, object]:
        return {
            "model_name": self.model_name,
            "model_version": self.model_version,
            "feature_names": self.feature_names,
            "regularization": self.regularization,
            "feature_mean": self.feature_mean,
            "feature_scale": self.feature_scale,
            "coefficients": self.coefficients,
            "intercept": self.intercept,
        }

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> BaselineLinearModel:
        feature_names = [str(item) for item in _as_iterable(payload["feature_names"])]
        feature_mean = [_as_float(item) for item in _as_iterable(payload["feature_mean"])]
        feature_scale = [_as_float(item) for item in _as_iterable(payload["feature_scale"])]
        coefficients = [_as_float(item) for item in _as_iterable(payload["coefficients"])]
        return cls(
            model_name=str(payload["model_name"]),
            model_version=str(payload["model_version"]),
            feature_names=feature_names,
            regularization=_as_float(payload["regularization"]),
            feature_mean=feature_mean,
            feature_scale=feature_scale,
            coefficients=coefficients,
            intercept=_as_float(payload["intercept"]),
        )


def fit_baseline_model(
    *,
    train_frame: Any,
    feature_names: list[str],
    label_name: str,
    model_config: BaselineModelConfig,
) -> tuple[BaselineLinearModel, dict[str, float | int]]:
    matrix = np.asarray(train_frame[feature_names], dtype=float)
    label = np.asarray(train_frame[label_name], dtype=float)
    means = matrix.mean(axis=0)
    scales = matrix.std(axis=0)
    scales = np.where(scales == 0, 1.0, scales)
    normalized = (matrix - means) / scales
    centered_label = label - float(label.mean())
    regularized = normalized.T @ normalized + model_config.regularization * np.eye(normalized.shape[1])
    coefficients = np.linalg.pinv(regularized) @ normalized.T @ centered_label
    intercept = float(label.mean())
    model = BaselineLinearModel(
        model_name=model_config.model_name,
        model_version=model_config.model_version,
        feature_names=feature_names,
        regularization=model_config.regularization,
        feature_mean=means.astype(float).tolist(),
        feature_scale=scales.astype(float).tolist(),
        coefficients=coefficients.astype(float).tolist(),
        intercept=intercept,
    )
    prediction = model.predict(train_frame)
    residual = prediction - label
    metrics: dict[str, float | int] = {
        "train_rows": int(len(train_frame)),
        "feature_count": int(len(feature_names)),
        "train_rmse": float(np.sqrt(np.mean(np.square(residual)))),
        "train_mae": float(np.mean(np.abs(residual))),
        "train_score_mean": float(prediction.mean()),
    }
    return model, metrics


def _as_iterable(value: object) -> Iterable[object]:
    if isinstance(value, list):
        return value
    raise TypeError(f"expected list payload, got {type(value).__name__}")


def _as_float(value: object) -> float:
    return float(str(value))
