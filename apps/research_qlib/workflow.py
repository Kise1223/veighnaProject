"""End-to-end M5 training, inference, and consistency workflow."""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

from apps.research_qlib.bootstrap import (
    init_qlib,
    load_dataset_config,
    load_model_config,
    load_runtime_config,
    recorder_uri,
)
from apps.research_qlib.dataset import (
    build_baseline_dataset,
    build_inference_frame,
    provider_symbols,
)
from apps.research_qlib.modeling import BaselineLinearModel, fit_baseline_model
from libs.common.time import ensure_cn_aware
from libs.marketdata.raw_store import stable_hash
from libs.marketdata.symbol_mapping import InstrumentCatalog
from libs.research.artifacts import ResearchArtifactStore
from libs.research.lineage import load_qlib_export_lineage
from libs.research.schemas import ModelRunRecord, PredictionRecord, ResearchRunStatus

DEFAULT_BASE_CONFIG = Path("configs/qlib/base.yaml")
DEFAULT_DATASET_CONFIG = Path("configs/qlib/dataset_baseline.yaml")
DEFAULT_MODEL_CONFIG = Path("configs/qlib/model_baseline.yaml")


def train_baseline_workflow(
    *,
    project_root: Path,
    base_config_path: Path = DEFAULT_BASE_CONFIG,
    dataset_config_path: Path = DEFAULT_DATASET_CONFIG,
    model_config_path: Path = DEFAULT_MODEL_CONFIG,
    force: bool = False,
) -> dict[str, object]:
    runtime_config = load_runtime_config(project_root / base_config_path)
    dataset_config = load_dataset_config(project_root / dataset_config_path)
    model_config = load_model_config(project_root / model_config_path)
    catalog = InstrumentCatalog.from_bootstrap_dir(project_root / "data" / "master" / "bootstrap")
    store = ResearchArtifactStore(project_root, project_root / runtime_config.artifacts_root)
    lineage = load_qlib_export_lineage(project_root / runtime_config.provider_uri, freq=dataset_config.freq)
    if lineage.source_standard_build_run_id is None:
        raise ValueError(
            "qlib export lineage is ambiguous; rebuild the provider from one standard build or run scripts.build_research_sample"
        )

    config_hash = stable_hash(
        {
            "experiment_name": runtime_config.experiment_name,
            "provider_uri": runtime_config.provider_uri,
            "dataset": dataset_config.model_dump(mode="json"),
            "model": model_config.model_dump(mode="json"),
            "source_qlib_export_run_id": lineage.build_run_id,
            "source_standard_build_run_id": lineage.source_standard_build_run_id,
        }
    )
    run_id = f"model_{config_hash[:12]}"
    if store.has_run(run_id):
        existing = store.load_run(run_id)
        if existing.status == ResearchRunStatus.SUCCESS and not force:
            return {
                "run_id": existing.run_id,
                "status": existing.status.value,
                "artifact_path": existing.artifact_path,
                "reused": True,
            }
        if existing.status != ResearchRunStatus.SUCCESS or force:
            store.clear_run(run_id)

    created_at = ensure_cn_aware(datetime.now())
    run = ModelRunRecord(
        run_id=run_id,
        experiment_name=runtime_config.experiment_name,
        model_name=model_config.model_name,
        model_version=model_config.model_version,
        feature_set_name=dataset_config.feature_set_name,
        feature_set_version=dataset_config.feature_set_version,
        provider_uri=runtime_config.provider_uri,
        calendar_start=dataset_config.train_start,
        calendar_end=dataset_config.infer_trade_date,
        train_start=dataset_config.train_start,
        train_end=dataset_config.train_end,
        infer_trade_date=dataset_config.infer_trade_date,
        status=ResearchRunStatus.RUNNING,
        artifact_path=store.run_dir(run_id).relative_to(project_root).as_posix(),
        metrics_json={},
        source_standard_build_run_id=lineage.source_standard_build_run_id,
        source_qlib_export_run_id=lineage.build_run_id,
        created_at=created_at,
        artifact_hash="pending",
        config_hash=config_hash,
        recorder_uri=runtime_config.recorder_uri,
    )
    store.save_run(run)
    try:
        init_qlib(project_root, runtime_config)
        bundle = build_baseline_dataset(
            provider_root=project_root / runtime_config.provider_uri,
            catalog=catalog,
            dataset_config=dataset_config,
        )
        model, metrics = fit_baseline_model(
            train_frame=bundle.train_frame,
            feature_names=bundle.feature_names,
            label_name=bundle.label_name,
            model_config=model_config,
        )
        artifact_files = [
            store.write_json_artifact(run_id, "base_config.json", runtime_config.model_dump(mode="json")),
            store.write_json_artifact(run_id, "dataset_config.json", dataset_config.model_dump(mode="json")),
            store.write_json_artifact(run_id, "model_config.json", model_config.model_dump(mode="json")),
            store.write_json_artifact(
                run_id,
                "lineage.json",
                {
                    "source_qlib_export_run_id": lineage.build_run_id,
                    "source_standard_build_run_id": lineage.source_standard_build_run_id,
                    "source_standard_build_run_ids": lineage.source_standard_build_run_ids,
                },
            ),
            store.write_json_artifact(
                run_id,
                "dataset_summary.json",
                {
                    "calendar_start": bundle.calendar_start.isoformat(),
                    "calendar_end": bundle.calendar_end.isoformat(),
                    "train_rows": len(bundle.train_frame),
                    "inference_rows": len(bundle.inference_frame),
                    "qlib_symbols": sorted(bundle.feature_frame["qlib_symbol"].unique().tolist()),
                },
            ),
            store.write_json_artifact(run_id, "model.json", model.to_payload()),
            store.write_parquet_artifact(
                run_id,
                "train_preview.parquet",
                bundle.train_frame.head(20).to_dict(orient="records"),
            ),
        ]
        recorder_id = _log_with_qlib_recorder(
            runtime_experiment_name=runtime_config.experiment_name,
            runtime_recorder_uri=recorder_uri(project_root, runtime_config),
            run=run,
            metrics=metrics,
            artifact_paths=[project_root / item.relative_path for item in artifact_files],
        )
        run = run.model_copy(
            update={
                "status": ResearchRunStatus.SUCCESS,
                "calendar_start": bundle.calendar_start,
                "calendar_end": bundle.calendar_end,
                "metrics_json": metrics,
                "artifact_hash": store.finalize_artifact_hash(artifact_files),
                "recorder_id": recorder_id,
            }
        )
        store.save_run(run)
        return {
            "run_id": run.run_id,
            "status": run.status.value,
            "artifact_path": run.artifact_path,
            "metrics": metrics,
            "reused": False,
        }
    except Exception as exc:
        failed = run.model_copy(
            update={
                "status": ResearchRunStatus.FAILED,
                "metrics_json": {"error": str(exc)},
                "artifact_hash": stable_hash({"run_id": run_id, "status": "failed"}),
            }
        )
        store.save_run(failed)
        raise


def run_daily_inference(
    *,
    project_root: Path,
    trade_date: date,
    run_id: str | None = None,
    base_config_path: Path = DEFAULT_BASE_CONFIG,
) -> dict[str, object]:
    runtime_config = load_runtime_config(project_root / base_config_path)
    store = ResearchArtifactStore(project_root, project_root / runtime_config.artifacts_root)
    run = _resolve_run(store, run_id)
    if run.status != ResearchRunStatus.SUCCESS:
        raise ValueError(f"run {run.run_id} is not successful and cannot be used for inference")
    if store.has_prediction(trade_date, run.run_id):
        manifest = store.load_prediction_manifest(trade_date, run.run_id)
        return {
            "run_id": run.run_id,
            "trade_date": trade_date.isoformat(),
            "row_count": manifest.row_count,
            "file_path": manifest.file_path,
            "reused": True,
        }

    catalog = InstrumentCatalog.from_bootstrap_dir(project_root / "data" / "master" / "bootstrap")
    dataset_config = load_dataset_config(store.run_dir(run.run_id) / "dataset_config.json")
    init_qlib(project_root, runtime_config)
    inference_frame = build_inference_frame(
        provider_root=project_root / runtime_config.provider_uri,
        catalog=catalog,
        dataset_config=dataset_config,
        trade_date=trade_date,
    )
    model_payload = json.loads((store.run_dir(run.run_id) / "model.json").read_text(encoding="utf-8"))
    model = BaselineLinearModel.from_payload(model_payload)
    scores = model.predict(inference_frame)
    created_at = ensure_cn_aware(datetime.now())
    records = [
        PredictionRecord(
            trade_date=trade_date,
            instrument_key=str(row.instrument_key),
            qlib_symbol=str(row.qlib_symbol),
            score=float(score),
            run_id=run.run_id,
            model_version=run.model_version,
            feature_set_version=run.feature_set_version,
            created_at=created_at,
        )
        for row, score in zip(inference_frame.itertuples(index=False), scores, strict=True)
    ]
    manifest = store.save_predictions(
        trade_date=trade_date,
        run_id=run.run_id,
        model_version=run.model_version,
        feature_set_version=run.feature_set_version,
        records=records,
    )
    return {
        "run_id": run.run_id,
        "trade_date": trade_date.isoformat(),
        "row_count": manifest.row_count,
        "file_path": manifest.file_path,
        "reused": False,
    }


def check_symbol_and_calendar_consistency(
    *,
    project_root: Path,
    base_config_path: Path = DEFAULT_BASE_CONFIG,
) -> dict[str, object]:
    runtime_config = load_runtime_config(project_root / base_config_path)
    provider_root = project_root / runtime_config.provider_uri
    catalog = InstrumentCatalog.from_bootstrap_dir(project_root / "data" / "master" / "bootstrap")
    issues: list[str] = []
    qlib_symbols = provider_symbols(provider_root)
    if not qlib_symbols:
        issues.append("qlib instruments/all.txt is empty")
    for qlib_symbol in qlib_symbols:
        try:
            catalog.from_qlib_symbol(qlib_symbol)
        except KeyError:
            issues.append(f"missing instrument_key mapping for qlib_symbol={qlib_symbol}")
    day_calendar = _load_calendar(provider_root / "calendars" / "day.txt")
    minute_calendar = _load_calendar(provider_root / "calendars" / "1min.txt")
    if not day_calendar:
        issues.append("day calendar is empty")
    if not minute_calendar:
        issues.append("1min calendar is empty")
    minute_trade_dates = {item[:10] for item in minute_calendar}
    missing_from_minute = sorted(set(day_calendar) - minute_trade_dates)
    if missing_from_minute:
        issues.append(f"1min calendar missing trade dates: {', '.join(missing_from_minute[:5])}")
    export_lineage = load_qlib_export_lineage(provider_root, freq="day")
    if export_lineage.source_standard_build_run_id is None:
        issues.append("day export manifest does not contain a single source_standard_build_run_id")
    return {
        "status": "passed" if not issues else "failed",
        "instrument_count": len(qlib_symbols),
        "day_calendar_size": len(day_calendar),
        "minute_calendar_size": len(minute_calendar),
        "source_qlib_export_run_id": export_lineage.build_run_id,
        "source_standard_build_run_id": export_lineage.source_standard_build_run_id,
        "issues": issues,
    }


def _load_calendar(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _resolve_run(store: ResearchArtifactStore, run_id: str | None) -> ModelRunRecord:
    if run_id is not None:
        return store.load_run(run_id)
    for run in store.list_runs():
        if run.status == ResearchRunStatus.SUCCESS:
            return run
    raise FileNotFoundError("no successful model_run artifact found")


def _log_with_qlib_recorder(
    *,
    runtime_experiment_name: str,
    runtime_recorder_uri: str,
    run: ModelRunRecord,
    metrics: dict[str, float | int],
    artifact_paths: list[Path],
) -> str:
    from qlib.workflow import R  # type: ignore[import-untyped]

    with R.start(
        experiment_name=runtime_experiment_name,
        recorder_name=run.run_id,
        uri=runtime_recorder_uri,
    ):
        recorder = R.get_recorder()
        recorder.log_params(
            run_id=run.run_id,
            model_name=run.model_name,
            model_version=run.model_version,
            feature_set_version=run.feature_set_version,
            source_qlib_export_run_id=run.source_qlib_export_run_id,
            source_standard_build_run_id=run.source_standard_build_run_id,
        )
        recorder.log_metrics(**metrics, step=0)
        for path in artifact_paths:
            recorder.log_artifact(str(path))
        return str(recorder.id)
