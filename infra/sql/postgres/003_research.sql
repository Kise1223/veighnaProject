CREATE TABLE IF NOT EXISTS model_run (
    run_id TEXT PRIMARY KEY,
    experiment_name TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    feature_set_name TEXT NOT NULL,
    feature_set_version TEXT NOT NULL,
    provider_uri TEXT NOT NULL,
    calendar_start DATE NOT NULL,
    calendar_end DATE NOT NULL,
    train_start DATE NOT NULL,
    train_end DATE NOT NULL,
    infer_trade_date DATE NOT NULL,
    status TEXT NOT NULL,
    artifact_path TEXT NOT NULL,
    artifact_hash TEXT NOT NULL,
    metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_standard_build_run_id TEXT,
    source_qlib_export_run_id TEXT,
    config_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_model_run_experiment_name ON model_run (experiment_name, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_model_run_source_qlib_export_run_id ON model_run (source_qlib_export_run_id);
CREATE INDEX IF NOT EXISTS ix_model_run_source_standard_build_run_id ON model_run (source_standard_build_run_id);

CREATE TABLE IF NOT EXISTS prediction (
    trade_date DATE NOT NULL,
    instrument_key TEXT NOT NULL,
    qlib_symbol TEXT NOT NULL,
    score NUMERIC NOT NULL,
    run_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    feature_set_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (trade_date, instrument_key, run_id)
);

CREATE INDEX IF NOT EXISTS ix_prediction_run_id ON prediction (run_id);
CREATE INDEX IF NOT EXISTS ix_prediction_trade_date ON prediction (trade_date);
CREATE INDEX IF NOT EXISTS ix_prediction_qlib_symbol ON prediction (qlib_symbol);
