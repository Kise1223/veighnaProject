-- M15 freezes the relational shape for file-first rolling retrain campaign artifacts.
-- The working closed loop remains parquet/json under data/analytics/; these tables are DDL only.

CREATE TABLE IF NOT EXISTS model_schedule_run (
    model_schedule_run_id TEXT PRIMARY KEY,
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    schedule_mode TEXT NOT NULL,
    fixed_model_run_id TEXT,
    latest_model_resolved_run_id TEXT,
    retrain_every_n_trade_days INTEGER,
    training_window_mode TEXT NOT NULL,
    lookback_trade_days INTEGER,
    explicit_schedule_path TEXT,
    benchmark_enabled BOOLEAN NOT NULL,
    benchmark_source_type TEXT NOT NULL,
    campaign_config_hash TEXT NOT NULL,
    campaign_run_id TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_schedule_run_lookup
    ON model_schedule_run (date_start, date_end, account_id, basket_id, schedule_mode, status);

CREATE TABLE IF NOT EXISTS model_schedule_day_row (
    model_schedule_run_id TEXT NOT NULL,
    campaign_run_id TEXT,
    trade_date DATE NOT NULL,
    schedule_action TEXT NOT NULL,
    resolved_model_run_id TEXT NOT NULL,
    resolved_prediction_run_id TEXT,
    train_start DATE NOT NULL,
    train_end DATE NOT NULL,
    model_switch_flag BOOLEAN NOT NULL,
    model_age_trade_days INTEGER NOT NULL,
    days_since_last_retrain INTEGER NOT NULL,
    day_status TEXT NOT NULL,
    reused_flags_json JSONB NOT NULL,
    error_summary TEXT,
    strategy_run_id TEXT,
    execution_task_id TEXT,
    paper_run_id TEXT,
    shadow_run_id TEXT,
    execution_analytics_run_id TEXT,
    portfolio_analytics_run_id TEXT,
    benchmark_analytics_run_id TEXT,
    source_qlib_export_run_id TEXT,
    source_standard_build_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_model_schedule_day_row_run_date
    ON model_schedule_day_row (model_schedule_run_id, trade_date);

ALTER TABLE campaign_run
    ADD COLUMN IF NOT EXISTS model_schedule_run_id TEXT,
    ADD COLUMN IF NOT EXISTS schedule_mode TEXT,
    ADD COLUMN IF NOT EXISTS fixed_model_run_id TEXT,
    ADD COLUMN IF NOT EXISTS latest_model_resolved_run_id TEXT,
    ADD COLUMN IF NOT EXISTS retrain_every_n_trade_days INTEGER,
    ADD COLUMN IF NOT EXISTS training_window_mode TEXT,
    ADD COLUMN IF NOT EXISTS lookback_trade_days INTEGER;

ALTER TABLE campaign_day_row
    ADD COLUMN IF NOT EXISTS model_schedule_run_id TEXT,
    ADD COLUMN IF NOT EXISTS schedule_action TEXT,
    ADD COLUMN IF NOT EXISTS train_start DATE,
    ADD COLUMN IF NOT EXISTS train_end DATE,
    ADD COLUMN IF NOT EXISTS model_switch_flag BOOLEAN,
    ADD COLUMN IF NOT EXISTS model_age_trade_days INTEGER,
    ADD COLUMN IF NOT EXISTS days_since_last_retrain INTEGER;

ALTER TABLE campaign_timeseries_row
    ADD COLUMN IF NOT EXISTS daily_model_switch_flag BOOLEAN,
    ADD COLUMN IF NOT EXISTS daily_model_age_trade_days INTEGER,
    ADD COLUMN IF NOT EXISTS daily_days_since_last_retrain INTEGER;

ALTER TABLE campaign_summary
    ADD COLUMN IF NOT EXISTS unique_model_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS retrain_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS average_model_age_trade_days NUMERIC(18, 6),
    ADD COLUMN IF NOT EXISTS max_model_age_trade_days INTEGER;

ALTER TABLE campaign_compare_run
    ADD COLUMN IF NOT EXISTS source_model_schedule_run_ids JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE campaign_compare_day_row
    ADD COLUMN IF NOT EXISTS left_model_age_trade_days INTEGER,
    ADD COLUMN IF NOT EXISTS right_model_age_trade_days INTEGER,
    ADD COLUMN IF NOT EXISTS delta_model_age_trade_days INTEGER;

ALTER TABLE campaign_compare_summary
    ADD COLUMN IF NOT EXISTS delta_unique_model_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS delta_retrain_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS delta_average_model_age_trade_days NUMERIC(18, 6);
