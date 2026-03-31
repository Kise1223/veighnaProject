-- M16 freezes the relational shape for file-first schedule realism and audit artifacts.
-- The working closed loop remains parquet/json under data/analytics/; these tables are DDL only.

ALTER TABLE model_schedule_run
    ADD COLUMN IF NOT EXISTS explicit_schedule_path TEXT;

ALTER TABLE model_schedule_day_row
    ADD COLUMN IF NOT EXISTS strict_no_lookahead_expected BOOLEAN,
    ADD COLUMN IF NOT EXISTS strict_no_lookahead_passed BOOLEAN,
    ADD COLUMN IF NOT EXISTS schedule_warning_code TEXT;

CREATE TABLE IF NOT EXISTS schedule_audit_run (
    schedule_audit_run_id TEXT PRIMARY KEY,
    model_schedule_run_id TEXT NOT NULL,
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    schedule_mode TEXT NOT NULL,
    training_window_mode TEXT,
    explicit_schedule_path TEXT,
    audit_config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedule_audit_run_lookup
    ON schedule_audit_run (model_schedule_run_id, status, created_at);

CREATE TABLE IF NOT EXISTS schedule_audit_day_row (
    schedule_audit_run_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    resolved_model_run_id TEXT NOT NULL,
    train_start DATE,
    train_end DATE,
    previous_trade_date DATE,
    strict_no_lookahead_expected BOOLEAN NOT NULL,
    strict_no_lookahead_passed BOOLEAN NOT NULL,
    model_switch_flag BOOLEAN NOT NULL,
    model_age_trade_days INTEGER NOT NULL,
    days_since_last_retrain INTEGER NOT NULL,
    schedule_warning_code TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedule_audit_day_row_run_date
    ON schedule_audit_day_row (schedule_audit_run_id, trade_date);

CREATE TABLE IF NOT EXISTS schedule_audit_summary (
    schedule_audit_run_id TEXT PRIMARY KEY,
    day_count INTEGER NOT NULL,
    strict_checked_day_count INTEGER NOT NULL,
    strict_pass_day_count INTEGER NOT NULL,
    strict_fail_day_count INTEGER NOT NULL,
    warning_day_count INTEGER NOT NULL,
    unique_model_count INTEGER NOT NULL,
    retrain_count INTEGER NOT NULL,
    average_model_age_trade_days NUMERIC(18, 6),
    max_model_age_trade_days INTEGER,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE campaign_summary
    ADD COLUMN IF NOT EXISTS strict_checked_day_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS strict_pass_day_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS strict_fail_day_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS warning_day_count INTEGER DEFAULT 0;

ALTER TABLE campaign_compare_summary
    ADD COLUMN IF NOT EXISTS delta_strict_fail_day_count INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS delta_warning_day_count INTEGER DEFAULT 0;
