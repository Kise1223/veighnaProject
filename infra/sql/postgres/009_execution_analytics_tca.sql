-- M11 freezes the relational shape for file-first execution analytics / TCA artifacts.
-- The working closed loop remains parquet/json under data/analytics/; these tables are DDL only.

CREATE TABLE IF NOT EXISTS execution_analytics_run (
    analytics_run_id TEXT PRIMARY KEY,
    trade_date DATE NOT NULL,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_run_ids JSONB NOT NULL,
    source_execution_task_id TEXT NOT NULL,
    source_strategy_run_id TEXT NOT NULL,
    source_prediction_run_id TEXT NOT NULL,
    analytics_config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    source_qlib_export_run_id TEXT,
    source_standard_build_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_analytics_run_lookup
    ON execution_analytics_run (trade_date, account_id, basket_id, source_type, status);

CREATE TABLE IF NOT EXISTS execution_tca_row (
    analytics_run_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    side TEXT NOT NULL,
    requested_quantity INTEGER NOT NULL,
    filled_quantity INTEGER NOT NULL,
    remaining_quantity INTEGER NOT NULL,
    fill_rate DOUBLE PRECISION NOT NULL,
    partial_fill_count INTEGER NOT NULL,
    avg_fill_price NUMERIC(18, 6),
    reference_price NUMERIC(18, 6) NOT NULL,
    previous_close NUMERIC(18, 6),
    planned_notional NUMERIC(18, 6) NOT NULL,
    filled_notional NUMERIC(18, 6) NOT NULL,
    estimated_cost_total NUMERIC(18, 6) NOT NULL,
    realized_cost_total NUMERIC(18, 6) NOT NULL,
    implementation_shortfall NUMERIC(18, 6) NOT NULL,
    first_fill_dt TIMESTAMPTZ,
    last_fill_dt TIMESTAMPTZ,
    session_end_status TEXT NOT NULL,
    replay_mode TEXT,
    fill_model_name TEXT,
    time_in_force TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_tca_row_run_instrument
    ON execution_tca_row (analytics_run_id, instrument_key);

CREATE TABLE IF NOT EXISTS execution_tca_summary (
    analytics_run_id TEXT PRIMARY KEY,
    order_count INTEGER NOT NULL,
    filled_order_count INTEGER NOT NULL,
    partially_filled_order_count INTEGER NOT NULL,
    expired_order_count INTEGER NOT NULL,
    rejected_order_count INTEGER NOT NULL,
    total_requested_notional NUMERIC(18, 6) NOT NULL,
    total_filled_notional NUMERIC(18, 6) NOT NULL,
    gross_fill_rate DOUBLE PRECISION NOT NULL,
    avg_fill_rate DOUBLE PRECISION NOT NULL,
    total_estimated_cost NUMERIC(18, 6) NOT NULL,
    total_realized_cost NUMERIC(18, 6) NOT NULL,
    total_implementation_shortfall NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_compare_run (
    compare_run_id TEXT PRIMARY KEY,
    left_run_id TEXT NOT NULL,
    right_run_id TEXT NOT NULL,
    compare_basis TEXT NOT NULL,
    analytics_config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    source_execution_task_ids JSONB NOT NULL,
    source_strategy_run_ids JSONB NOT NULL,
    source_prediction_run_ids JSONB NOT NULL,
    source_qlib_export_run_ids JSONB NOT NULL,
    source_standard_build_run_ids JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_compare_run_basis
    ON execution_compare_run (compare_basis, status, created_at);

CREATE TABLE IF NOT EXISTS execution_compare_row (
    compare_run_id TEXT NOT NULL,
    left_run_id TEXT NOT NULL,
    right_run_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    left_value TEXT,
    right_value TEXT,
    delta_value TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_execution_compare_row_run_metric
    ON execution_compare_row (compare_run_id, instrument_key, metric_name);

CREATE TABLE IF NOT EXISTS execution_compare_summary (
    compare_run_id TEXT PRIMARY KEY,
    left_run_id TEXT NOT NULL,
    right_run_id TEXT NOT NULL,
    compare_basis TEXT NOT NULL,
    order_count INTEGER NOT NULL,
    comparable_order_count INTEGER NOT NULL,
    delta_filled_notional NUMERIC(18, 6) NOT NULL,
    delta_fill_rate DOUBLE PRECISION NOT NULL,
    delta_realized_cost NUMERIC(18, 6) NOT NULL,
    delta_implementation_shortfall NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
