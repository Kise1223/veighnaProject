-- M13 freezes the relational shape for file-first benchmark / attribution analytics artifacts.
-- The working closed loop remains parquet/json under data/analytics/; these tables are DDL only.

CREATE TABLE IF NOT EXISTS benchmark_reference_run (
    benchmark_run_id TEXT PRIMARY KEY,
    trade_date DATE NOT NULL,
    benchmark_name TEXT NOT NULL,
    benchmark_source_type TEXT NOT NULL,
    source_portfolio_analytics_run_id TEXT NOT NULL,
    source_strategy_run_id TEXT NOT NULL,
    source_prediction_run_id TEXT NOT NULL,
    benchmark_config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    source_qlib_export_run_id TEXT,
    source_standard_build_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_benchmark_reference_run_lookup
    ON benchmark_reference_run (trade_date, benchmark_source_type, status);

CREATE TABLE IF NOT EXISTS benchmark_weight_row (
    benchmark_run_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    benchmark_weight NUMERIC(18, 6) NOT NULL,
    benchmark_rank INTEGER NOT NULL,
    group_key_optional TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_benchmark_weight_row_run_instrument
    ON benchmark_weight_row (benchmark_run_id, instrument_key);

CREATE TABLE IF NOT EXISTS benchmark_summary (
    benchmark_run_id TEXT PRIMARY KEY,
    holdings_count INTEGER NOT NULL,
    benchmark_cash_weight NUMERIC(18, 6) NOT NULL,
    top1_concentration NUMERIC(18, 6) NOT NULL,
    top3_concentration NUMERIC(18, 6) NOT NULL,
    top5_concentration NUMERIC(18, 6) NOT NULL,
    hhi_concentration NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS benchmark_analytics_run (
    benchmark_analytics_run_id TEXT PRIMARY KEY,
    trade_date DATE NOT NULL,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    source_portfolio_analytics_run_id TEXT NOT NULL,
    source_run_type TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    source_execution_task_id TEXT NOT NULL,
    source_strategy_run_id TEXT NOT NULL,
    source_prediction_run_id TEXT NOT NULL,
    benchmark_run_id TEXT NOT NULL,
    analytics_config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    source_qlib_export_run_id TEXT,
    source_standard_build_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_benchmark_analytics_run_lookup
    ON benchmark_analytics_run (trade_date, account_id, basket_id, source_run_type, status);

CREATE TABLE IF NOT EXISTS benchmark_position_row (
    benchmark_analytics_run_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    target_weight NUMERIC(18, 6) NOT NULL,
    executed_weight NUMERIC(18, 6) NOT NULL,
    benchmark_weight NUMERIC(18, 6) NOT NULL,
    active_weight_target NUMERIC(18, 6) NOT NULL,
    active_weight_executed NUMERIC(18, 6) NOT NULL,
    target_rank INTEGER,
    target_score DOUBLE PRECISION,
    market_value_end NUMERIC(18, 6) NOT NULL,
    realized_pnl NUMERIC(18, 6) NOT NULL,
    unrealized_pnl NUMERIC(18, 6) NOT NULL,
    portfolio_contribution_proxy NUMERIC(18, 6) NOT NULL,
    benchmark_contribution_proxy NUMERIC(18, 6) NOT NULL,
    active_contribution_proxy NUMERIC(18, 6) NOT NULL,
    instrument_return_proxy NUMERIC(18, 6) NOT NULL,
    replay_mode TEXT,
    fill_model_name TEXT,
    time_in_force TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_benchmark_position_row_run_instrument
    ON benchmark_position_row (benchmark_analytics_run_id, instrument_key);

CREATE TABLE IF NOT EXISTS benchmark_group_row (
    benchmark_analytics_run_id TEXT NOT NULL,
    group_type TEXT NOT NULL,
    group_key TEXT NOT NULL,
    target_weight_sum NUMERIC(18, 6) NOT NULL,
    executed_weight_sum NUMERIC(18, 6) NOT NULL,
    benchmark_weight_sum NUMERIC(18, 6) NOT NULL,
    active_weight_sum NUMERIC(18, 6) NOT NULL,
    portfolio_return_proxy NUMERIC(18, 6) NOT NULL,
    benchmark_return_proxy NUMERIC(18, 6) NOT NULL,
    allocation_proxy NUMERIC(18, 6) NOT NULL,
    selection_proxy NUMERIC(18, 6) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_benchmark_group_row_run_group
    ON benchmark_group_row (benchmark_analytics_run_id, group_type, group_key);

CREATE TABLE IF NOT EXISTS benchmark_summary_row (
    benchmark_analytics_run_id TEXT PRIMARY KEY,
    holdings_count_benchmark INTEGER NOT NULL,
    holdings_overlap_count INTEGER NOT NULL,
    target_active_share NUMERIC(18, 6) NOT NULL,
    executed_active_share NUMERIC(18, 6) NOT NULL,
    active_cash_weight NUMERIC(18, 6) NOT NULL,
    benchmark_cash_weight NUMERIC(18, 6) NOT NULL,
    delta_top1_concentration NUMERIC(18, 6) NOT NULL,
    delta_top5_concentration NUMERIC(18, 6) NOT NULL,
    delta_hhi_concentration NUMERIC(18, 6) NOT NULL,
    total_portfolio_contribution_proxy NUMERIC(18, 6) NOT NULL,
    total_benchmark_contribution_proxy NUMERIC(18, 6) NOT NULL,
    total_active_contribution_proxy NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS benchmark_compare_run (
    benchmark_compare_run_id TEXT PRIMARY KEY,
    left_benchmark_analytics_run_id TEXT NOT NULL,
    right_benchmark_analytics_run_id TEXT NOT NULL,
    compare_basis TEXT NOT NULL,
    analytics_config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    source_execution_task_ids JSONB NOT NULL,
    source_strategy_run_ids JSONB NOT NULL,
    source_prediction_run_ids JSONB NOT NULL,
    source_portfolio_analytics_run_ids JSONB NOT NULL,
    benchmark_run_ids JSONB NOT NULL,
    source_qlib_export_run_ids JSONB NOT NULL,
    source_standard_build_run_ids JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_benchmark_compare_run_basis
    ON benchmark_compare_run (compare_basis, status, created_at);

CREATE TABLE IF NOT EXISTS benchmark_compare_row (
    benchmark_compare_run_id TEXT NOT NULL,
    left_benchmark_analytics_run_id TEXT NOT NULL,
    right_benchmark_analytics_run_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    left_value TEXT,
    right_value TEXT,
    delta_value TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_benchmark_compare_row_run_metric
    ON benchmark_compare_row (benchmark_compare_run_id, instrument_key, metric_name);

CREATE TABLE IF NOT EXISTS benchmark_compare_summary (
    benchmark_compare_run_id TEXT PRIMARY KEY,
    left_benchmark_analytics_run_id TEXT NOT NULL,
    right_benchmark_analytics_run_id TEXT NOT NULL,
    compare_basis TEXT NOT NULL,
    comparable_count INTEGER NOT NULL,
    delta_executed_active_share NUMERIC(18, 6) NOT NULL,
    delta_active_cash_weight NUMERIC(18, 6) NOT NULL,
    delta_total_active_contribution_proxy NUMERIC(18, 6) NOT NULL,
    delta_delta_hhi_concentration NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
