-- M12 freezes the relational shape for file-first portfolio / risk analytics artifacts.
-- The working closed loop remains parquet/json under data/analytics/; these tables are DDL only.

CREATE TABLE IF NOT EXISTS portfolio_analytics_run (
    portfolio_analytics_run_id TEXT PRIMARY KEY,
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

CREATE INDEX IF NOT EXISTS idx_portfolio_analytics_run_lookup
    ON portfolio_analytics_run (trade_date, account_id, basket_id, source_type, status);

CREATE TABLE IF NOT EXISTS portfolio_position_row (
    portfolio_analytics_run_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    target_weight NUMERIC(18, 6) NOT NULL,
    executed_weight NUMERIC(18, 6) NOT NULL,
    weight_drift NUMERIC(18, 6) NOT NULL,
    target_rank INTEGER,
    target_score DOUBLE PRECISION,
    quantity_end INTEGER NOT NULL,
    sellable_quantity_end INTEGER NOT NULL,
    avg_price_end NUMERIC(18, 6) NOT NULL,
    market_value_end NUMERIC(18, 6) NOT NULL,
    executed_price_reference NUMERIC(18, 6),
    realized_pnl NUMERIC(18, 6) NOT NULL,
    unrealized_pnl NUMERIC(18, 6) NOT NULL,
    planned_notional NUMERIC(18, 6) NOT NULL,
    filled_notional NUMERIC(18, 6) NOT NULL,
    fill_rate DOUBLE PRECISION NOT NULL,
    session_end_status TEXT,
    replay_mode TEXT,
    fill_model_name TEXT,
    time_in_force TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_portfolio_position_row_run_instrument
    ON portfolio_position_row (portfolio_analytics_run_id, instrument_key);

CREATE TABLE IF NOT EXISTS portfolio_group_row (
    portfolio_analytics_run_id TEXT NOT NULL,
    group_type TEXT NOT NULL,
    group_key TEXT NOT NULL,
    target_weight_sum NUMERIC(18, 6) NOT NULL,
    executed_weight_sum NUMERIC(18, 6) NOT NULL,
    weight_drift_sum NUMERIC(18, 6) NOT NULL,
    market_value_end NUMERIC(18, 6) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_portfolio_group_row_run_group
    ON portfolio_group_row (portfolio_analytics_run_id, group_type, group_key);

CREATE TABLE IF NOT EXISTS portfolio_summary (
    portfolio_analytics_run_id TEXT PRIMARY KEY,
    holdings_count_target INTEGER NOT NULL,
    holdings_count_end INTEGER NOT NULL,
    target_cash_weight NUMERIC(18, 6) NOT NULL,
    executed_cash_weight NUMERIC(18, 6) NOT NULL,
    gross_exposure_end NUMERIC(18, 6) NOT NULL,
    net_exposure_end NUMERIC(18, 6) NOT NULL,
    realized_turnover NUMERIC(18, 6) NOT NULL,
    filled_notional_total NUMERIC(18, 6) NOT NULL,
    planned_notional_total NUMERIC(18, 6) NOT NULL,
    fill_rate_gross DOUBLE PRECISION NOT NULL,
    top1_concentration NUMERIC(18, 6) NOT NULL,
    top3_concentration NUMERIC(18, 6) NOT NULL,
    top5_concentration NUMERIC(18, 6) NOT NULL,
    hhi_concentration NUMERIC(18, 6) NOT NULL,
    total_realized_pnl NUMERIC(18, 6) NOT NULL,
    total_unrealized_pnl NUMERIC(18, 6) NOT NULL,
    total_weight_drift_l1 NUMERIC(18, 6) NOT NULL,
    tracking_error_proxy NUMERIC(18, 6) NOT NULL,
    net_liquidation_start NUMERIC(18, 6) NOT NULL,
    net_liquidation_end NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_compare_run (
    portfolio_compare_run_id TEXT PRIMARY KEY,
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

CREATE INDEX IF NOT EXISTS idx_portfolio_compare_run_basis
    ON portfolio_compare_run (compare_basis, status, created_at);

CREATE TABLE IF NOT EXISTS portfolio_compare_row (
    portfolio_compare_run_id TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_portfolio_compare_row_run_metric
    ON portfolio_compare_row (portfolio_compare_run_id, instrument_key, metric_name);

CREATE TABLE IF NOT EXISTS portfolio_compare_summary (
    portfolio_compare_run_id TEXT PRIMARY KEY,
    left_run_id TEXT NOT NULL,
    right_run_id TEXT NOT NULL,
    compare_basis TEXT NOT NULL,
    comparable_count INTEGER NOT NULL,
    delta_cash_weight NUMERIC(18, 6) NOT NULL,
    delta_fill_rate DOUBLE PRECISION NOT NULL,
    delta_weight_drift_l1 NUMERIC(18, 6) NOT NULL,
    delta_top1_concentration NUMERIC(18, 6) NOT NULL,
    delta_top5_concentration NUMERIC(18, 6) NOT NULL,
    delta_hhi_concentration NUMERIC(18, 6) NOT NULL,
    delta_realized_turnover NUMERIC(18, 6) NOT NULL,
    delta_net_liquidation_end NUMERIC(18, 6) NOT NULL,
    delta_total_realized_pnl NUMERIC(18, 6) NOT NULL,
    delta_total_unrealized_pnl NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
