-- M14 freezes the relational shape for file-first walk-forward campaign artifacts.
-- The working closed loop remains parquet/json under data/analytics/; these tables are DDL only.

CREATE TABLE IF NOT EXISTS campaign_run (
    campaign_run_id TEXT PRIMARY KEY,
    date_start DATE NOT NULL,
    date_end DATE NOT NULL,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    execution_source_type TEXT NOT NULL,
    market_replay_mode TEXT,
    tick_fill_model TEXT,
    time_in_force TEXT,
    benchmark_enabled BOOLEAN NOT NULL,
    benchmark_source_type TEXT NOT NULL,
    model_run_id TEXT NOT NULL,
    campaign_config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_campaign_run_lookup
    ON campaign_run (date_start, date_end, account_id, basket_id, execution_source_type, status);

CREATE TABLE IF NOT EXISTS campaign_day_row (
    campaign_run_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    day_status TEXT NOT NULL,
    model_run_id TEXT NOT NULL,
    prediction_run_id TEXT,
    strategy_run_id TEXT,
    execution_task_id TEXT,
    paper_run_id TEXT,
    shadow_run_id TEXT,
    execution_analytics_run_id TEXT,
    portfolio_analytics_run_id TEXT,
    benchmark_run_id TEXT,
    benchmark_analytics_run_id TEXT,
    reused_flags_json JSONB NOT NULL,
    error_summary TEXT,
    source_qlib_export_run_id TEXT,
    source_standard_build_run_id TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_campaign_day_row_run_date
    ON campaign_day_row (campaign_run_id, trade_date);

CREATE TABLE IF NOT EXISTS campaign_timeseries_row (
    campaign_run_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    net_liquidation_end NUMERIC(18, 6) NOT NULL,
    cash_weight_end NUMERIC(18, 6) NOT NULL,
    daily_realized_pnl NUMERIC(18, 6) NOT NULL,
    daily_unrealized_pnl NUMERIC(18, 6) NOT NULL,
    daily_filled_notional NUMERIC(18, 6) NOT NULL,
    daily_realized_cost NUMERIC(18, 6) NOT NULL,
    daily_turnover NUMERIC(18, 6) NOT NULL,
    daily_fill_rate DOUBLE PRECISION NOT NULL,
    daily_weight_drift_l1 NUMERIC(18, 6) NOT NULL,
    daily_top5_concentration NUMERIC(18, 6) NOT NULL,
    daily_hhi_concentration NUMERIC(18, 6) NOT NULL,
    daily_active_share NUMERIC(18, 6),
    daily_active_contribution_proxy NUMERIC(18, 6),
    replay_mode TEXT,
    fill_model_name TEXT,
    time_in_force TEXT,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_campaign_timeseries_row_run_date
    ON campaign_timeseries_row (campaign_run_id, trade_date);

CREATE TABLE IF NOT EXISTS campaign_summary (
    campaign_run_id TEXT PRIMARY KEY,
    day_count INTEGER NOT NULL,
    success_day_count INTEGER NOT NULL,
    reused_day_count INTEGER NOT NULL,
    failed_day_count INTEGER NOT NULL,
    net_liquidation_start NUMERIC(18, 6) NOT NULL,
    net_liquidation_end NUMERIC(18, 6) NOT NULL,
    cumulative_realized_pnl NUMERIC(18, 6) NOT NULL,
    final_unrealized_pnl NUMERIC(18, 6) NOT NULL,
    cumulative_filled_notional NUMERIC(18, 6) NOT NULL,
    cumulative_realized_cost NUMERIC(18, 6) NOT NULL,
    average_fill_rate DOUBLE PRECISION NOT NULL,
    average_turnover NUMERIC(18, 6) NOT NULL,
    average_weight_drift_l1 NUMERIC(18, 6) NOT NULL,
    final_top5_concentration NUMERIC(18, 6) NOT NULL,
    final_hhi_concentration NUMERIC(18, 6) NOT NULL,
    average_active_share NUMERIC(18, 6),
    final_active_share NUMERIC(18, 6),
    cumulative_active_contribution_proxy NUMERIC(18, 6),
    max_drawdown NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS campaign_compare_run (
    campaign_compare_run_id TEXT PRIMARY KEY,
    left_campaign_run_id TEXT NOT NULL,
    right_campaign_run_id TEXT NOT NULL,
    compare_basis TEXT NOT NULL,
    compare_config_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    source_model_run_ids JSONB NOT NULL,
    source_prediction_run_ids JSONB NOT NULL,
    source_strategy_run_ids JSONB NOT NULL,
    source_execution_task_ids JSONB NOT NULL,
    source_paper_run_ids JSONB NOT NULL,
    source_shadow_run_ids JSONB NOT NULL,
    source_execution_analytics_run_ids JSONB NOT NULL,
    source_portfolio_analytics_run_ids JSONB NOT NULL,
    source_benchmark_analytics_run_ids JSONB NOT NULL,
    source_qlib_export_run_ids JSONB NOT NULL,
    source_standard_build_run_ids JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_campaign_compare_run_basis
    ON campaign_compare_run (compare_basis, status, created_at);

CREATE TABLE IF NOT EXISTS campaign_compare_day_row (
    campaign_compare_run_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    left_campaign_run_id TEXT NOT NULL,
    right_campaign_run_id TEXT NOT NULL,
    left_net_liquidation_end NUMERIC(18, 6) NOT NULL,
    right_net_liquidation_end NUMERIC(18, 6) NOT NULL,
    delta_net_liquidation_end NUMERIC(18, 6) NOT NULL,
    left_fill_rate DOUBLE PRECISION NOT NULL,
    right_fill_rate DOUBLE PRECISION NOT NULL,
    delta_fill_rate DOUBLE PRECISION NOT NULL,
    left_active_share NUMERIC(18, 6),
    right_active_share NUMERIC(18, 6),
    delta_active_share NUMERIC(18, 6),
    left_top5_concentration NUMERIC(18, 6) NOT NULL,
    right_top5_concentration NUMERIC(18, 6) NOT NULL,
    delta_top5_concentration NUMERIC(18, 6) NOT NULL,
    left_active_contribution_proxy NUMERIC(18, 6),
    right_active_contribution_proxy NUMERIC(18, 6),
    delta_active_contribution_proxy NUMERIC(18, 6),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_campaign_compare_day_row_run_date
    ON campaign_compare_day_row (campaign_compare_run_id, trade_date);

CREATE TABLE IF NOT EXISTS campaign_compare_summary (
    campaign_compare_run_id TEXT PRIMARY KEY,
    left_campaign_run_id TEXT NOT NULL,
    right_campaign_run_id TEXT NOT NULL,
    compare_basis TEXT NOT NULL,
    overlapping_day_count INTEGER NOT NULL,
    delta_net_liquidation_end NUMERIC(18, 6) NOT NULL,
    delta_cumulative_realized_pnl NUMERIC(18, 6) NOT NULL,
    delta_cumulative_realized_cost NUMERIC(18, 6) NOT NULL,
    delta_average_fill_rate DOUBLE PRECISION NOT NULL,
    delta_average_turnover NUMERIC(18, 6) NOT NULL,
    delta_final_active_share NUMERIC(18, 6),
    delta_final_top5_concentration NUMERIC(18, 6) NOT NULL,
    delta_max_drawdown NUMERIC(18, 6) NOT NULL,
    delta_cumulative_active_contribution_proxy NUMERIC(18, 6),
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
