CREATE TABLE IF NOT EXISTS paper_execution_run (
    paper_run_id TEXT PRIMARY KEY,
    strategy_run_id TEXT NOT NULL,
    execution_task_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    fill_model_name TEXT NOT NULL,
    fill_model_config_hash TEXT NOT NULL,
    market_data_hash TEXT NOT NULL,
    account_state_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    source_prediction_run_id TEXT NOT NULL,
    source_qlib_export_run_id TEXT,
    source_standard_build_run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_paper_execution_run_lookup
    ON paper_execution_run (trade_date, account_id, basket_id, execution_task_id);

CREATE TABLE IF NOT EXISTS paper_order (
    order_id TEXT PRIMARY KEY,
    paper_run_id TEXT NOT NULL REFERENCES paper_execution_run (paper_run_id),
    execution_task_id TEXT NOT NULL,
    strategy_run_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    limit_price NUMERIC(18, 6) NOT NULL,
    reference_price NUMERIC(18, 6) NOT NULL,
    previous_close NUMERIC(18, 6),
    source_order_intent_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_order_run
    ON paper_order (paper_run_id, instrument_key, side);

CREATE TABLE IF NOT EXISTS paper_trade (
    trade_id TEXT PRIMARY KEY,
    paper_run_id TEXT NOT NULL REFERENCES paper_execution_run (paper_run_id),
    order_id TEXT NOT NULL REFERENCES paper_order (order_id),
    execution_task_id TEXT NOT NULL,
    strategy_run_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(18, 6) NOT NULL,
    notional NUMERIC(18, 6) NOT NULL,
    cost_breakdown_json JSONB NOT NULL,
    fill_bar_dt TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_paper_trade_run
    ON paper_trade (paper_run_id, instrument_key);

CREATE TABLE IF NOT EXISTS paper_account_snapshot (
    paper_run_id TEXT PRIMARY KEY REFERENCES paper_execution_run (paper_run_id),
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    cash_start NUMERIC(18, 6) NOT NULL,
    cash_end NUMERIC(18, 6) NOT NULL,
    fees_total NUMERIC(18, 6) NOT NULL,
    realized_pnl NUMERIC(18, 6) NOT NULL,
    market_value_end NUMERIC(18, 6) NOT NULL,
    net_liquidation_end NUMERIC(18, 6) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_position_snapshot (
    paper_run_id TEXT NOT NULL REFERENCES paper_execution_run (paper_run_id),
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    sellable_quantity INTEGER NOT NULL,
    avg_price NUMERIC(18, 6) NOT NULL,
    market_value NUMERIC(18, 6) NOT NULL,
    unrealized_pnl NUMERIC(18, 6) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (paper_run_id, instrument_key)
);

CREATE INDEX IF NOT EXISTS idx_paper_position_snapshot_account
    ON paper_position_snapshot (account_id, trade_date);

CREATE TABLE IF NOT EXISTS paper_reconcile_report (
    paper_run_id TEXT PRIMARY KEY REFERENCES paper_execution_run (paper_run_id),
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    planned_order_count INTEGER NOT NULL,
    filled_order_count INTEGER NOT NULL,
    rejected_order_count INTEGER NOT NULL,
    unfilled_order_count INTEGER NOT NULL,
    planned_notional NUMERIC(18, 6) NOT NULL,
    filled_notional NUMERIC(18, 6) NOT NULL,
    estimated_cost_total NUMERIC(18, 6) NOT NULL,
    realized_cost_total NUMERIC(18, 6) NOT NULL,
    summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
