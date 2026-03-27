CREATE TABLE IF NOT EXISTS shadow_session_run (
    shadow_run_id TEXT PRIMARY KEY,
    paper_run_id TEXT REFERENCES paper_execution_run (paper_run_id),
    strategy_run_id TEXT NOT NULL,
    execution_task_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    market_replay_mode TEXT NOT NULL,
    fill_model_name TEXT NOT NULL,
    fill_model_config_hash TEXT NOT NULL,
    market_data_hash TEXT NOT NULL,
    account_state_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    source_prediction_run_id TEXT NOT NULL,
    source_qlib_export_run_id TEXT,
    source_standard_build_run_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_shadow_session_run_lookup
    ON shadow_session_run (trade_date, account_id, basket_id, execution_task_id);

CREATE TABLE IF NOT EXISTS shadow_order_state_event (
    shadow_run_id TEXT NOT NULL REFERENCES shadow_session_run (shadow_run_id),
    order_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    event_dt TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    state_before TEXT,
    state_after TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    remaining_quantity INTEGER NOT NULL,
    reference_price NUMERIC(18, 6) NOT NULL,
    limit_price NUMERIC(18, 6) NOT NULL,
    reason TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (shadow_run_id, order_id, event_dt, event_type)
);

CREATE INDEX IF NOT EXISTS idx_shadow_order_state_event_lookup
    ON shadow_order_state_event (shadow_run_id, instrument_key, event_dt);

CREATE TABLE IF NOT EXISTS shadow_fill_event (
    shadow_run_id TEXT NOT NULL REFERENCES shadow_session_run (shadow_run_id),
    trade_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    instrument_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL,
    side TEXT NOT NULL,
    fill_dt TIMESTAMPTZ NOT NULL,
    price NUMERIC(18, 6) NOT NULL,
    quantity INTEGER NOT NULL,
    notional NUMERIC(18, 6) NOT NULL,
    cost_breakdown_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (shadow_run_id, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_shadow_fill_event_lookup
    ON shadow_fill_event (shadow_run_id, instrument_key, fill_dt);

CREATE TABLE IF NOT EXISTS shadow_session_report (
    shadow_run_id TEXT PRIMARY KEY REFERENCES shadow_session_run (shadow_run_id),
    paper_run_id TEXT,
    account_id TEXT NOT NULL,
    basket_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    order_count INTEGER NOT NULL,
    filled_order_count INTEGER NOT NULL,
    expired_order_count INTEGER NOT NULL,
    rejected_order_count INTEGER NOT NULL,
    unfilled_order_count INTEGER NOT NULL,
    filled_notional NUMERIC(18, 6) NOT NULL,
    realized_cost_total NUMERIC(18, 6) NOT NULL,
    session_summary_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

