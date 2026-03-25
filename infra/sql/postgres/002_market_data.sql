CREATE TABLE IF NOT EXISTS recording_runs (
    run_id TEXT PRIMARY KEY,
    source_gateway TEXT NOT NULL,
    mode TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS raw_file_manifest (
    file_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES recording_runs (run_id),
    trade_date DATE NOT NULL,
    exchange TEXT NOT NULL CHECK (exchange IN ('SSE', 'SZSE')),
    symbol TEXT NOT NULL,
    instrument_key TEXT NOT NULL REFERENCES instruments (instrument_key),
    gateway_name TEXT NOT NULL,
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    file_path TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS raw_file_manifest_trade_date_idx
    ON raw_file_manifest (trade_date, exchange, symbol);

CREATE INDEX IF NOT EXISTS raw_file_manifest_instrument_idx
    ON raw_file_manifest (instrument_key, created_at);

CREATE TABLE IF NOT EXISTS standard_file_manifest (
    file_id TEXT PRIMARY KEY,
    build_run_id TEXT NOT NULL,
    layer TEXT NOT NULL,
    trade_date DATE,
    exchange TEXT CHECK (exchange IN ('SSE', 'SZSE')),
    symbol TEXT,
    instrument_key TEXT REFERENCES instruments (instrument_key),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    file_path TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS standard_file_manifest_layer_idx
    ON standard_file_manifest (layer, trade_date, exchange, symbol);

CREATE INDEX IF NOT EXISTS standard_file_manifest_instrument_idx
    ON standard_file_manifest (instrument_key, layer, created_at);

CREATE TABLE IF NOT EXISTS corporate_actions (
    action_id TEXT PRIMARY KEY,
    instrument_key TEXT NOT NULL REFERENCES instruments (instrument_key),
    exchange TEXT NOT NULL CHECK (exchange IN ('SSE', 'SZSE')),
    symbol TEXT NOT NULL,
    action_type TEXT NOT NULL CHECK (
        action_type IN ('cash_dividend', 'stock_split', 'reverse_split', 'rights_issue', 'bonus_share')
    ),
    ex_date DATE NOT NULL,
    effective_date DATE NOT NULL,
    cash_per_share DOUBLE PRECISION,
    share_ratio DOUBLE PRECISION,
    rights_price DOUBLE PRECISION,
    source TEXT NOT NULL,
    source_hash TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS corporate_actions_effective_idx
    ON corporate_actions (instrument_key, effective_date);

CREATE TABLE IF NOT EXISTS adjustment_factors (
    instrument_key TEXT NOT NULL REFERENCES instruments (instrument_key),
    trade_date DATE NOT NULL,
    adj_factor DOUBLE PRECISION NOT NULL,
    adj_mode TEXT NOT NULL CHECK (adj_mode IN ('forward', 'backward')),
    source_run_id TEXT NOT NULL,
    PRIMARY KEY (instrument_key, trade_date, adj_mode)
);

CREATE INDEX IF NOT EXISTS adjustment_factors_trade_date_idx
    ON adjustment_factors (trade_date, instrument_key);

CREATE TABLE IF NOT EXISTS dq_reports (
    report_id TEXT PRIMARY KEY,
    layer TEXT NOT NULL,
    trade_date DATE,
    scope TEXT NOT NULL,
    status TEXT NOT NULL,
    issue_count INTEGER NOT NULL CHECK (issue_count >= 0),
    report_path TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS dq_reports_trade_date_idx
    ON dq_reports (trade_date, layer);
