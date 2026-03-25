CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS instrument_keys (
    instrument_key TEXT PRIMARY KEY,
    canonical_symbol TEXT NOT NULL,
    vendor_symbol TEXT NOT NULL,
    broker_symbol TEXT NOT NULL,
    vt_symbol TEXT NOT NULL,
    qlib_symbol TEXT NOT NULL,
    symbol TEXT NOT NULL,
    exchange TEXT NOT NULL CHECK (exchange IN ('SSE', 'SZSE')),
    source TEXT NOT NULL,
    source_version TEXT NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE
);

CREATE UNIQUE INDEX IF NOT EXISTS instrument_keys_vt_symbol_idx
    ON instrument_keys (vt_symbol);

CREATE TABLE IF NOT EXISTS instruments (
    instrument_key TEXT PRIMARY KEY REFERENCES instrument_keys (instrument_key),
    exchange TEXT NOT NULL CHECK (exchange IN ('SSE', 'SZSE')),
    symbol TEXT NOT NULL,
    instrument_type TEXT NOT NULL CHECK (instrument_type IN ('EQUITY', 'ETF')),
    board TEXT NOT NULL CHECK (board IN ('MAIN', 'GEM', 'STAR', 'ETF')),
    list_date DATE NOT NULL,
    delist_date DATE,
    settlement_type TEXT NOT NULL CHECK (settlement_type IN ('T0', 'T1')),
    pricetick NUMERIC(18, 6) NOT NULL,
    min_buy_lot INTEGER NOT NULL,
    odd_lot_sell_only BOOLEAN NOT NULL,
    limit_pct NUMERIC(10, 6),
    ipo_free_limit_days INTEGER NOT NULL DEFAULT 0,
    after_hours_fixed_price_supported BOOLEAN NOT NULL DEFAULT FALSE,
    source TEXT NOT NULL,
    source_version TEXT NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE
);

CREATE TABLE IF NOT EXISTS market_rules (
    rule_id TEXT PRIMARY KEY,
    exchange TEXT NOT NULL CHECK (exchange IN ('SSE', 'SZSE')),
    instrument_type TEXT NOT NULL CHECK (instrument_type IN ('EQUITY', 'ETF')),
    board TEXT NOT NULL CHECK (board IN ('MAIN', 'GEM', 'STAR', 'ETF')),
    effective_from DATE NOT NULL,
    effective_to DATE,
    trading_sessions JSONB NOT NULL,
    cancel_restricted_windows JSONB NOT NULL,
    price_limit_ratio NUMERIC(10, 6),
    ipo_free_limit_days INTEGER NOT NULL DEFAULT 0,
    after_hours_supported BOOLEAN NOT NULL DEFAULT FALSE,
    source TEXT NOT NULL,
    source_version TEXT NOT NULL
);

ALTER TABLE market_rules
    ADD CONSTRAINT market_rules_no_overlap
    EXCLUDE USING gist (
        exchange WITH =,
        instrument_type WITH =,
        board WITH =,
        daterange(effective_from, COALESCE(effective_to, 'infinity'::date), '[]') WITH &&
    );

CREATE TABLE IF NOT EXISTS cost_profiles (
    cost_profile_id TEXT PRIMARY KEY,
    broker TEXT NOT NULL,
    instrument_type TEXT NOT NULL CHECK (instrument_type IN ('EQUITY', 'ETF')),
    exchange TEXT CHECK (exchange IN ('SSE', 'SZSE')),
    effective_from DATE NOT NULL,
    effective_to DATE,
    commission_rate NUMERIC(18, 8) NOT NULL,
    commission_min NUMERIC(18, 2) NOT NULL,
    tax_sell_rate NUMERIC(18, 8) NOT NULL,
    handling_fee_rate NUMERIC(18, 8) NOT NULL,
    transfer_fee_rate NUMERIC(18, 8) NOT NULL,
    reg_fee_rate NUMERIC(18, 8) NOT NULL,
    source TEXT NOT NULL,
    source_version TEXT NOT NULL
);

ALTER TABLE cost_profiles
    ADD CONSTRAINT cost_profiles_no_overlap
    EXCLUDE USING gist (
        broker WITH =,
        instrument_type WITH =,
        COALESCE(exchange, '') WITH =,
        daterange(effective_from, COALESCE(effective_to, 'infinity'::date), '[]') WITH &&
    );

CREATE TABLE IF NOT EXISTS raw_adapter_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    gateway_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    exchange_ts TIMESTAMPTZ NOT NULL,
    received_ts TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS account_compliance_counters (
    counter_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id TEXT NOT NULL,
    gateway_name TEXT NOT NULL,
    granularity TEXT NOT NULL CHECK (granularity IN ('SECOND', 'DAY')),
    bucket_start TIMESTAMPTZ NOT NULL,
    per_second_order_count INTEGER NOT NULL DEFAULT 0,
    per_second_cancel_count INTEGER NOT NULL DEFAULT 0,
    daily_order_count INTEGER NOT NULL DEFAULT 0,
    daily_cancel_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (account_id, gateway_name, granularity, bucket_start)
);
