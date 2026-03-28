-- M10 extends the file-first shadow-session contract with deterministic L1 partial-fill metadata.
-- These DDL changes freeze the relational shape only; the working closed loop remains file-first.

ALTER TABLE shadow_session_run
    ADD COLUMN IF NOT EXISTS tick_fill_model TEXT,
    ADD COLUMN IF NOT EXISTS time_in_force TEXT;

CREATE INDEX IF NOT EXISTS idx_shadow_session_run_tick_fill_model
    ON shadow_session_run (trade_date, account_id, basket_id, market_replay_mode, tick_fill_model, time_in_force);

ALTER TABLE shadow_session_report
    ADD COLUMN IF NOT EXISTS partially_filled_order_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_shadow_session_report_partial_counts
    ON shadow_session_report (trade_date, account_id, basket_id, partially_filled_order_count);
