ALTER TABLE shadow_session_run
    ADD COLUMN IF NOT EXISTS tick_source_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_shadow_session_run_mode_lookup
    ON shadow_session_run (trade_date, account_id, basket_id, market_replay_mode, execution_task_id);

CREATE INDEX IF NOT EXISTS idx_shadow_session_run_tick_source_hash
    ON shadow_session_run (tick_source_hash);
