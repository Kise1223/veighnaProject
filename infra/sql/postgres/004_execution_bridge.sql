CREATE TABLE approved_target_weight (
    approved_target_weight_id uuid PRIMARY KEY,
    strategy_run_id text NOT NULL,
    prediction_run_id text NOT NULL,
    account_id text NOT NULL,
    basket_id text NOT NULL,
    trade_date date NOT NULL,
    instrument_key text NOT NULL,
    qlib_symbol text NOT NULL,
    score numeric(20, 8) NOT NULL,
    rank integer NOT NULL,
    target_weight numeric(20, 8) NOT NULL CHECK (target_weight >= 0),
    status text NOT NULL,
    approved_by text NOT NULL,
    approved_at timestamptz NOT NULL,
    model_version text NOT NULL,
    feature_set_version text NOT NULL,
    config_hash text NOT NULL,
    source_qlib_export_run_id text,
    source_standard_build_run_id text,
    created_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX approved_target_weight_reuse_idx
    ON approved_target_weight (trade_date, prediction_run_id, account_id, basket_id, config_hash, instrument_key);

CREATE INDEX approved_target_weight_strategy_idx
    ON approved_target_weight (strategy_run_id);

CREATE INDEX approved_target_weight_trade_date_idx
    ON approved_target_weight (trade_date, account_id, basket_id);

CREATE TABLE execution_task (
    execution_task_id uuid PRIMARY KEY,
    strategy_run_id text NOT NULL,
    account_id text NOT NULL,
    basket_id text NOT NULL,
    trade_date date NOT NULL,
    exec_style text NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    source_target_weight_hash text NOT NULL,
    planner_config_hash text NOT NULL,
    plan_only boolean NOT NULL DEFAULT true,
    summary_json jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX execution_task_reuse_idx
    ON execution_task (trade_date, account_id, basket_id, source_target_weight_hash, planner_config_hash);

CREATE INDEX execution_task_strategy_idx
    ON execution_task (strategy_run_id);

CREATE INDEX execution_task_trade_date_idx
    ON execution_task (trade_date, account_id, basket_id);
