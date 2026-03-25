# ADR 0002: Frozen Research-to-Trade Table Shapes

- Status: Accepted
- Date: 2026-03-26

The research side is not implemented in M0-M2, but the hand-off schema is frozen now to avoid a split contract later.

## model_run

- `model_run_id` UUID primary key
- `strategy_run_id` text unique
- `model_name` text
- `model_version` text
- `feature_version` text
- `config_hash` text
- `trade_date` date
- `status` text
- `started_at` timestamptz
- `finished_at` timestamptz

## prediction

- `prediction_id` UUID primary key
- `model_run_id` UUID foreign key
- `instrument_key` text foreign key
- `score` numeric
- `rank` integer
- `exchange_ts` timestamptz
- `received_ts` timestamptz

## approved_target_weight

- `target_weight_id` UUID primary key
- `strategy_run_id` text
- `instrument_key` text foreign key
- `account_id` text
- `basket_id` text
- `trade_date` date
- `target_weight` numeric
- `status` text
- `approved_by` text
- `approved_at` timestamptz

## execution_task

- `execution_task_id` UUID primary key
- `strategy_run_id` text
- `account_id` text
- `basket_id` text
- `trade_date` date
- `exec_style` text
- `status` text
- `created_at` timestamptz

