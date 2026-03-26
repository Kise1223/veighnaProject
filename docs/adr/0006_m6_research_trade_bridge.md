# ADR 0006: M6 Research-to-Trade Dry-Run Bridge

- Status: Accepted
- Date: 2026-03-26

## Scope

- `M6` delivers a file-first bridge from `prediction` to `approved_target_weight`, `execution_task`, and dry-run order-request previews.
- It explicitly stops before `gateway.send_order`, live OMS placement, or any resident signal service.

## Decision 1: M6 Stops At Dry-Run Order Intents

- `M6` may build `approved_target_weight`, `execution_task`, and dry-run `OrderRequest` previews.
- It must not call `send_order`.
- `trade_server` ingestion is dry-run only and only updates local file-first artifacts plus task status.

## Decision 2: Planning Artifacts Stay File-First

- `approved_target_weight` lives under `data/research/approved_target_weights/`.
- `execution_task` lives under `data/trading/execution_tasks/`.
- `order_intent_preview` and dry-run request previews live under `data/trading/order_intents/`.
- `infra/sql/postgres/004_execution_bridge.sql` freezes relational DDL, but no database is required for the M6 closed loop.

## Decision 3: Deterministic Local Smoke Beats Optimizers

- Target-weight generation is a deterministic long-only baseline: top-K plus equal-weight or simple score normalization.
- Rebalance planning uses existing M1 rules, previous-close-based validation, lot rounding, odd-lot sell splitting, and itemized cost estimation.
- `M6` does not introduce optimizers, risk models, neutralization, or complex execution algorithms.

## Idempotency

- `approved_target_weight` reuse key is `trade_date + prediction_run_id + account_id + basket_id + config_hash`.
- `execution_task` reuse key is `source_target_weight_hash + planner_config_hash + account snapshot hash + positions hash + market snapshot hash`.
- `--force` clears and rebuilds the same deterministic artifact path; silent overwrite is not allowed.
