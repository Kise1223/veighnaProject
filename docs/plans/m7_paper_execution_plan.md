# M7 Paper Execution Plan

## Goal

Build the paper-only bridge:

`execution_task -> order_intent_preview -> paper_execution_run -> paper orders / trades -> local ledger -> reconcile report`

## Frozen Scope

- Supported: SSE/SZSE cash equities and ETFs, deterministic local ledger, file-first artifacts, SQL DDL freeze.
- Out of scope: live `send_order`, broker routing, resident signal service, optimizer, GUI, multi-account orchestration.

## Deliverables

- `libs/execution/*`
- `apps/trade_server/app/paper/*`
- `configs/execution/*`
- `005_paper_execution.sql`
- CLI, tests, README, ADR, runbook
