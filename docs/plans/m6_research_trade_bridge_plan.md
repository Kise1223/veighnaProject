# M6 Research-to-Trade Dry-Run Bridge Plan

## Goal

Build the file-first dry-run bridge:

`prediction -> approved_target_weight -> execution_task -> trade_server dry-run ingestion -> order request preview`

## Frozen Scope

- Supported: SSE/SZSE cash equities and ETFs, deterministic sample inputs, local CLI, file-first artifacts, SQL DDL freeze.
- Out of scope: live send-order, signal service, optimizer, risk model, multi-account scheduler, UI.

## Deliverables

- `libs/planning/*`
- `apps/trade_server/app/planning/*`
- `configs/planning/*`
- deterministic execution sample
- `004_execution_bridge.sql`
- CLI, tests, README, ADR, runbook
