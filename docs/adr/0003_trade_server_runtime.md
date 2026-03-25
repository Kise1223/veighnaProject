# ADR 0003: Trade Server Runtime Contract

- Status: Accepted
- Date: 2026-03-26

## Scope

- `M3` provides the execution-side process bootstrap only.
- It does not add real OpenCTP binary binding, strategy scheduling, or research-side orchestration.

## Runtime Shape

- The process boots one VeighNa `EventEngine`.
- The process boots one VeighNa `MainEngine`.
- `OmsEngine` is treated as mandatory execution-side state and must remain available after bootstrap.
- `OpenCTPSecGateway` is registered through `MainEngine.add_gateway()`.

## Gateway Boot Contract

- `connect()` remains non-blocking and performs initial synchronization asynchronously.
- Contracts, accounts, positions, orders, and trades must enter VeighNa through the standard `on_*` callbacks so `OmsEngine` owns the canonical cache.
- Gateway adapter binding stays mock-first; real SDK wiring must fit the same listener contract.

## Environment Contract

- The process must prepare a project-local `.vntrader/` directory before importing runtime-sensitive VeighNa modules.
- The working directory is the repository root for local runs so VeighNa state stays under versioned project control.
- Health reporting must not require the real gateway to be connected.

## Health Contract

- `/healthz` returns runtime shape and gateway/module state.
- `/readyz` returns `200` only when the runtime is bootstrapped and required modules are not missing.
- Health payloads include module load status, gateway registration state, last sync time, and unfinished order count.

## Optional App Contract

- Optional VeighNa apps are loaded by explicit registry.
- Missing optional apps report `missing_optional` and do not fail bootstrap.
- Required apps report `missing_required` and must make readiness fail until installed.
