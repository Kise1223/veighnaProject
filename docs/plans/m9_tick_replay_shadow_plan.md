# M9 Tick-Replay Shadow Session Plan

- Extend the existing `M8` shadow session with `market_replay_mode=ticks_l1`.
- Reuse current shadow artifacts, rules engine, cost model, and `M7` ledger/reconcile.
- Add deterministic tick source resolution, tick-source hashing, CLI support, and regression tests.
- Keep `bars_1m` backward compatible and keep the entire path paper-only.
