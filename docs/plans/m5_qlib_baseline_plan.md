# M5 Qlib Baseline Plan

## Goal

Build the smallest repeatable research loop on top of the M4 qlib provider:

- qlib init
- baseline dataset
- baseline model training
- `model_run` artifact
- daily inference
- `prediction` artifact

## Frozen Scope

- Independent `apps/research_qlib` runtime
- File-first artifacts under `data/research/`
- Minimal deterministic baseline features
- One stable baseline model
- Lineage back to qlib export and standard build

## Out Of Scope

- `approved_target_weight`
- optimization
- execution planner
- signal service
- trade-server order routing
- hyper-parameter tuning
- ensemble models
