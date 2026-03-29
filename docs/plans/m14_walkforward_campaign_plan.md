# M14 Walk-Forward Campaign Plan

- Reuse one successful `model_run` across a trade-date window.
- Orchestrate existing `M5-M13` daily artifact producers without re-implementing execution or analytics logic.
- Emit file-first campaign day rows, time-series rows, summaries, and compare artifacts.
- Keep benchmark-relative daily metrics optional and nullable when benchmark is disabled.
