# AGENTS

## Interpreter

- Always use `.\.venv\Scripts\python.exe`.

## Default Quality Gates

```powershell
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m mypy apps gateways libs scripts
.\.venv\Scripts\python.exe -m pytest -q
```

## Project Rules

- For research-related tasks, install first: `.\.venv\Scripts\python.exe -m pip install -e ".[research]"`.
- Current status: `M0-M11` is complete; `M7` stops at one-shot paper execution and local ledger, `M8` stops at bar-driven paper-only shadow execution plus final ledger snapshots, `M9` extends that shadow path to deterministic tick-replay paper execution, `M10` adds deterministic L1 top-of-book constrained partial fills plus simple `DAY/IOC` semantics while remaining paper-only, and `M11` adds file-first execution analytics / TCA for `M7-M10` artifacts without changing execution boundaries.
- Do not enter live order placement or call `send_order` unless the user explicitly asks for it.
- Keep `research` and trade runtime decoupled.
- `pyqlib` must not enter the trade runtime startup path.
- `instrument_key` is the persistent primary key; do not use `vt_symbol` as a database primary key.
- Time semantics are fixed to `Asia/Shanghai`; preserve both `exchange_ts` and `received_ts`.
- Keep artifacts file-first; do not add PostgreSQL write paths unless explicitly requested.
- When behavior changes, update `README`, ADRs, runbooks, and tests together.
- Do not commit runtime artifacts such as `data/research`, `data/trading`, `data/raw`, `data/standard`, or `data/qlib_bin`.
- Build the smallest closed loop first; do not plan `M12` ahead unless asked.
