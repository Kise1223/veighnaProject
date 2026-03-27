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
- Current status: `M0-M8` is complete; `M7` stops at one-shot paper execution and local ledger, and `M8` stops at replay-driven paper-only shadow execution plus final ledger snapshots.
- Do not enter live order placement or call `send_order` unless the user explicitly asks for it.
- Keep `research` and trade runtime decoupled.
- `pyqlib` must not enter the trade runtime startup path.
- `instrument_key` is the persistent primary key; do not use `vt_symbol` as a database primary key.
- Time semantics are fixed to `Asia/Shanghai`; preserve both `exchange_ts` and `received_ts`.
- Keep artifacts file-first; do not add PostgreSQL write paths unless explicitly requested.
- When behavior changes, update `README`, ADRs, runbooks, and tests together.
- Do not commit runtime artifacts such as `data/research`, `data/trading`, `data/raw`, `data/standard`, or `data/qlib_bin`.
- Build the smallest closed loop first; do not plan `M9` ahead unless asked.
