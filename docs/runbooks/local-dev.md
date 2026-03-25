# Local Development Runbook

## Preconditions

- Use `.\.venv\Scripts\python.exe` for every project command.
- Install dependencies from `pyproject.toml` before running lint or tests.
- Docker is optional for code work; `compose.yaml` is prepared but not required for unit tests.

## Workflow

1. `.\scripts\dev.ps1 bootstrap`
2. `.\scripts\dev.ps1 lint`
3. `.\scripts\dev.ps1 test`
4. `.\scripts\dev.ps1 up` when Docker is available

## Notes

- The gateway layer is mock-first until the real OpenCTP SDK and simulation credentials are available.
- The integration smoke test may require the VeighNa package to be installed in the virtual environment.

