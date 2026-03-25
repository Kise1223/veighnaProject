param(
    [Parameter(Position = 0)]
    [ValidateSet("bootstrap", "lint", "test", "up")]
    [string]$Command = "test"
)

$ErrorActionPreference = "Stop"
$python = Join-Path $PSScriptRoot "..\\.venv\\Scripts\\python.exe"

if (-not (Test-Path $python)) {
    throw "Expected virtualenv interpreter at $python"
}

switch ($Command) {
    "bootstrap" {
        & $python -m pip install -e ".[dev,db]"
    }
    "lint" {
        & $python -m ruff check .
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        & $python -m mypy apps gateways libs scripts
    }
    "test" {
        & $python -m pytest
    }
    "up" {
        if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
            throw "docker is not available in PATH; install Docker Desktop or skip the up command."
        }
        docker compose up -d
    }
}
