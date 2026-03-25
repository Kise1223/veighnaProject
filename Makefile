PYTHON ?= .venv/Scripts/python.exe

.PHONY: bootstrap lint test up

bootstrap:
	$(PYTHON) -m pip install -e .[dev,db]

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m mypy apps gateways libs scripts

test:
	$(PYTHON) -m pytest

up:
	docker compose up -d
