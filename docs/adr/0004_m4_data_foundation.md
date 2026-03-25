# ADR 0004: M4 Data Foundation

- Status: Accepted
- Date: 2026-03-26

## Scope

- `M4` only covers SSE/SZSE cash equities and ETFs.
- It delivers raw tick recording, standard ETL, minimal corporate actions, adjustment factors, qlib provider export, data quality checks, and replay samples.
- It explicitly does not deliver ClickHouse, large backfill, model training, signal service, or execution planning.

## Decision 1: Parquet-First With PostgreSQL-Style Manifests

- Bulk market data is written to partitioned parquet files first.
- Every recorder, standardization, and export step emits manifest metadata with run identifiers and file hashes.
- Local execution stores manifests as JSON documents, while `infra/sql/postgres/002_market_data.sql` freezes the relational schema for later PostgreSQL deployment.

## Decision 2: Bulk Market Data Stays Out Of PostgreSQL Main Tables

- Raw ticks, standardized ticks, minute bars, and daily bars are not loaded into PostgreSQL primary tables in `M4`.
- PostgreSQL is reserved for recording runs, file manifests, corporate actions, adjustment factors, and DQ reports.
- This keeps the local workflow lightweight and avoids making Docker or ClickHouse a prerequisite for M4 validation.

## Decision 3: Qlib Remains A Consumer, Not A Runtime Dependency

- Qlib is treated as an optional research dependency.
- The trade runtime does not import qlib during bootstrap or gateway startup.
- M4 stops at exporting a provider that `qlib.init(provider_uri=..., region="cn")` can open; it does not start baseline training or any signal service before M5.
