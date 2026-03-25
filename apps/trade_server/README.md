# Trade Server

`apps/trade_server` is the M3 bootstrap for the execution-side process.

## Goals

- Start a real `MainEngine`
- Register `OpenCTPSecGateway`
- Load optional VeighNa apps when installed
- Expose health snapshots over HTTP
- Keep the runtime mock-first until the real OpenCTP adapters are wired in
- Attach M4 recorder and replay helpers without mutating gateway or OMS contracts

## Config

Use [config.example.json](C:/Users/whq/PycharmProjects/veighnaProject/apps/trade_server/config.example.json) as the local template.

## Run

```powershell
.\.venv\Scripts\python.exe -m scripts.run_trade_server --config apps/trade_server/config.example.json
```

## Health

- `GET /healthz`
- `GET /readyz`

## Recording And Replay

- Recorder services live under `app/recording/`
- Replay helpers live under `app/replay/`
- They consume `TickData` and standardized parquet files without changing `OpenCTPSecGateway`
