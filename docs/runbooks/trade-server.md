# Trade Server Runbook

## Preconditions

- Use `.\.venv\Scripts\python.exe`
- Keep the process working directory at the repository root so `vn.py` resolves `.vntrader/` locally
- Bind real OpenCTP adapters only after the M2 mock flow is already green

## Start

```powershell
.\.venv\Scripts\python.exe -m scripts.run_trade_server --config apps/trade_server/config.example.json --print-health
```

## Expected State

- `engines` contains at least `email`, `log`, `oms`
- `gateway.registered` is `true`
- `modules` shows `loaded`, `disabled`, or `missing_optional`

## Shutdown

- `Ctrl+C` from the foreground process
- Or terminate the process and let the runtime close the gateway and `MainEngine`
