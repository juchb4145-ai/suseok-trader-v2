# Mock Gateway Runbook

PR 3 provides a mock Gateway process for validating Core/Gateway transport without any real
broker runtime, strategy, risk, OMS, order, or AI execution path.

## Start Core

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

Helper:

```powershell
.\tools\start_core.ps1
```

## Run Once

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
```

Helper:

```powershell
.\tools\start_mock_gateway_once.ps1
```

Expected once-mode events:

- `heartbeat`
- `price_tick`
- `condition_event`

## Run Loop

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --interval-sec 1.0
```

Helper:

```powershell
.\tools\start_mock_gateway_loop.ps1
```

Loop mode periodically emits heartbeats and deterministic price ticks, sends one startup
condition event, and keeps polling Core commands.

## Token

If Core is configured with `TRADING_CORE_TOKEN`, pass the same value as `GATEWAY_CORE_TOKEN` or
with `--token`:

```powershell
$env:TRADING_CORE_TOKEN = "change-me-local-token"
$env:GATEWAY_CORE_TOKEN = "change-me-local-token"
python -m apps.mock_gateway --once
```

The Gateway sends the token as `X-Core-Token`.

## Verify Core State

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/status
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/events/recent
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/commands/status
```

`last_heartbeat_at` should be populated after the first successful mock run.

## Command Polling Test

Tests enqueue commands through `storage.gateway_command_store.enqueue_command()`. The mock
Gateway polls `GET /api/gateway/commands`, dispatches allowed command types, and posts
`command_started`, command-specific mock events, and `command_ack` or `command_failed`.

Order-like commands remain disabled. A forbidden command receives `command_failed` and no order
behavior is executed.

## Troubleshooting

- `Core transport failed`: start Core or check `--core-url`.
- `Core HTTP 401`: configure `GATEWAY_CORE_TOKEN` or pass `--token`.
- `Core HTTP 403`: token value differs from `TRADING_CORE_TOKEN`.
- No recent events: confirm Core and Gateway use the same `TRADING_DB_PATH` when inspecting
  local SQLite state directly.
