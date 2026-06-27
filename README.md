# suseok-trader-v2

Broker-neutral foundation for a Kiwoom OpenAPI+ based domestic stock trading system.

The project currently contains the Core API bootstrap, settings loading, SQLite initialization,
PR 1 broker contract models, the PR 2A read-only AI Sidecar contract, the PR 2B Event Store
plus Gateway transport surface, the PR 3 mock Gateway process skeleton, and the PR 4 read-only
Market Data Service projection. It intentionally does not contain Kiwoom or PyQt imports,
strategy decisions, risk policy execution, OMS behavior, OpenAI API calls, or live order APIs.

## Broker Contract

Broker/Core contracts live under `domain/broker`:

- `events.py`: `GatewayEvent` and `EventEnvelope` for Gateway-to-Core observations.
- `commands.py`: `GatewayCommand` and `CommandEnvelope` for Core-to-Gateway instructions.
- `market.py`: `BrokerPriceTick` for normalized real-time market ticks.
- `orders.py`: `BrokerOrderRequest`, `BrokerOrderResult`, and `BrokerExecutionEvent`.
- `tr.py`: `BrokerTrRequest` and `BrokerTrResponse`.
- `conditions.py`: `BrokerConditionEvent`.
- `utils.py`: UTC timestamp generation, message ID generation, dictionary serialization helpers,
  payload normalization, and integer/float parsing helpers.

The contracts serialize through `to_dict()` and reconstruct through `from_dict()`. They preserve
idempotency keys, auto-generate event/command IDs when omitted, normalize broker-style numeric
strings, and reject unsafe values such as invalid stock codes, non-positive quantities, and
invalid limit prices.

See `docs/event_contract.md` for field-level details.

## Gateway Transport Surface

PR 2B adds a broker-neutral HTTP boundary for a future Gateway process:

- `POST /api/gateway/events`: ingest Gateway event envelopes into `raw_events` and
  `gateway_events`.
- `GET /api/gateway/commands`: long-poll queued Core commands and mark them `DISPATCHED`.
- `GET /api/gateway/status`: read transport health, event, and command counters.
- `GET /api/gateway/events/recent`: read recent stored Gateway events.
- `GET /api/gateway/commands/status`: read command status counts.

Gateway command enqueue is intentionally service-only for now. There is no public command
enqueue endpoint and no order endpoint. Order-like command types such as `send_order`,
`submit_order`, `cancel_order`, `modify_order`, and `order_intent` are rejected until a later
OMS/Risk PR creates a reviewed order path.

## Market Data Service

PR 4 adds a read-only projection over accepted Gateway events. The Event Store remains the source
of truth; market data tables can be rebuilt from accepted `price_tick`, `condition_event`, and
`tr_response` rows.

Market data endpoints:

- `GET /api/market-data/status`
- `GET /api/market-data/ticks/latest`
- `GET /api/market-data/ticks/{code}`
- `GET /api/market-data/bars/{code}?interval_sec=60`
- `GET /api/market-data/readiness/{code}`
- `GET /api/market-data/conditions/recent`
- `GET /api/market-data/tr-snapshots/recent`
- `GET /api/market-data/projection-errors`

The projection normalizes latest ticks, tick samples, 1/3/5 minute bars, VWAP, condition signals,
TR market snapshots, freshness/readiness, and projection errors. It is observation-only: it does
not create strategy decisions, risk decisions, order intents, or broker commands.

Rebuild the projection from accepted Event Store rows:

```powershell
python -m tools.rebuild_market_data_projection --clear-projection
```

Omit `--clear-projection` to run a non-destructive replay that skips already projected events.

## Mock Gateway

PR 3 adds a separate `gateway/` package and executable mock Gateway harness. The mock process
calls only Core HTTP endpoints and uses only broker-neutral contracts:

- `gateway/settings.py`: Gateway-only environment settings.
- `gateway/core_client.py`: JSON HTTP client for Core Gateway endpoints.
- `gateway/runtime.py`: shared heartbeat, queue, polling, and snapshot loop.
- `gateway/mock_runtime.py`: deterministic heartbeat, price tick, condition, and command flow.
- `gateway/command_handlers.py`: mock handlers for allowed non-order commands.
- `gateway/event_factory.py`: broker-neutral mock event builders.
- `apps/mock_gateway.py`: CLI entrypoint for transport tests.
- `apps/kiwoom_gateway.py`: safe placeholder for a future 32-bit Gateway entrypoint.

Run Core first:

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

Then run one deterministic mock pass:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
```

Or run the loop:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --interval-sec 1.0
```

Confirm Core received the mock events:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/status
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/events/recent
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/status
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/ticks/005930
Invoke-RestMethod "http://127.0.0.1:8000/api/market-data/bars/005930?interval_sec=60"
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/conditions/recent
```

`apps/kiwoom_gateway.py` is intentionally a skeleton in PR 3. It imports no broker UI/ActiveX
runtime and prints a safe message directing operators to the mock Gateway for transport tests.

## Local Token

Set `TRADING_CORE_TOKEN` to require local token auth for Gateway event ingest and command
polling. Either header is accepted:

```powershell
$env:TRADING_CORE_TOKEN = "change-me-local-token"
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/events `
  -Method Post `
  -Headers @{"X-Core-Token" = "change-me-local-token"} `
  -ContentType "application/json" `
  -Body '{"event_type":"heartbeat","source":"local-gateway","payload":{"status":"ok"}}'
```

If `TRADING_CORE_TOKEN` is empty, local development can call the Gateway write/poll endpoints
without a token. `GET /health`, `GET /api/status`, and read-only status endpoints do not require
the token.

## Local Checks

```powershell
python -m pytest
python -m ruff check .
```
