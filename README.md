# suseok-trader-v2

Broker-neutral foundation for a Kiwoom OpenAPI+ based domestic stock trading system.

The project currently contains the Core API bootstrap, settings loading, SQLite initialization,
PR 1 broker contract models, the PR 2A read-only AI Sidecar contract, and the PR 2B Event Store
plus Gateway transport surface. It intentionally does not contain Kiwoom or PyQt imports,
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
