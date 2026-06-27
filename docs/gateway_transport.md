# Gateway Transport Surface

PR 2B creates the broker-neutral HTTP and SQLite surface between the future Gateway process and
Core. PR 3 adds a separate mock Gateway process skeleton that exercises this surface before any
real broker runtime is introduced. It does not implement Kiwoom OpenAPI+, PyQt, strategy logic,
risk gates, OMS behavior, OpenAI API calls, or order execution.

## Boundary

- Gateway to Core: `POST /api/gateway/events`
- Core to Gateway: `GET /api/gateway/commands`
- Operator/test read APIs:
  - `GET /api/gateway/status`
  - `GET /api/gateway/events/recent`
  - `GET /api/gateway/commands/status`

The Core accepts only broker-neutral envelope dictionaries from `domain/broker`. Future Kiwoom
or PyQt code must stay outside the Core process.

## Mock Gateway Flow

`python -m apps.mock_gateway --once` performs one deterministic transport pass:

1. Emit `heartbeat`.
2. Emit `price_tick`.
3. Emit `condition_event`.
4. Poll `GET /api/gateway/commands`.
5. Dispatch allowed commands through mock handlers.
6. Emit command lifecycle events back to Core.

Loop mode repeats heartbeat and price ticks on configured intervals while continuing to poll
Core commands. The default loop does not emit `execution_event`.

## Event Ingest

`POST /api/gateway/events` accepts a `GatewayEvent` envelope. Core stores the payload in
canonical JSON form and writes both:

- `raw_events`: durable raw ingest table with `payload_hash` and `duplicate_count`.
- `gateway_events`: transport-facing event table with status and optional error text.

Known payload event types are validated where a broker-neutral payload model already exists:
`price_tick`, `condition_event`, `tr_response`, and `execution_event`. Unknown event types are
stored with `UNKNOWN_EVENT_TYPE` status rather than dropped.

Duplicate policy:

- Same `event_id` and same payload hash: accepted as duplicate and counted.
- Same `event_id` and different payload hash: rejected as conflict.

`heartbeat` updates `gateway_status.last_heartbeat_at`. Every stored event updates
`gateway_status.last_event_received_at`.

## Market Projection After Ingest

PR 4 connects accepted Gateway observations to the read-only Market Data Service. After
`append_gateway_event()` stores a non-duplicate event with `status='ACCEPTED'`, Core projects
these event types:

- `price_tick`: latest tick, tick sample, configured minute bars, VWAP, freshness inputs.
- `condition_event`: append-only condition signal and latest condition state per condition/code.
- `tr_response`: append-only TR row snapshots for market observation.

Duplicate events are not projected again. Events stored as `REJECTED`, `CONFLICT`, or
`UNKNOWN_EVENT_TYPE` are not projected. If projection fails after ingest, Core records the
failure in `market_projection_errors` and leaves order, strategy, risk, and OMS behavior
untouched.

The Gateway event response may include `projection_status` for projected market events:

```json
{
  "accepted": true,
  "event_id": "evt_...",
  "duplicate": false,
  "status": "ACCEPTED",
  "projection_status": "APPLIED"
}
```

The Event Store remains the source of truth. Market projection tables are rebuildable with:

```powershell
python -m tools.rebuild_market_data_projection --clear-projection
```

## Command Polling

`GET /api/gateway/commands` is a long-poll endpoint with:

- `limit`: default `20`, maximum `100`
- `wait_sec`: default `0`, maximum `5`

Polling selects `QUEUED` commands, skips expired commands, marks selected commands as
`DISPATCHED`, increments `attempts`, and records `dispatched_at`.

There is intentionally no public enqueue endpoint in PR 2B. Internal tests and future Core
services can enqueue allowed commands through `storage.gateway_command_store.enqueue_command()`.

## Command Status

Command states are:

- `QUEUED`
- `DISPATCHED`
- `ACKED`
- `REJECTED`
- `FAILED`
- `EXPIRED`
- `CANCELLED`

Gateway events with `event_type` of `command_started`, `command_ack`, or `command_failed` and a
matching `command_id` update `gateway_commands` and add a row to `gateway_command_events`.

PR 3 mock handlers use this lifecycle:

- `heartbeat_request`: `command_started` then `command_ack`.
- `request_tr`: `command_started`, mock `tr_response`, then `command_ack`.
- `register_realtime`: `command_started`, add code to the mock subscription set, then
  `command_ack`.
- `remove_realtime`: `command_started`, remove code from the mock subscription set, then
  `command_ack`.
- `load_conditions`: `command_started`, mock `condition_load_result`, then `command_ack`.
- `send_condition`: `command_started`, mock `condition_event`, then `command_ack`.
- `stop_condition`: `command_started` then `command_ack`.

Example `request_tr` flow:

```text
Core gateway_commands.QUEUED
Gateway polls command and Core marks it DISPATCHED
Gateway posts command_started
Gateway posts tr_response
Gateway posts command_ack
Core marks command ACKED
```

## Token Auth

When `TRADING_CORE_TOKEN` is empty, local development may call Gateway write/poll endpoints
without a token.

When `TRADING_CORE_TOKEN` is set, Gateway write/poll endpoints require either:

- `X-Local-Token: <token>`
- `X-Core-Token: <token>`

`GET /health`, `GET /api/status`, and read-only Gateway status/list endpoints remain token-free.

## Order Command Policy

Order commands remain rejected by default. PR12 adds one narrow exception: `send_order` can be
queued only by the LIVE_SIM service after its safety gate passes and only when the command envelope
and payload prove simulation-only routing.

Still rejected:

- `submit_order`
- `cancel_order`
- `modify_order`
- `enqueue_order`
- `order_intent`
- `gateway_order`
- `live_order`

Allowed only for LIVE_SIM:

- `command_type=send_order`
- `source=live_sim`
- command and payload `idempotency_key`
- `mode=LIVE_SIM` and `live_mode=LIVE_SIM`
- simulation-like `account_mode`, `broker_env`, `server_mode`
- `metadata.live_sim_only=true`
- `metadata.live_real_allowed=false`
- `metadata.live_sim_intent_id`
- BUY side only in PR12

There is still no public enqueue endpoint. `/api/orders/enqueue` is intentionally absent.

PR12 mock Gateway keeps the default rejection policy for malformed or non-LIVE_SIM order commands.
For valid LIVE_SIM mock commands it emits `command_started` and `command_ack` with a mock broker
order number and performs no real broker side effect.
