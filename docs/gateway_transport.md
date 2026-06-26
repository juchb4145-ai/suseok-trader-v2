# Gateway Transport Surface

PR 2B creates the broker-neutral HTTP and SQLite surface between the future Gateway process and
Core. It does not implement Kiwoom OpenAPI+, PyQt, a concrete Gateway process, strategy logic,
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

## Token Auth

When `TRADING_CORE_TOKEN` is empty, local development may call Gateway write/poll endpoints
without a token.

When `TRADING_CORE_TOKEN` is set, Gateway write/poll endpoints require either:

- `X-Local-Token: <token>`
- `X-Core-Token: <token>`

`GET /health`, `GET /api/status`, and read-only Gateway status/list endpoints remain token-free.

## Order Command Disabled Policy

Order commands are disabled until later OMS/Risk PRs create a reviewed order path. PR 2B rejects
order-like command types including:

- `send_order`
- `submit_order`
- `cancel_order`
- `modify_order`
- `enqueue_order`
- `order_intent`
- `gateway_order`
- `live_order`

`GET /api/gateway/status` always returns `order_commands_allowed=false` in PR 2B.
