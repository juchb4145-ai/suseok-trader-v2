# Roadmap

## PR 0: Bootstrap

- Create the project layout.
- Add Python project configuration, lint/test settings, and environment examples.
- Add FastAPI Core API with `GET /health` and `GET /api/status`.
- Add settings defaults with `OBSERVE` mode and disabled live flags.
- Add SQLite initialization with WAL, `busy_timeout`, `synchronous=NORMAL`, and `app_metadata`.
- Document architecture boundaries, event contract direction, and excluded legacy scope.

## PR 1: Broker-Neutral Contracts

- Add typed contract models for `GatewayEvent`, `GatewayCommand`, `BrokerPriceTick`,
  `BrokerOrderRequest`, and `BrokerExecutionEvent`.
- Add serialization tests and idempotency field validation.
- Keep contracts free of Kiwoom/PyQt runtime imports.

## PR 2: Gateway Transport Surface

- Add `POST /api/gateway/events` and `GET /api/gateway/commands`.
- Store inbound events and command state in SQLite.
- Add command idempotency and duplicate suppression tests.

## PR 3: Gateway Adapter Skeleton

- Add a separate 32-bit Gateway package skeleton.
- Keep Kiwoom OpenAPI+ and PyQt dependencies isolated from Core requirements.
- Implement heartbeat and mock event posting before any broker action.

## PR 4: Operator Dashboard Foundation

- Add read-only status and event views.
- Keep the default operating posture observable and non-trading.

## PR 5: Risk and Live Enablement Design

- Design explicit risk guards and operator enablement flags.
- Define LIVE_SIM and LIVE_REAL acceptance criteria.
- Do not create active order paths until the risk design is reviewed and tested.
