# suseok-trader-v2 Architecture

This project starts as a new codebase. The existing `juchb4145-ai/suseok_ai` repository is
used only as a reference for boundary and safety principles. Runtime code, legacy strategy
logic, and Kiwoom/PyQt integration are intentionally not copied into this repository.

## Target Process Split

- 32-bit Kiwoom Gateway: future process that owns Kiwoom OpenAPI+ ActiveX/QAxWidget access.
- 64-bit Core/API/Web Dashboard: future process family that owns state, persistence, risk
  policy, APIs, and operator-facing views.

The Core must not import PyQt5, QAxWidget, or a concrete Kiwoom client. It should depend only
on broker-neutral contracts. The Gateway should capture Kiwoom communication, real-time ticks,
condition search, TR responses, order requests, and execution events, but it must not make
strategy decisions.

## Intended Transport

The future Gateway/Core transport is HTTP-based:

- Gateway to Core: `POST /api/gateway/events`
- Core to Gateway: `GET /api/gateway/commands` using long-polling

PR 0 does not implement these endpoints. They are documented as the intended contract boundary
for later work.

## Safety Defaults

- `TRADING_MODE` defaults to `OBSERVE`.
- `TRADING_ALLOW_LIVE_SIM` defaults to `false`.
- `TRADING_ALLOW_LIVE_REAL` defaults to `false`.
- There is no order, buy, sell, strategy, or Kiwoom execution path in PR 0.
- Future order-related commands must use `command_id`, `request_id`, and `idempotency_key`
  before any live-like path can be considered.

## Project Layout

- `apps/`: executable application entrypoints.
- `api/`: FastAPI route modules and HTTP-facing code.
- `domain/`: broker-neutral domain models and policies.
- `infrastructure/`: external adapters, added only when needed.
- `services/`: application services such as settings.
- `storage/`: database connection and persistence helpers.
- `web/`: future dashboard assets.
- `tests/`: automated tests.
- `docs/`: architecture, contracts, and roadmap.
- `tools/`: local scripts.

## Explicitly Excluded Legacy Scope

This bootstrap does not copy `runtime_factory.py`, legacy runtime code, hybrid strategy code,
dirty/shadow variants, threshold A/B machinery, or any Kiwoom OpenAPI+/PyQt implementation.
Those concepts may inform future trade-off discussions, but they are not part of this new
project foundation.
