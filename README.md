# suseok-trader-v2

Broker-neutral foundation for a Kiwoom OpenAPI+ based domestic stock trading system.

The project currently contains the Core API bootstrap, settings loading, SQLite initialization,
PR 1 broker contract models, the PR 2A read-only AI Sidecar contract, the PR 2B Event Store
plus Gateway transport surface, the PR 3 mock Gateway process skeleton, the PR 4 read-only
Market Data Service projection, the PR 5 read-only Theme Membership + Theme Snapshot layer,
the PR 6 observe-only Candidate FSM, the PR 7 observe-only Strategy Observation layer, the
PR 8 observe-only Risk Gate layer, the PR 9 read-only Dashboard V1 operator surface, and the
PR AI-1 read-only LLM Context Builder.
It intentionally does not contain Kiwoom or PyQt imports, risk-driven order approval, OMS
behavior, OpenAI API calls, or live order APIs.

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

## Theme Service

PR 5 adds read-only theme membership and snapshot projections over Market Data Service rows.
Theme membership stores `source_type` so later importers can distinguish `MANUAL`, `IMPORTED`,
`NAVER_REFERENCE`, `CONDITION_DERIVED`, and `MOCK` sources. The bundled sample file is only a
bootstrap/testing seed for verifying the import and snapshot contract; it is not an operator
workflow for maintaining all production theme membership by hand and is not investment advice.

Theme endpoints:

- `GET /api/themes/status`
- `GET /api/themes`
- `GET /api/themes/{theme_id}`
- `GET /api/themes/{theme_id}/members`
- `GET /api/themes/by-code/{code}`
- `GET /api/themes/snapshots/latest`
- `GET /api/themes/{theme_id}/snapshot/latest`
- `GET /api/themes/{theme_id}/snapshots`
- `GET /api/themes/projection-errors`
- `POST /api/themes/import`
- `POST /api/themes/snapshots/rebuild`

Import the sample theme membership:

```powershell
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
```

Rebuild snapshots from the current market data projection:

```powershell
python -m tools.rebuild_theme_snapshots
```

Check the mock-to-theme flow:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots --theme-id semiconductor
Invoke-RestMethod http://127.0.0.1:8000/api/themes/status
Invoke-RestMethod http://127.0.0.1:8000/api/themes/snapshots/latest
```

Theme snapshots aggregate latest ticks, readiness, 1/3/5 minute trade-value deltas, VWAP, and
condition observations. They classify a theme as `DATA_WAIT`, `WATCH`, `SPREADING`, or `LEADING`
with deterministic observation rules. PR 6 Candidate FSM can read these snapshots as source
context, but theme snapshots still do not create strategy decisions, risk decisions, order intents,
or broker commands.

See `docs/theme_service.md` for the schema, score formula, state rules, and API details.

## Candidate FSM

PR 6 adds an observe-only candidate lifecycle layer. A Candidate is an observation episode keyed
by `candidate_instance_id`, not a buy candidate. It can merge condition, theme, market, manual,
and mock sources into deterministic lifecycle states:

- `DETECTED`
- `HYDRATING`
- `WATCHING`
- `CONTEXT_READY`
- `DATA_WAIT`
- `BLOCKED_OBSERVATION`
- `STALE`
- `COOLDOWN`
- `CLOSED`

Candidate endpoints:

- `GET /api/candidates/status`
- `GET /api/candidates`
- `GET /api/candidates/{candidate_instance_id}`
- `GET /api/candidates/{candidate_instance_id}/sources`
- `GET /api/candidates/{candidate_instance_id}/transitions`
- `GET /api/candidates/by-code/{code}`
- `GET /api/candidates/projection-errors`
- `POST /api/candidates/rebuild`

Rebuild candidates from current condition/theme observations:

```powershell
python -m tools.rebuild_candidates
```

Inspect one episode:

```powershell
python -m tools.inspect_candidate --candidate-instance-id CAND-2026-06-27-005930-1 `
  --include-context --include-sources --include-transitions
```

Candidate FSM reads Market Data readiness, latest ticks, bars, VWAP, condition observations, and
Theme Snapshot context. It records source events, source latest state, candidate snapshots,
state transitions, context latest rows, and projection errors. It does not create Strategy scores,
Risk pass/fail decisions, OrderIntent, EntryPlan, PositionSizing, GatewayCommand rows, or order
API calls.

See `docs/candidate_fsm.md` for source types, transition rules, schema, API, and safety scope.

## Strategy Engine Observe-Only

PR 7 adds deterministic setup observation over PR 6 candidates. It reads `CONTEXT_READY`
candidate context, Market Data projection rows, and Theme Snapshot rows, then stores
`StrategyObservation` and `SetupObservation` records. A strategy observation is not an entry
plan, risk approval, position size, or order request. `MATCHED_OBSERVATION` means a setup
classifier matched its observation rule; it is not buy readiness.

Initial setup classifiers:

- `THEME_LEADER_PULLBACK`
- `VWAP_RECLAIM`
- `BREAKOUT_RETEST`
- `THEME_FOLLOWER_EXPANSION`

Strategy endpoints:

- `GET /api/strategy/status`
- `GET /api/strategy/observations/latest`
- `GET /api/strategy/candidates/{candidate_instance_id}`
- `GET /api/strategy/candidates/{candidate_instance_id}/history`
- `GET /api/strategy/observations/{strategy_observation_id}/setups`
- `GET /api/strategy/runs`
- `GET /api/strategy/errors`
- `POST /api/strategy/evaluate`

Evaluate strategy observations from current candidate context:

```powershell
python -m tools.evaluate_strategy --trade-date 2026-06-27
```

Inspect the latest observation for one candidate:

```powershell
python -m tools.inspect_strategy_observation `
  --candidate-instance-id CAND-2026-06-27-005930-1
```

Check the full observe-only flow:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_candidates
python -m tools.rebuild_candidates
python -m tools.evaluate_strategy
Invoke-RestMethod http://127.0.0.1:8000/api/strategy/status
Invoke-RestMethod http://127.0.0.1:8000/api/strategy/observations/latest
```

PR 7 strategy observations are read by PR 8 Risk Gate as observation input only. See
`docs/strategy_engine_observe.md` for the status definitions, setup rules, storage schema, and
API details.

## Risk Gate Observe-Only

PR 8 adds deterministic risk observation over PR 7 strategy observations. It reads Candidate
context, Strategy observations, Market Data projection rows, and Theme Snapshot rows. It stores
`RiskObservation` and `RiskCheckObservation` records for dashboards and operator review.

`OBSERVE_PASS` is not order approval. `OBSERVE_BLOCK` is not an order cancellation or execution
command. The Risk Gate does not create OrderIntent, EntryPlan, PositionSizing, GatewayCommand
rows, OMS state, send/cancel/modify order calls, or live flag changes.

Risk statuses:

- `NOT_EVALUATED`
- `DATA_WAIT`
- `OBSERVE_PASS`
- `OBSERVE_CAUTION`
- `OBSERVE_BLOCK`
- `INVALID_CONTEXT`
- `STALE_CONTEXT`

Risk categories:

- `DATA_QUALITY`
- `MARKET_CONTEXT`
- `THEME_CONTEXT`
- `CANDIDATE_CONTEXT`
- `STRATEGY_CONTEXT`
- `CHASE_OVERHEAT`
- `LIQUIDITY_SPREAD`
- `DUPLICATE_COOLDOWN`
- `PORTFOLIO_PLACEHOLDER`

Risk endpoints:

- `GET /api/risk/status`
- `GET /api/risk/observations/latest`
- `GET /api/risk/candidates/{candidate_instance_id}`
- `GET /api/risk/candidates/{candidate_instance_id}/history`
- `GET /api/risk/observations/{risk_observation_id}`
- `GET /api/risk/observations/{risk_observation_id}/checks`
- `GET /api/risk/runs`
- `GET /api/risk/errors`
- `POST /api/risk/evaluate`

Evaluate risk observations from current strategy observations:

```powershell
python -m tools.evaluate_risk --trade-date 2026-06-27
```

Inspect the latest risk observation for one candidate:

```powershell
python -m tools.inspect_risk_observation `
  --candidate-instance-id CAND-2026-06-27-005930-1
```

Check the full observe-only flow through Risk:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_candidates
python -m tools.rebuild_candidates
python -m tools.evaluate_strategy
python -m tools.evaluate_risk
Invoke-RestMethod http://127.0.0.1:8000/api/risk/status
Invoke-RestMethod http://127.0.0.1:8000/api/risk/observations/latest
```

See `docs/risk_gate_observe.md` for the check rules, storage tables, API details, CLI usage,
and safety scope.

## Dashboard V1

PR 9 adds a read-only operator dashboard for the current observation pipeline:

- `GET /`
- `GET /dashboard`
- `GET /api/dashboard/status`
- `GET /api/dashboard/snapshot`
- `GET /api/dashboard/funnel`
- `GET /api/dashboard/errors`

The dashboard shows Safety, System Status, Gateway, Market Data, Theme, Candidate, Strategy,
Risk, Recent Events/Errors, and AI Sidecar insight display sections. It uses only read-only GET
endpoints and does not add order controls, OMS behavior, Gateway command creation, AI execution,
or OpenAI API calls. `MATCHED_OBSERVATION` remains a setup classifier result, not a buy signal.
`OBSERVE_PASS` remains an observation result, not order approval.

Run Core and open the dashboard:

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

Then visit:

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/dashboard`

Check the mock observe flow and dashboard counts:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_candidates
python -m tools.rebuild_candidates
python -m tools.evaluate_strategy
python -m tools.evaluate_risk
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/snapshot
```

See `docs/dashboard_v1.md` for the snapshot structure, UI sections, runbook, and safety policy.

## AI Sidecar Context Builder

PR AI-1 adds a bounded, redacted, deterministic context packet builder for the read-only AI
Sidecar. It prepares operator-review context before any future model call, but it does not call
OpenAI APIs, create AI insights, create prompts, enqueue work, or connect to Strategy/Risk/OMS
automatic decisions.

Context endpoints:

- `GET /api/ai-sidecar/context/status`
- `GET /api/ai-sidecar/context/preview?task_type=NO_TRADE_RCA`
- `GET /api/ai-sidecar/context/packets`
- `GET /api/ai-sidecar/context/packets/{context_id}`
- `GET /api/ai-sidecar/context/errors`
- `GET /api/ai-sidecar/context/candidate/{candidate_instance_id}`
- `GET /api/ai-sidecar/context/theme/{theme_id}`
- `GET /api/ai-sidecar/context/no-trade/{trade_date}`

Preview example:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/ai-sidecar/context/preview?task_type=NO_TRADE_RCA&trade_date=2026-06-27"
Invoke-RestMethod "http://127.0.0.1:8000/api/ai-sidecar/context/preview?task_type=CANDIDATE_BLOCK_RCA&related_entity_id=CAND-2026-06-27-005930-1"
```

`persist=true` stores the final context packet in `ai_context_packets` for audit/review. It is
still not AI execution and does not write `ai_requests` or `ai_insights`. Context packets remove
secrets, account-like values, local absolute paths, raw headers/env values, and order/action-like
fields by default. `AI_SIDECAR_ALLOW_ORDER_CONTEXT=false` remains the default.

The dashboard AI section now reports context builder status only. It still has no AI execution
button and no OpenAI client.

See `docs/ai_context_builder.md` for task sections, redaction, order-context restriction,
truncation, hash, API, storage, and dashboard details.

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
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_candidates
python -m tools.rebuild_candidates
python -m tools.evaluate_strategy
python -m tools.evaluate_risk
Invoke-RestMethod http://127.0.0.1:8000/api/candidates/status
Invoke-RestMethod http://127.0.0.1:8000/api/strategy/status
Invoke-RestMethod http://127.0.0.1:8000/api/risk/status
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
