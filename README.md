# suseok-trader-v2

Broker-neutral foundation for a Kiwoom OpenAPI+ based domestic stock trading system.

The project currently contains the Core API bootstrap, settings loading, SQLite initialization,
PR 1 broker contract models, the PR 2A read-only AI Sidecar contract, the PR 2B Event Store
plus Gateway transport surface, the PR 3 mock Gateway process skeleton, the PR 4 read-only
Market Data Service projection, the PR 5 read-only Theme Membership + Theme Snapshot layer,
the PR 6 observe-only Candidate FSM, the PR 7 observe-only Strategy Observation layer, the
PR 8 observe-only Risk Gate layer, the PR 9 read-only Dashboard V1 operator surface, and the
PR AI-1 read-only LLM Context Builder, PR AI-2 optional structured AI execution, PR AI-3
No-trade RCA / Candidate Block RCA reports, PR AI-4 Dashboard AI Explanation Cards, and
PR AI-5 human-copyable Codex Prompt Generator drafts, PR10 DRY_RUN-only OMS simulation, and
PR11 DRY_RUN-only Exit Engine evaluation/simulated close accounting.
It intentionally does not contain Kiwoom or PyQt imports, risk-driven order approval, OMS
broker routing behavior, automatic OpenAI calls, or live order APIs.

## Codex Prompt Generator

PR AI-5 adds a read-only Codex Prompt Generator. It turns RCA reports, candidates, no-trade
context, ops incidents, and PR10 safety-review needs into human-copyable prompt drafts. The
deterministic generator works without an OpenAI API key. Passing `--run-ai` or `run_ai=true`
explicitly may call the PR AI-2 `CODEX_PROMPT_DRAFT` runner, but invalid/unavailable/policy
rejected AI output leaves the deterministic draft intact.

Codex prompt drafts are review artifacts only. They do not call Codex, do not create GitHub
branches, commits, pushes, or PRs, do not modify repository files, do not create `OrderIntent` or
`GatewayCommand`, and do not feed Strategy/Risk/OMS automatic decisions.

API:

- `GET /api/ai-sidecar/codex-prompts/status`
- `POST /api/ai-sidecar/codex-prompts/from-rca/{report_id}?run_ai=false`
- `POST /api/ai-sidecar/codex-prompts/from-candidate/{candidate_instance_id}?run_ai=false`
- `POST /api/ai-sidecar/codex-prompts/from-no-trade/{trade_date}?run_ai=false`
- `POST /api/ai-sidecar/codex-prompts/from-ops-incident`
- `POST /api/ai-sidecar/codex-prompts/safety-review`
- `GET /api/ai-sidecar/codex-prompts`
- `GET /api/ai-sidecar/codex-prompts/{draft_id}`
- `GET /api/ai-sidecar/codex-prompts/{draft_id}/text`
- `GET /api/ai-sidecar/codex-prompts/errors`

CLI:

```powershell
python tools/build_codex_prompt_from_rca.py --report-id ai_rca_report_x
python tools/build_codex_prompt_from_candidate.py --candidate-instance-id candidate-1
python tools/build_codex_prompt_from_no_trade.py --trade-date 2026-06-27
python tools/build_codex_safety_review_prompt.py
python tools/inspect_codex_prompt.py --draft-id ai_codex_prompt_x --text
```

Dashboard shows stored Codex prompt drafts as read-only AI explanation cards. Browser clipboard
copy is allowed; Codex execution, GitHub actions, generation buttons, and apply controls are not.

## OMS + DRY_RUN

PR10 adds an internal DRY_RUN-only OMS simulation. It can store `DryRunIntent`, `DryRunOrder`,
`DryRunExecution`, `DryRunPosition`, and ledger records in SQLite, but it never sends broker
orders and never creates Gateway order commands. Defaults are disabled:

- `DRY_RUN_OMS_ENABLED=false`
- `DRY_RUN_INTENT_CREATION_ENABLED=false`
- `DRY_RUN_SIMULATED_FILL_ENABLED=false`
- `DRY_RUN_ORDER_ROUTING_ENABLED=false`
- `DRY_RUN_GATEWAY_COMMAND_ENABLED=false`

Intent creation requires the PR10 safety gate plus latest `MATCHED_OBSERVATION`, latest
`OBSERVE_PASS`, Candidate `CONTEXT_READY`, fresh latest tick, duplicate checks, limit checks, and
explicit dry-run enablement. Passing those checks creates only an internal simulation record.

DRY_RUN API:

- `GET /api/dry-run/status`
- `GET /api/dry-run/eligibility`
- `GET /api/dry-run/intents`
- `GET /api/dry-run/intents/{dry_run_intent_id}`
- `GET /api/dry-run/orders`
- `GET /api/dry-run/orders/{dry_run_order_id}`
- `GET /api/dry-run/executions`
- `GET /api/dry-run/positions`
- `GET /api/dry-run/ledger`
- `GET /api/dry-run/errors`
- `POST /api/dry-run/evaluate`
- `POST /api/dry-run/intents/from-candidate/{candidate_instance_id}`
- `POST /api/dry-run/orders/from-intent/{dry_run_intent_id}`
- `POST /api/dry-run/orders/{dry_run_order_id}/simulate-fill`
- `POST /api/dry-run/positions/mark-to-market`

POST endpoints require the local token when `TRADING_CORE_TOKEN` is configured and return
`dry_run_only=true`, `live_order_allowed=false`, `gateway_command_allowed=false`, and
`broker_order_sent=false`.

CLI:

```powershell
python tools/evaluate_dry_run_eligibility.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/create_dry_run_intent.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/create_dry_run_order.py --dry-run-intent-id dry_run_intent_x
python tools/simulate_dry_run_fill.py --dry-run-order-id dry_run_order_x
python tools/mark_to_market_dry_run_positions.py
```

Dashboard shows a read-only DRY_RUN panel with status, counts, recent intents/orders, and paper
positions. It has no DRY_RUN execution buttons and does not POST to dry-run endpoints.

See `docs/oms_dry_run.md` for the safety gate, eligibility rules, DB tables, lifecycle, and
forbidden scope. PR12 LIVE_SIM remains a separate future safety-gated project.

## DRY_RUN Exit Engine

PR11 adds deterministic exit evaluation for existing `DryRunPosition` rows. It observes
stop-loss, take-profit, trailing stop, max-hold, stale tick caution, theme weakening, risk
deterioration, strategy invalidation, and manual-review placeholders. An exit signal is not a
broker sell approval. It can only lead to internal `DryRunExitIntent`, `DryRunExitOrder`,
`DryRunExitExecution`, and simulated position close/reduce accounting when explicit local-token
API or CLI calls are made and the PR11 safety gate passes.

Defaults are disabled:

- `DRY_RUN_EXIT_ENGINE_ENABLED=false`
- `DRY_RUN_EXIT_INTENT_CREATION_ENABLED=false`
- `DRY_RUN_EXIT_ORDER_CREATION_ENABLED=false`
- `DRY_RUN_EXIT_SIMULATED_FILL_ENABLED=false`
- `DRY_RUN_EXIT_ORDER_ROUTING_ENABLED=false`
- `DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED=false`
- `DRY_RUN_EXIT_ALLOW_SHORT=false`

Exit API:

- `GET /api/dry-run/exits/status`
- `GET /api/dry-run/exits/evaluations`
- `GET /api/dry-run/exits/evaluations/{exit_evaluation_id}`
- `GET /api/dry-run/exits/signals`
- `GET /api/dry-run/exits/intents`
- `GET /api/dry-run/exits/intents/{dry_run_exit_intent_id}`
- `GET /api/dry-run/exits/orders`
- `GET /api/dry-run/exits/orders/{dry_run_exit_order_id}`
- `GET /api/dry-run/exits/executions`
- `GET /api/dry-run/exits/runs`
- `GET /api/dry-run/exits/errors`
- `POST /api/dry-run/exits/evaluate`
- `POST /api/dry-run/exits/intents/from-position/{dry_run_position_id}`
- `POST /api/dry-run/exits/orders/from-intent/{dry_run_exit_intent_id}`
- `POST /api/dry-run/exits/orders/{dry_run_exit_order_id}/simulate-fill`

Exit CLI:

```powershell
python tools/evaluate_dry_run_exits.py --dry-run-position-id dry_run_position_x
python tools/create_dry_run_exit_intent.py --dry-run-position-id dry_run_position_x
python tools/create_dry_run_exit_order.py --dry-run-exit-intent-id dry_run_exit_intent_x
python tools/simulate_dry_run_exit_fill.py --dry-run-exit-order-id dry_run_exit_order_x
python tools/inspect_dry_run_exit.py --dry-run-position-id dry_run_position_x
```

Dashboard shows a read-only DRY_RUN Exit panel under the DRY_RUN section. It displays evaluation
and signal rows but has no sell, close, fill, cancel, modify, or exit execution buttons. PR11 does
not create `GatewayCommand`, does not send `BrokerOrderRequest`, and does not implement LIVE_SIM or
LIVE_REAL. See `docs/exit_engine_dry_run.md`.

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
- `GET /api/dashboard/ai-explanations`
- `GET /api/dashboard/ai-explanations/status`
- `GET /api/dashboard/ai-explanations/candidate/{candidate_instance_id}`
- `GET /api/dashboard/ai-explanations/no-trade/{trade_date}`

The dashboard shows Safety, System Status, Gateway, Market Data, Theme, Candidate, Strategy,
Risk, Recent Events/Errors, AI Sidecar insight display sections, and PR AI-4 AI Explanation Cards.
The cards present stored RCA reports, AI insights, AI request failures, and AI/context build
warnings with Korean-friendly labels. They use only read-only GET endpoints and do not add order
controls, OMS behavior, Gateway command creation, AI execution, RCA creation, or OpenAI API calls.
`MATCHED_OBSERVATION` remains a setup classifier result, not a buy signal. `OBSERVE_PASS` remains an
observation result, not order approval.

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
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/ai-explanations
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/ai-explanations/status
```

See `docs/dashboard_v1.md` and `docs/dashboard_ai_explanation_cards.md` for the snapshot structure,
UI sections, card labels, runbook, and safety policy.

## AI Sidecar Context + Structured Execution

PR AI-1 adds a bounded, redacted, deterministic context packet builder for the read-only AI
Sidecar. PR AI-2 adds an optional OpenAI Responses API adapter, prompt registry, strict structured
output schema builder, manual runner, and request/insight persistence layer.

The AI Sidecar remains read-only. Valid AI output is stored only as an operator-review insight for
Dashboard, reports, and review workflows. It is not Strategy input, Risk input, OMS input, candidate
state mutation, live-flag mutation, or order intent.

Context endpoints:

- `GET /api/ai-sidecar/context/status`
- `GET /api/ai-sidecar/context/preview?task_type=NO_TRADE_RCA`
- `GET /api/ai-sidecar/context/packets`
- `GET /api/ai-sidecar/context/packets/{context_id}`
- `GET /api/ai-sidecar/context/errors`
- `GET /api/ai-sidecar/context/candidate/{candidate_instance_id}`
- `GET /api/ai-sidecar/context/theme/{theme_id}`
- `GET /api/ai-sidecar/context/no-trade/{trade_date}`

Execution and audit endpoints:

- `GET /api/ai-sidecar/execution/status`
- `POST /api/ai-sidecar/run`
- `POST /api/ai-sidecar/run/candidate/{candidate_instance_id}`
- `POST /api/ai-sidecar/run/no-trade/{trade_date}`
- `POST /api/ai-sidecar/run/theme/{theme_id}`
- `GET /api/ai-sidecar/requests`
- `GET /api/ai-sidecar/requests/{request_id}`
- `GET /api/ai-sidecar/insights`
- `GET /api/ai-sidecar/insights/{insight_id}`

Preview example:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/ai-sidecar/context/preview?task_type=NO_TRADE_RCA&trade_date=2026-06-27"
Invoke-RestMethod "http://127.0.0.1:8000/api/ai-sidecar/context/preview?task_type=CANDIDATE_BLOCK_RCA&related_entity_id=CAND-2026-06-27-005930-1"
```

`persist=true` stores the final context packet in `ai_context_packets` for audit/review. Context
preview is still not AI execution and does not write `ai_requests` or `ai_insights`. Context packets remove
secrets, account-like values, local absolute paths, raw headers/env values, and order/action-like
fields by default. `AI_SIDECAR_ALLOW_ORDER_CONTEXT=false` remains the default.

Manual execution requires `AI_SIDECAR_ENABLED=true`, `AI_SIDECAR_MODEL`, an API key in
`AI_SIDECAR_OPENAI_API_KEY_ENV` (default `OPENAI_API_KEY`), the optional OpenAI SDK, and the local
token when `TRADING_CORE_TOKEN` is configured. API key absence never breaks startup, `/health`,
`/api/status`, context preview, or unit tests. Model calls are tested with `MockAISidecarModelClient`.

Run example:

```powershell
$headers = @{ "X-Local-Token" = "change-me-local-token" }
$body = @{
  task_type = "NO_TRADE_RCA"
  trade_date = "2026-06-27"
  persist_context = $true
} | ConvertTo-Json
Invoke-RestMethod "http://127.0.0.1:8000/api/ai-sidecar/run" -Method POST -Headers $headers -Body $body -ContentType "application/json"
```

Structured outputs use task-specific JSON Schema with `additionalProperties=false`, required
fields, read-only `operator_action` enums, `confidence` range `0..1`, and
`forbidden_actions_confirmed=true`. The runner validates locally after the model response. Invalid
JSON, schema mismatch, timeout, model error, missing API key, and forbidden action output update
`ai_requests` only; `ai_insights` is written only for validated output.

OpenAI tools/function calling, web search, code interpreter, MCP tool integration, and order tools
are disabled. The dashboard AI section can show request/insight status, but it has no AI execution
button and no POST call from `dashboard.js`.

See `docs/ai_context_builder.md`, `docs/ai_openai_client.md`, and
`docs/ai_sidecar_architecture.md` for context, prompt, client, validation, persistence, and safety
details.

## AI RCA Reports

PR AI-3 adds deterministic operator-review RCA reports for the two most common questions:

- why no buy/order happened for a trade date
- why a candidate was blocked or did not progress

RCA reports use the PR AI-1 context builder and can optionally link to PR AI-2 AI insights. They
are report artifacts only. They are not Strategy input, Risk input, OMS input, order approval,
OrderIntent, GatewayCommand, buy/sell recommendation, or live-mode mutation.

RCA endpoints:

- `GET /api/ai-sidecar/rca/status`
- `POST /api/ai-sidecar/rca/no-trade/{trade_date}?run_ai=false`
- `POST /api/ai-sidecar/rca/candidate/{candidate_instance_id}?run_ai=false`
- `POST /api/ai-sidecar/rca/candidates/batch`
- `GET /api/ai-sidecar/rca/reports`
- `GET /api/ai-sidecar/rca/reports/{report_id}`
- `GET /api/ai-sidecar/rca/errors`

POST endpoints require the local token when `TRADING_CORE_TOKEN` is configured. `run_ai=false` is
the default, so deterministic reports work without an OpenAI API key. When `run_ai=true` is
explicitly requested, the workflow calls the AI-2 manual runner only if AI Sidecar execution is
enabled and configured. AI disabled, unavailable, timeout, invalid output, or policy rejection
does not discard the deterministic report; the report links the failed `ai_request_id` and stores
no `ai_insight_id` unless output passed validation.

CLI examples:

```powershell
python tools/build_no_trade_rca.py --trade-date 2026-06-27
python tools/build_candidate_block_rca.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/build_candidate_block_rca_batch.py --trade-date 2026-06-27 --limit 20
```

Add `--run-ai` only for an explicit manual AI run. Dashboard snapshot and
`/api/dashboard/ai-explanations` can show recent RCA reports, AI insights, failed AI requests, and
invalid/error/timeout/policy-rejected states as read-only cards. Dashboard still has no RCA creation
button, no AI execution button, and sends no RCA POST request.

See `docs/ai_rca_workflows.md` for storage tables, root-cause categories, API, CLI, AI linking,
and forbidden scope.

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

## PR12 LIVE_SIM Enablement

PR12 adds a simulation-account-only order path for Kiwoom mock trading acceptance tests. It is not
LIVE_REAL and it does not add real account order submission. LIVE_SIM defaults to disabled and also
requires the kill switch to be off, a simulation account ID, simulation-like account/server/broker
mode, a fresh Gateway heartbeat, Gateway orderability, limits, duplicate checks, and manual local
token protected API or CLI calls.

Required environment flags for a local acceptance run:

```powershell
$env:TRADING_MODE = "LIVE_SIM"
$env:TRADING_ALLOW_LIVE_SIM = "true"
$env:TRADING_ALLOW_LIVE_REAL = "false"
$env:LIVE_SIM_ENABLED = "true"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "true"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "true"
$env:LIVE_SIM_ACCOUNT_ID = "SIM-ACCOUNT-ID"
$env:LIVE_SIM_KILL_SWITCH = "false"
```

Manual flow:

```powershell
python tools/evaluate_live_sim_eligibility.py --candidate-instance-id CANDIDATE_ID
python tools/create_live_sim_intent.py --candidate-instance-id CANDIDATE_ID
python tools/queue_live_sim_order.py --live-sim-intent-id live_sim_intent_x
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python tools/reconcile_live_sim.py
```

LIVE_SIM commands are written only to the Gateway command queue as `send_order` with
`source=live_sim`, `mode=LIVE_SIM`, a required `idempotency_key`, and metadata requiring
`live_sim_only=true` and `live_real_allowed=false`. There is still no `/api/orders/enqueue`, no
cancel/modify endpoint, no background live-sim worker, no Dashboard order button, and no AI/RCA/Codex
output-to-order path.

Read-only API:

- `GET /api/live-sim/status`
- `GET /api/live-sim/intents`
- `GET /api/live-sim/orders`
- `GET /api/live-sim/executions`
- `GET /api/live-sim/rejections`
- `GET /api/live-sim/reconcile`
- `GET /api/live-sim/errors`

Manual local-token protected API:

- `POST /api/live-sim/evaluate`
- `POST /api/live-sim/intents/from-candidate/{candidate_instance_id}`
- `POST /api/live-sim/orders/from-intent/{live_sim_intent_id}`
- `POST /api/live-sim/reconcile`

See `docs/live_sim_enablement.md` for the full safety gate, eligibility, command, reconcile, and
rollback policy.

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
