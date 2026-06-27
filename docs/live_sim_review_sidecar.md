# LIVE_SIM Review Sidecar

PR AI-6 adds read-only operator review reports for LIVE_SIM activity. It helps answer why a
simulation order was created, rejected, failed, filled, left unfilled, or mismatched during
local-only reconcile.

## Purpose

- Summarize LIVE_SIM session, order, reconcile, and incident evidence.
- Preserve deterministic review generation without OpenAI.
- Optionally attach PR AI-2 insights when an operator explicitly passes `run_ai=true`.
- Keep all outputs as after-the-fact review artifacts.

## Deterministic Review vs AI Insight

Deterministic review always runs locally from SQLite rows and settings. It creates
`LiveSimReviewReport` plus deterministic sections.

AI insight is optional:

- default is `run_ai=false`
- session/order reviews may use `TRADE_REVIEW`
- session/incident/reconcile reviews may use `OPS_INCIDENT_SUMMARY`
- AI disabled, unavailable, invalid, or policy-rejected output does not discard the report
- invalid/policy-rejected output may link `ai_request_id` but stores no `ai_insight_id`

AI output is never an order input, retry instruction, cancel instruction, modify instruction, or
reconcile command.

## Workflows

- `build_live_sim_session_review(connection, trade_date, run_ai=false)`
- `build_live_sim_order_review(connection, live_sim_order_id, run_ai=false)`
- `build_live_sim_reconcile_review(connection, reconcile_id, run_ai=false)`
- `build_live_sim_incident_review(connection, trade_date=None, related_entity_id=None, run_ai=false)`
- `build_live_sim_order_reviews_for_trade_date(connection, trade_date, limit=50, run_ai=false)`

Sections cover safety/config posture, intents, orders, executions, rejections, reconcile snapshots,
gateway command status, command ack/failure, execution events, errors, idempotency evidence, and
dashboard safety reminders.

## Root Cause Categories

- `SAFETY_GATE`
- `ELIGIBILITY`
- `ORDER_COMMAND_QUEUE`
- `GATEWAY_TRANSPORT`
- `BROKER_ACK`
- `BROKER_REJECTION`
- `EXECUTION_EVENT`
- `PARTIAL_FILL`
- `NO_FILL`
- `RECONCILE_MISMATCH`
- `LOCAL_ONLY_RECONCILE`
- `DUPLICATE_IDEMPOTENCY`
- `LIMIT_GUARD`
- `ACCOUNT_GUARD`
- `CONFIGURATION`
- `AI_EXECUTION`
- `UNKNOWN`

Typical classification examples:

- no activity: `CONFIGURATION`
- safety or eligibility rejection: `SAFETY_GATE`, `ELIGIBILITY`, `LIMIT_GUARD`, `ACCOUNT_GUARD`
- queued/dispatched without ack: `GATEWAY_TRANSPORT` or `BROKER_ACK`
- broker reject: `BROKER_REJECTION`
- acked but no execution: `NO_FILL`
- partial execution: `PARTIAL_FILL`
- reconcile mismatch: `RECONCILE_MISMATCH`
- local-only reconcile without mismatch: `LOCAL_ONLY_RECONCILE`

## Storage Tables

- `ai_live_sim_review_reports`
- `ai_live_sim_review_sections`
- `ai_live_sim_review_links`
- `ai_live_sim_review_errors`

Reports enforce:

- `observe_only=true`
- `review_only=true`
- `no_trading_side_effects=true`
- `live_real_allowed=false`
- `order_action_allowed=false`
- `gateway_command_allowed=false`

## API

- `GET /api/ai-sidecar/live-sim-review/status`
- `POST /api/ai-sidecar/live-sim-review/session/{trade_date}?run_ai=false`
- `POST /api/ai-sidecar/live-sim-review/order/{live_sim_order_id}?run_ai=false`
- `POST /api/ai-sidecar/live-sim-review/reconcile/{reconcile_id}?run_ai=false`
- `POST /api/ai-sidecar/live-sim-review/incident`
- `POST /api/ai-sidecar/live-sim-review/orders/batch`
- `GET /api/ai-sidecar/live-sim-review/reports`
- `GET /api/ai-sidecar/live-sim-review/reports/{review_id}`
- `GET /api/ai-sidecar/live-sim-review/errors`

POST endpoints require the local token when `TRADING_CORE_TOKEN` is configured. They create
reports only and do not call LIVE_SIM order creation, Gateway command queue, or broker APIs.

## CLI

```powershell
python tools/build_live_sim_session_review.py --trade-date 2026-06-27
python tools/build_live_sim_order_review.py --live-sim-order-id live_sim_order_x
python tools/build_live_sim_reconcile_review.py --reconcile-id live_sim_reconcile_x
python tools/build_live_sim_incident_review.py --trade-date 2026-06-27
python tools/build_live_sim_order_review_batch.py --trade-date 2026-06-27 --limit 20
```

Add `--run-ai` only for an explicit manual AI insight attempt.

## Dashboard Display Policy

Dashboard snapshot includes:

- `live_sim.live_sim_review_available`
- `live_sim.live_sim_review_report_count`
- `live_sim.latest_live_sim_review_reports`
- `live_sim.latest_live_sim_review_errors`
- matching `ai_sidecar` summary fields

AI Explanation Cards support:

- `LIVE_SIM_SESSION_REVIEW`
- `LIVE_SIM_ORDER_REVIEW`
- `LIVE_SIM_RECONCILE_REVIEW`
- `LIVE_SIM_INCIDENT_REVIEW`

Dashboard remains read-only. It has no review generation button, AI run button, retry button,
cancel button, modify button, or order execution button. `dashboard.js` does not call LIVE_SIM
review POST endpoints.

## LIVE_REAL Boundary

PR AI-6 is not a LIVE_REAL safety project. It does not enable real account mode, real server mode,
Kiwoom real order calls, real broker order routing, or production readiness.

## Forbidden Scope

- LIVE_REAL implementation
- real account orders
- GatewayCommand creation or enqueue
- order send/cancel/modify calls
- generic `/api/orders/enqueue`
- LIVE_SIM order retry/cancel/modify APIs
- AI output to order creation
- RCA/review output to Strategy/Risk/Candidate/OMS mutation
- Dashboard execution controls
- background automatic review worker
- OpenAI tools/function calling, web search, code interpreter, MCP tools
