# AI RCA Workflows

## Purpose

PR AI-3 adds read-only report workflows for:

- `NO_TRADE_RCA`: why no buy/order occurred for a trade date.
- `CANDIDATE_BLOCK_RCA`: why a candidate was blocked, stale, closed, data-waiting, or otherwise
  not progressing.

The output is an operator review artifact. It is not a Strategy input, Risk input, OMS input,
OrderIntent, GatewayCommand, buy/sell signal, or live-mode control.

## Deterministic Report vs AI Insight

The deterministic RCA report is always built from local SQLite projections and PR AI-1 context
packets. It works with no OpenAI API key and with `AI_SIDECAR_ENABLED=false`.

An AI insight is optional. When `run_ai=true` is explicitly requested, the workflow calls the PR
AI-2 manual runner for the matching task. A valid structured output creates `ai_insights` and the
RCA report stores `ai_request_id`, `ai_insight_id`, and `ai_summary`.

If AI is disabled, unavailable, invalid, or policy rejected, the deterministic report is still
saved. In those cases the report may link `ai_request_id`, but it does not store `ai_insight_id`.

## No-trade RCA Workflow

`build_no_trade_rca_report(connection, trade_date, run_ai=false)`:

1. Builds and persists a `NO_TRADE_RCA` context packet.
2. Creates deterministic sections for safety posture, gateway transport, market data, theme
   snapshots, candidate funnel, strategy observations, risk observations, projection/evaluation
   errors, and AI execution status.
3. Always includes a `NO_ORDER_PATH_BY_DESIGN` warning because this project still has no OMS,
   OrderIntent, order API, or gateway order command path.
4. Summarizes candidate, strategy, and risk reason codes.
5. Classifies root cause into categories such as `NO_ORDER_PATH_BY_DESIGN`,
   `GATEWAY_TRANSPORT`, `STRATEGY_CONTEXT`, `RISK_CONTEXT`, or projection/evaluation categories.
6. Optionally calls the AI-2 runner only when `run_ai=true`.
7. Saves the report and sections.

## Candidate Block RCA Workflow

`build_candidate_block_rca_report(connection, candidate_instance_id, run_ai=false)`:

1. Builds and persists a `CANDIDATE_BLOCK_RCA` context packet.
2. Reads candidate detail, latest context, source events, transitions, market readiness, theme
   context, latest strategy observation, latest risk observation, and related errors.
3. Creates deterministic sections for candidate state, source attribution, market readiness,
   theme context, strategy observation, risk observation, errors, and safety reminders.
4. Classifies `DATA_WAIT`, `STALE`, `CLOSED`, strategy non-match states, risk blocks, market
   readiness gaps, theme gaps, and projection/evaluation errors.
5. Optionally links a valid AI insight when `run_ai=true`.
6. Saves the report and sections.

## Root-cause Categories

`AIRCARootCauseCategory` values:

- `OBSERVE_ONLY_PIPELINE`
- `NO_ORDER_PATH_BY_DESIGN`
- `DATA_QUALITY`
- `THEME_CONTEXT`
- `CANDIDATE_CONTEXT`
- `STRATEGY_CONTEXT`
- `RISK_CONTEXT`
- `GATEWAY_TRANSPORT`
- `MARKET_DATA_PROJECTION`
- `THEME_PROJECTION`
- `CANDIDATE_PROJECTION`
- `STRATEGY_EVALUATION`
- `RISK_EVALUATION`
- `AI_EXECUTION`
- `UNKNOWN`

Candidate `STALE` is represented as root cause text `STALE_CONTEXT` with category
`CANDIDATE_CONTEXT`. Candidate `CLOSED` is represented as `SOURCE_EXITED` with category
`CANDIDATE_CONTEXT`.

## Report Storage

PR AI-3 adds additive SQLite tables:

- `ai_rca_reports`
- `ai_rca_sections`
- `ai_rca_report_links`
- `ai_rca_report_errors`

Reports reject `observe_only=false` or `no_trading_side_effects=false`. Sections store
deterministic evidence, reason codes, and source refs. Links connect reports to candidate/context,
AI request, and AI insight rows when those exist.

## Report API

- `GET /api/ai-sidecar/rca/status`
- `POST /api/ai-sidecar/rca/no-trade/{trade_date}?run_ai=false`
- `POST /api/ai-sidecar/rca/candidate/{candidate_instance_id}?run_ai=false`
- `POST /api/ai-sidecar/rca/candidates/batch`
- `GET /api/ai-sidecar/rca/reports`
- `GET /api/ai-sidecar/rca/reports/{report_id}`
- `GET /api/ai-sidecar/rca/errors`

POST endpoints are report creation endpoints, not trading endpoints. They require the local token
when `TRADING_CORE_TOKEN` is configured.

## CLI

```powershell
python tools/build_no_trade_rca.py --trade-date 2026-06-27
python tools/build_candidate_block_rca.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/build_candidate_block_rca_batch.py --trade-date 2026-06-27 --limit 20
```

Add `--run-ai` only when a manual AI run is explicitly desired.

## run_ai Policy

- Default is `run_ai=false`.
- API callers must explicitly pass `run_ai=true`.
- CLI callers must explicitly pass `--run-ai`.
- Dashboard does not call RCA POST endpoints.
- `run_ai=false` never calls OpenAI or the model client.
- `run_ai=true` still requires AI Sidecar enablement, model config, API key availability, and
  local token protection on API POST routes.
- AI failures do not break deterministic report creation.

## AI Disabled/Unavailable Behavior

When the AI-2 runner returns `AI_DISABLED`, `API_KEY_MISSING`, `CLIENT_UNAVAILABLE`, `TIMEOUT`,
`MODEL_ERROR`, `AI_OUTPUT_INVALID`, or `POLICY_REJECTED`, the report remains available for review.
Only valid structured output creates `ai_insights`. Invalid or rejected output is failure metadata,
not a normal insight.

## Dashboard Minimal Integration

Dashboard snapshot includes:

- `rca_available=true`
- `rca_report_count`
- `latest_rca_reports`
- `latest_rca_errors`
- `ai_explanations.latest_cards`
- `ai_explanations.status_counts`
- `ai_explanations.severity_counts`

PR AI-4 turns recent RCA reports, linked AI insights, failed AI requests, and RCA/context errors
into Dashboard AI Explanation Cards. Dashboard displays these cards read-only through
`GET /api/dashboard/snapshot` and `GET /api/dashboard/ai-explanations*`.

RCA report creation remains manual through the PR AI-3 CLI/API. Dashboard has no RCA execution
button, no RCA creation button, and no RCA POST call.

## Codex Prompt Generator Linkage

PR AI-5 can use an RCA report as a Codex prompt source. The prompt generator reads
`ai_rca_reports`, `ai_rca_sections`, links, related context packets, and linked AI insights to
create a human-copyable prompt draft.

This does not change the RCA contract. RCA reports and Codex prompt drafts are still operator
review artifacts only. They are not Strategy input, Risk input, OMS input, OrderIntent,
GatewayCommand, buy/sell signals, live-mode controls, or automatic code-change triggers.

## Forbidden Scope

PR AI-3 does not implement OMS, `OrderIntent`, `EntryPlan`, `PositionSizing`,
`send_order`, `cancel_order`, `modify_order`, `POST /api/orders/enqueue`, GatewayCommand creation,
OpenAI tools/function calling, web search, code interpreter, MCP tool servers, order tools,
background AI workers, Dashboard RCA execution controls, candidate/strategy/risk mutation from AI,
live flag mutation from AI, or automated trading decisions from RCA reports.
