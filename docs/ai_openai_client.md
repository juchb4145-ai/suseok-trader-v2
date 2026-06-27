# AI OpenAI Client

## Purpose

PR AI-2 adds an optional OpenAI Responses API adapter for the read-only AI Sidecar. It consumes
PR AI-1 context packets, builds task prompts, requests strict structured JSON, validates locally,
and stores validated insights for operator review.

The client does not create trading decisions, order intents, gateway commands, strategy mutations,
risk mutations, candidate mutations, live-flag changes, or OMS actions.

## Availability

The OpenAI client is available only when all of these are true:

- `AI_SIDECAR_ENABLED=true`
- `AI_SIDECAR_MODEL` is non-empty
- the environment variable named by `AI_SIDECAR_OPENAI_API_KEY_ENV` exists
- the optional OpenAI SDK import succeeds
- `AI_SIDECAR_USE_RESPONSES_API=true`
- `AI_SIDECAR_STRUCTURED_OUTPUTS_ENABLED=true`
- `AI_SIDECAR_TOOLS_ENABLED=false`
- `AI_SIDECAR_ORDER_TOOLS_ENABLED=false`

API key or SDK absence does not break startup, `/health`, `/api/status`, context preview, dashboard
snapshot, or tests. Unit tests use `MockAISidecarModelClient`.

## Responses API

`OpenAIResponsesClient` uses the Responses API and sends:

- model name from settings
- system prompt from the prompt registry
- user prompt built from the redacted context payload
- `text.format` JSON Schema metadata with `strict=true`
- `store=false`

The adapter does not send tool definitions, function calls, web search, code interpreter, MCP
connections, or order tools.

## Structured Outputs

`services/ai_sidecar/output_schema.py` provides task schemas:

- `daily_market_brief_output_v1`
- `theme_brief_output_v1`
- `candidate_block_rca_output_v1`
- `no_trade_rca_output_v1`
- `trade_review_output_v1`
- `ops_incident_summary_output_v1`
- `codex_prompt_draft_output_v1`

Every schema is an object with `additionalProperties=false`, required fields, severity enum,
read-only `operator_action` enum, `confidence` range `0..1`, and
`forbidden_actions_confirmed=true`. `CODEX_PROMPT_DRAFT` additionally requires `prompt_draft`.

Local validation runs after the model response. Domain policy validation then rejects forbidden
trading/action shapes before any insight is saved.

PR AI-5 uses `CODEX_PROMPT_DRAFT` only as optional AI-assisted prompt drafting. The default Codex
Prompt Generator path is deterministic. When `run_ai=true` is explicitly requested, the runner may
produce a `prompt_draft`, but the PR AI-5 sanitizer rejects automatic Codex execution, GitHub
branch/commit/push/PR creation, order execution, and live-flag changes before an insight is saved.

## Prompt Registry

`services/ai_sidecar/prompt_registry.py` owns task prompts. Prompts require read-only analysis,
schema-only JSON output, no tools/function calling, no order/account/live-mode changes, and allowed
operator actions only:

- `WATCH_ONLY`
- `REVIEW_ONLY`
- `CHECK_DATA`
- `CHECK_PIPELINE`
- `CHECK_POLICY`
- `NO_ACTION`

The user prompt uses the context packet's redacted payload and stable metadata. It does not expose
raw headers, env values, local paths, secrets, or bypass fields.

## Run Flow

Manual execution uses `run_ai_sidecar_task`:

1. Validate settings and task type.
2. Create an `ai_requests` row.
3. Load a stored context packet or build a new PR AI-1 packet.
4. Reject order context when `AI_SIDECAR_ALLOW_ORDER_CONTEXT=false`.
5. Build prompts and output schema metadata.
6. Call the model client.
7. Validate structured output locally.
8. Re-run AI Sidecar domain policy checks.
9. Save `ai_insights` only for valid output.
10. Mark `ai_requests` completed or failed.

The runner is manual-only. There is no intraday worker, dashboard run button, or automatic
Strategy/Risk/OMS connection.

## Persistence

`ai_requests` records every attempt and failure. AI-2 adds:

- `context_id`
- `output_schema_name`
- `latency_ms`
- `input_chars`
- `output_chars`
- `validation_error`
- `raw_response_json`
- `metadata_json`

`ai_insights` is written only after valid structured output and policy validation. It stores
summary, root cause, severity, operator action, and `output_json`.

## Failure Policy

Failures do not create `ai_insights`.

- disabled manual execution: `AI_DISABLED`
- missing API key: `API_KEY_MISSING`
- unavailable SDK/model/config: `CLIENT_UNAVAILABLE`
- timeout: `TIMEOUT`
- model error: `MODEL_ERROR`
- schema mismatch or invalid JSON shape: `AI_OUTPUT_INVALID`
- forbidden action output or blocked order context: `POLICY_REJECTED`
- context load/build failure: `CONTEXT_ERROR`

OpenAI failures do not affect Core health/status endpoints.

## Manual API

- `GET /api/ai-sidecar/execution/status`
- `POST /api/ai-sidecar/run`
- `POST /api/ai-sidecar/run/candidate/{candidate_instance_id}`
- `POST /api/ai-sidecar/run/no-trade/{trade_date}`
- `POST /api/ai-sidecar/run/theme/{theme_id}`
- `GET /api/ai-sidecar/requests`
- `GET /api/ai-sidecar/requests/{request_id}`
- `GET /api/ai-sidecar/insights`
- `GET /api/ai-sidecar/insights/{insight_id}`

POST endpoints require the local token when `TRADING_CORE_TOKEN` is configured.

## RCA Workflow Linkage

PR AI-3 RCA workflows may call `run_ai_sidecar_task` manually when an operator explicitly passes
`run_ai=true` or CLI `--run-ai`. The deterministic RCA report is built and saved regardless of
AI availability.

When the runner returns a valid insight, the RCA report stores `ai_request_id`, `ai_insight_id`,
and a short AI summary. When the runner returns disabled/unavailable/invalid/policy failure, the
RCA report stores failure status and may link `ai_request_id`, but no `ai_insight_id` is recorded.

AI output remains report context only. It is not Strategy input, Risk input, Candidate mutation,
OMS input, order approval, OrderIntent, GatewayCommand, live-flag mutation, or automated trading
decision input.

## Codex Prompt Generator Linkage

PR AI-5 may call `run_ai_sidecar_task` with `CODEX_PROMPT_DRAFT` only when an operator explicitly
passes `run_ai=true` or CLI `--run-ai`. `run_ai=false` never calls OpenAI. Valid AI output can
augment a deterministic prompt draft; invalid, unavailable, disabled, or policy-rejected output
leaves the deterministic draft intact and does not create a normal insight for rejected output.

Tools/function calling, web search, code interpreter, MCP connections, and order tools remain
disabled for this task.

## Dashboard Policy

Dashboard can display AI Sidecar status, request counts, recent request rows, recent insight rows,
last error metadata, and PR AI-4 AI Explanation Cards for stored insights and failed requests.
Dashboard has no AI run button and `dashboard.js` sends no AI execution POST request.

PR AI-4 does not run models from Dashboard. It only reads persisted `ai_requests` and `ai_insights`
and maps statuses such as `API_KEY_MISSING`, `TIMEOUT`, `MODEL_ERROR`, `AI_OUTPUT_INVALID`, and
`POLICY_REJECTED` into operator-readable cards.

## Disabled Scope

PR AI-2 explicitly does not implement OpenAI tools/function calling, web search, code interpreter,
MCP tool servers, order tools, OMS, order enqueue APIs, order intents, entry plans, position sizing,
gateway command creation, candidate mutation, strategy mutation, risk mutation, live flag mutation,
or automatic trading decisions.
