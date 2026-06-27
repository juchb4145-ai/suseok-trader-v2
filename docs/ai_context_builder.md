# AI Context Builder

## Purpose

PR AI-1 adds a read-only LLM Context Builder for the AI Sidecar. It builds bounded,
redacted, schema-versioned context packets from Event Store and projection state so an operator
can preview or persist the exact packet used by later Sidecar execution.

The Context Builder itself does not call OpenAI, create AI insights, enqueue work, create
`GatewayCommand`, create `OrderIntent`, implement OMS, or feed AI context into Strategy/Risk/OMS
automatic decisions. PR AI-2's runner may consume a context packet as model input, but the packet
and validated insight remain read-only operator-review artifacts.

## Read-only Principles

- Context Builder only reads SQLite projection tables and existing read-only service queries.
- `GET /api/ai-sidecar/context/preview` builds a packet but does not call a model.
- `persist=true` stores the final context packet for audit only.
- Context preview writes no row to `ai_requests` or `ai_insights`.
- PR AI-2 manual execution may write `ai_requests`, and writes `ai_insights` only after valid
  structured output.
- `AI_SIDECAR_ENABLED=false` does not disable preview, because preview has no model call.
- `execution_api_available=true` can be reported by AI-2, but Dashboard still has no execution
  control and preview remains GET/read-only.

## Task Sections

`DAILY_MARKET_BRIEF` includes dashboard safety, gateway status, market-data status, theme snapshot
summary, candidate state counts, strategy status counts, risk status counts, recent event/error
summary, and AI Sidecar status.

`THEME_BRIEF` includes theme detail, latest theme snapshot, top snapshot members, recent snapshot
history, market readiness for top members, related candidates, related strategy observations, and
related risk observations.

`CANDIDATE_BLOCK_RCA` includes candidate detail, candidate context latest, sources, transitions,
latest market tick/readiness, theme context, latest strategy observation and setups, latest risk
observation and checks, related projection/evaluation errors, and dashboard safety warnings.

`NO_TRADE_RCA` includes dashboard safety, pipeline funnel counts, candidate/strategy/risk counts,
top reason codes, recent errors, gateway transport status, and AI Sidecar status. It always warns
that PR AI-1 has no OMS/order path by design.

`TRADE_REVIEW` is intentionally limited. Because OMS/position/trade tables do not exist in PR
AI-1, it returns observation review context and marks `OMS_UNAVAILABLE`,
`TRADE_TABLE_UNAVAILABLE`, and `LIVE_SIM_NOT_ENABLED` as missing sections.

`OPS_INCIDENT_SUMMARY` includes gateway status, rejected/unknown/conflict gateway events when
available, gateway transport status, projection/evaluation errors, and heartbeat timestamps.

`CODEX_PROMPT_DRAFT` includes selected observation summary, docs pointers, recent errors, safety
policy summary, and forbidden-scope summary. It does not generate a prompt body or automate code
changes.

## Redaction Policy

The redactor masks or removes:

- account/account_id/account_no fields
- password, token, api_key, secret, authorization, cookie fields
- `X-Core-Token`, `X-Local-Token`, `TRADING_CORE_TOKEN`, `OPENAI_API_KEY`
- raw headers and raw environment mappings
- local absolute paths such as `C:\Users\...`, `/home/...`, and `/mnt/c/...`
- long account-like numeric strings

It preserves normal market values such as stock code `005930`, prices, volumes, trade value,
candidate IDs, theme IDs, strategy observation IDs, and risk observation IDs.

## Order-context Restriction

`AI_SIDECAR_ALLOW_ORDER_CONTEXT=false` is the default. When disabled, context policy drops
order-like keys or action/tool fields such as:

- `order`, `orders`, `order_intent`, `order_request`
- `gateway_command`
- `send_order`, `cancel_order`, `modify_order`
- `position_size`
- `live_real`, `live_sim`
- `account`

Natural-language safety text such as "order disabled" is allowed. Executable action/tool fields
are not allowed in the context packet.

## Size And Truncation

The builder uses `AI_SIDECAR_MAX_CONTEXT_CHARS` as the final packet limit and
`AI_SIDECAR_CONTEXT_DEFAULT_LIMIT` / `AI_SIDECAR_CONTEXT_MAX_LIMIT` for row caps.

When the packet is too large:

1. Optional sections are summarized first.
2. Lists are compacted and raw/metadata payloads are removed.
3. Required sections are reduced to summary-only if still necessary.
4. `truncated=true` and `CONTEXT_TRUNCATED` are added.

The context hash is calculated from the final redacted/truncated payload and stable packet
metadata, not from `generated_at`.

## Deterministic Hash

`context_hash` is a SHA-256 hash of canonical JSON. The hash material includes task type,
schema version, related entity fields, source sections, missing sections, warnings, flags, and the
final payload. The generated timestamp and context ID are excluded so the same final input produces
the same hash.

## API

- `GET /api/ai-sidecar/context/status`
- `GET /api/ai-sidecar/context/preview`
- `GET /api/ai-sidecar/context/packets`
- `GET /api/ai-sidecar/context/packets/{context_id}`
- `GET /api/ai-sidecar/context/errors`
- `GET /api/ai-sidecar/context/candidate/{candidate_instance_id}`
- `GET /api/ai-sidecar/context/theme/{theme_id}`
- `GET /api/ai-sidecar/context/no-trade/{trade_date}`

All context endpoints are GET. PR AI-2 adds manual execution endpoints separately under
`/api/ai-sidecar/run*`; those endpoints call the runner, not the context preview API.

## Storage

`ai_context_packets` stores the final context packet metadata and payload:

- `context_id`
- `task_type`
- `trade_date`
- `related_entity_type`
- `related_entity_id`
- `context_hash`
- `schema_version`
- `size_chars`
- `max_size_chars`
- `truncated`
- `redaction_applied`
- `order_context_included`
- `missing_sections_json`
- `warnings_json`
- `source_sections_json`
- `payload_json`
- `created_at`

`ai_context_build_errors` stores preview/build failures. Runner context failures are also recorded
in `ai_requests` with `CONTEXT_ERROR`.

## Dashboard Integration

Dashboard snapshot now includes context builder status in the AI Sidecar status section:

- `context_builder_available=true`
- `openai_client_available=false`
- `execution_api_available=true`
- `order_context_allowed=false`
- `tools_enabled=false`
- `order_tools_enabled=false`

Dashboard UI remains display/status only. There is still no AI execution control or POST call from
dashboard JavaScript.

## Explicitly Out Of Scope

- intraday AI worker
- OMS
- `OrderIntent`, `EntryPlan`, `PositionSizing`
- `send_order`, `cancel_order`, `modify_order`
- `POST /api/orders/enqueue`
- GatewayCommand creation/transmission
- automatic buy/sell/order decisions based on context
