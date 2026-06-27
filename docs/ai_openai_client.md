# AI OpenAI Client

## 요약

PR AI-2는 read-only AI Sidecar를 위한 optional OpenAI Responses API adapter를 추가한다. PR AI-1 context packet을 읽고 strict structured JSON을 요청한 뒤 local validation을 통과한 insight만 운영자 검토용으로 저장한다. OpenAI client는 trading decision, order intent, gateway command, strategy/risk/candidate mutation, live flag change, OMS action을 만들지 않는다.

## Availability

다음 조건이 모두 true일 때만 available이다.

- `AI_SIDECAR_ENABLED=true`
- `AI_SIDECAR_MODEL` non-empty
- `AI_SIDECAR_OPENAI_API_KEY_ENV`가 가리키는 environment variable 존재
- optional OpenAI SDK import 성공
- `AI_SIDECAR_USE_RESPONSES_API=true`
- `AI_SIDECAR_STRUCTURED_OUTPUTS_ENABLED=true`
- `AI_SIDECAR_TOOLS_ENABLED=false`
- `AI_SIDECAR_ORDER_TOOLS_ENABLED=false`

API key나 SDK가 없어도 startup, `/health`, `/api/status`, context preview, Dashboard snapshot, tests는 깨지면 안 된다.

## Responses API 사용 방식

`OpenAIResponsesClient`가 보내는 것:

- settings의 model name
- prompt registry의 system prompt
- redacted context payload 기반 user prompt
- `text.format` JSON Schema metadata with `strict=true`
- `store=false`

보내지 않는 것:

- tool definitions
- function calls
- web search
- code interpreter
- MCP connection
- order tools

## Structured Outputs

`services/ai_sidecar/output_schema.py`의 schema:

- `daily_market_brief_output_v1`
- `theme_brief_output_v1`
- `candidate_block_rca_output_v1`
- `no_trade_rca_output_v1`
- `trade_review_output_v1`
- `ops_incident_summary_output_v1`
- `codex_prompt_draft_output_v1`

공통 특성:

- `additionalProperties=false`
- required fields
- severity enum
- read-only `operator_action` enum
- `confidence` range `0..1`
- `forbidden_actions_confirmed=true`

`CODEX_PROMPT_DRAFT`는 `prompt_draft`를 추가로 요구한다.

## Run Flow

`run_ai_sidecar_task` 흐름:

1. settings와 task type 검증
2. `ai_requests` row 생성
3. stored context packet load 또는 새 packet build
4. `AI_SIDECAR_ALLOW_ORDER_CONTEXT=false`이면 order context 거부
5. prompt와 output schema metadata build
6. model client call
7. structured output local validation
8. domain policy validation
9. valid output만 `ai_insights` 저장
10. `ai_requests` completed 또는 failed 기록

이 runner는 manual-only다. intraday worker, Dashboard run button, Strategy/Risk/OMS 자동 연결이 없다.

## Failure Policy

실패는 `ai_insights`를 만들지 않는다.

| Status | 의미 |
| --- | --- |
| `AI_DISABLED` | manual execution disabled |
| `API_KEY_MISSING` | API key 없음 |
| `CLIENT_UNAVAILABLE` | SDK/model/config unavailable |
| `TIMEOUT` | request timeout |
| `MODEL_ERROR` | model call error |
| `AI_OUTPUT_INVALID` | invalid JSON 또는 schema mismatch |
| `POLICY_REJECTED` | forbidden action output 또는 blocked order context |
| `CONTEXT_ERROR` | context load/build failure |

OpenAI failure는 Core health/status endpoint에 영향을 주지 않는다.

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

POST endpoint는 `TRADING_CORE_TOKEN` 설정 시 local token이 필요하다. 이것은 분석 실행 endpoint이지 주문 endpoint가 아니다.

## RCA / Codex / LIVE_SIM Review 연결

- RCA workflow는 `run_ai=true` 또는 `--run-ai`가 명시된 경우에만 AI runner를 호출한다.
- Codex Prompt Generator는 `CODEX_PROMPT_DRAFT` task를 optional로 사용할 수 있다.
- LIVE_SIM Review Sidecar는 `TRADE_REVIEW`, `OPS_INCIDENT_SUMMARY`를 optional review enrichment로 사용할 수 있다.

모든 경우 AI output은 review-only다. Strategy input, Risk input, OMS input, LIVE_SIM order input, retry input, cancel/modify input, GatewayCommand input, LIVE_REAL enablement가 아니다.

## Dashboard Policy

Dashboard는 AI request/insight 상태와 AI Explanation Cards를 표시할 수 있다. Dashboard에는 AI run button이 없고 `dashboard.js`는 AI execution POST를 보내지 않는다.

## 운영자 체크포인트

- `run_ai=true`는 주문 요청이 아니다.
- tools/function calling과 order tools가 disabled인지 확인한다.
- invalid/policy-rejected output은 정상 insight가 아니다.
- API key가 없어도 시스템 관찰 기능은 동작해야 한다.
