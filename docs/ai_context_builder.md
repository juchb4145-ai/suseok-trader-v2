# AI Context Builder

## 요약

AI Context Builder는 AI에게 넘기기 전 정리된 입력자료인 `AISidecarContextPacket`을 만든다. 모델을 호출하지 않고, insight를 만들지 않고, 주문 관련 command를 만들지 않는다. Context packet과 이후 AI insight는 모두 review-only artifact다.

## Read-only 원칙

- SQLite projection table과 read-only service query만 읽는다.
- `GET /api/ai-sidecar/context/preview`는 packet을 만들지만 model call을 하지 않는다.
- `persist=true`는 audit용 packet 저장만 수행한다.
- context preview는 `ai_requests`, `ai_insights`를 쓰지 않는다.
- `AI_SIDECAR_ENABLED=false`여도 preview는 가능하다.
- Dashboard에는 context build나 AI execution control이 없다.

## Task Sections

| Task | 포함 내용 |
| --- | --- |
| `DAILY_MARKET_BRIEF` | dashboard safety, gateway, market data, theme, candidate, strategy, risk, recent error summary |
| `THEME_BRIEF` | theme detail, latest snapshot, top members, readiness, related candidate/strategy/risk |
| `CANDIDATE_BLOCK_RCA` | candidate detail, context, sources, transitions, market/theme/strategy/risk evidence |
| `NO_TRADE_RCA` | safety, pipeline funnel, reason counts, recent errors, gateway status |
| `TRADE_REVIEW` | observation review context, OMS unavailable marker |
| `OPS_INCIDENT_SUMMARY` | gateway status, rejected/unknown/conflict events, projection/evaluation errors |
| `CODEX_PROMPT_DRAFT` | observation summary, docs pointers, safety policy, forbidden scope |

PR AI-3 RCA workflow는 `NO_TRADE_RCA`, `CANDIDATE_BLOCK_RCA` packet을 deterministic report evidence로 사용한다. report도 review-only다.

## Redaction Policy

redactor가 masking/removal하는 값:

- account/account_id/account_no
- password, token, api_key, secret, authorization, cookie
- `X-Core-Token`, `X-Local-Token`, `TRADING_CORE_TOKEN`, `OPENAI_API_KEY`
- raw headers, raw environment mappings
- local absolute path
- long account-like numeric string

보존되는 값:

- stock code `005930`
- price, volume, trade value
- candidate ID
- theme ID
- strategy observation ID
- risk observation ID

## Order-context Restriction

`AI_SIDECAR_ALLOW_ORDER_CONTEXT=false`가 기본이다. disabled 상태에서는 다음 action/tool-like key를 context packet에서 제거한다.

- `order`
- `orders`
- `order_intent`
- `order_request`
- `gateway_command`
- `send_order`
- `cancel_order`
- `modify_order`
- `position_size`
- `live_real`
- `live_sim`
- `account`

자연어 safety text는 가능하지만 실행 가능한 action field는 허용되지 않는다.

## Size / Truncation

`AI_SIDECAR_MAX_CONTEXT_CHARS`가 최종 packet limit이다.

too large일 때:

1. optional section 요약
2. list compact와 raw/metadata payload 제거
3. required section을 summary-only로 축소
4. `truncated=true`, `CONTEXT_TRUNCATED` 추가

`context_hash`는 redacted/truncated final payload와 stable metadata 기준으로 계산한다. `generated_at`은 hash material에서 제외한다.

## API

- `GET /api/ai-sidecar/context/status`
- `GET /api/ai-sidecar/context/preview`
- `GET /api/ai-sidecar/context/packets`
- `GET /api/ai-sidecar/context/packets/{context_id}`
- `GET /api/ai-sidecar/context/errors`
- `GET /api/ai-sidecar/context/candidate/{candidate_instance_id}`
- `GET /api/ai-sidecar/context/theme/{theme_id}`
- `GET /api/ai-sidecar/context/no-trade/{trade_date}`

모든 context endpoint는 GET이다. AI execution endpoint는 별도의 `/api/ai-sidecar/run*`에 있다.

## Storage

`ai_context_packets`:

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

`ai_context_build_errors`는 preview/build failure를 저장한다.

## 운영자 체크포인트

- context preview는 AI 실행이 아니다.
- `persist=true`는 audit 저장이지 trading action이 아니다.
- order-context restriction이 켜져 있는지 확인한다.
- AI context는 Strategy/Risk/OMS 자동 입력이 아니다.
