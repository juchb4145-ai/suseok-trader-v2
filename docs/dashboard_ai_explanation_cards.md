# Dashboard AI Explanation Cards

## 요약

Dashboard AI Explanation Cards는 저장된 AI Sidecar, RCA, Codex Prompt Draft, LIVE_SIM Review 결과를 운영자가 읽기 쉽게 표시하는 read-only display layer다. 카드는 판단/실행이 아니라 표시 데이터다. Strategy/Risk/OMS 자동 입력이 아니다.

## Read-only 원칙

- GET 조회만 사용한다.
- Dashboard는 AI run API를 호출하지 않는다.
- Dashboard는 RCA build POST API를 호출하지 않는다.
- Dashboard는 LIVE_SIM review POST API를 호출하지 않는다.
- Dashboard는 OpenAI API를 직접 호출하지 않는다.
- 카드에는 실행 버튼, RCA 생성 버튼, 주문 관련 버튼이 없다.
- 모든 카드는 `observe_only=true`, `no_trading_side_effects=true`, `actions_available=false`, `execution_controls_available=false`를 포함한다.

## 표시 데이터 출처

- `ai_rca_reports`
- `ai_rca_sections`
- `ai_codex_prompt_drafts`
- `ai_codex_prompt_errors`
- `ai_live_sim_review_reports`
- `ai_live_sim_review_sections`
- `ai_live_sim_review_errors`
- `ai_insights`
- `ai_requests`
- `ai_context_build_errors`
- `ai_rca_report_errors`

카드는 이 table을 읽기만 한다.

## Card Type

| Type | 한글 의미 |
| --- | --- |
| `NO_TRADE_RCA` | 주문 없음 RCA |
| `CANDIDATE_BLOCK_RCA` | 후보 차단 RCA |
| `CODEX_PROMPT_DRAFT` | Codex 프롬프트 초안 |
| `AI_INSIGHT` | AI 인사이트 |
| `AI_REQUEST_FAILURE` | AI 요청 오류 |
| `AI_CONTEXT_WARNING` | AI context/RCA build 경고 |
| `LIVE_SIM_SESSION_REVIEW` | LIVE_SIM 세션 복기 |
| `LIVE_SIM_ORDER_REVIEW` | LIVE_SIM 주문 복기 |
| `LIVE_SIM_RECONCILE_REVIEW` | LIVE_SIM 정합성 확인 복기 |
| `LIVE_SIM_INCIDENT_REVIEW` | LIVE_SIM incident 복기 |

공통 field:

- `card_id`
- `card_type`
- `title`
- `subtitle`
- `status`
- `severity`
- `root_cause_category`
- `root_cause`
- `summary`
- `suggested_checks`
- `warnings`
- `related_entity_type`
- `related_entity_id`
- `trade_date`
- `generated_at`
- `ai_request_id`
- `ai_insight_id`
- `rca_report_id`
- `context_id`
- `source`

LIVE_SIM review card는 `review_only=true`, `order_action_allowed=false`, `gateway_command_allowed=false`, `live_real_allowed=false`를 포함한다.

## Status / Severity

Status 예:

- `COMPLETED`
- `PARTIAL`
- `FAILED`
- `AI_DISABLED`
- `AI_UNAVAILABLE`
- `CLIENT_UNAVAILABLE`
- `API_KEY_MISSING`
- `TIMEOUT`
- `MODEL_ERROR`
- `AI_OUTPUT_INVALID`
- `POLICY_REJECTED`
- `CONTEXT_ERROR`

Severity:

- `INFO`
- `LOW`
- `MEDIUM`
- `HIGH`
- `CRITICAL`

## 실패 상태 해석

| Status | 운영자 해석 |
| --- | --- |
| `API_KEY_MISSING` | API key가 없어 AI insight는 없지만 deterministic report는 유지 |
| `AI_OUTPUT_INVALID` | schema validation 실패로 insight 저장 안 됨 |
| `POLICY_REJECTED` | forbidden action-like output 감지 |
| `TIMEOUT` / `MODEL_ERROR` | 요청 실패 card로 표시 |
| `CONTEXT_ERROR` | context build/load 실패 |

실패 card는 실행 지시가 아니다.

## API

- `GET /api/dashboard/ai-explanations`
- `GET /api/dashboard/ai-explanations/status`
- `GET /api/dashboard/ai-explanations/candidate/{candidate_instance_id}`
- `GET /api/dashboard/ai-explanations/no-trade/{trade_date}`

filter:

- `card_type`
- `severity`
- `status`
- `related_entity_type`
- `related_entity_id`
- `limit`

## Codex Prompt Draft Card

`CODEX_PROMPT_DRAFT` card는 prompt draft summary, acceptance criteria, forbidden scope, test plan, read-only prompt text area를 보여준다. browser clipboard copy button은 가능하지만 Codex 실행, GitHub write, branch, commit, push, PR, automatic apply가 아니다.

## LIVE_SIM Review Card

LIVE_SIM review card는 root cause category, severity, suggested checks, warnings, related order/reconcile ID를 보여준다. review generation, retry, cancel, modify, reconcile command, order execution control은 없다.

## 운영자 체크포인트

- card는 표시 데이터다.
- AI/RCA/Codex/LIVE_SIM review card를 주문 입력으로 쓰지 않는다.
- Dashboard JavaScript가 POST를 보내지 않는지 유지한다.
- `MATCHED_OBSERVATION`과 `OBSERVE_PASS` 관련 card도 실행 의미가 아니다.
