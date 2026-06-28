# External LLM Adapter for AI Candidate Scorer

## 요약

PR-6.5 external LLM adapter는 AI Candidate Scorer에 OpenAI-compatible HTTP provider를 연결하는 선택 기능이다. 역할은 후보 평가 advisory 생성뿐이다.

외부 LLM은 다음만 반환할 수 있다.

- 후보별 `score`
- 후보별 `confidence`
- 후보별 `analysis`
- 후보별 `avoid` reason
- clamp 대상 `risk_reward` suggestion
- summary/no_trade_reason

외부 LLM은 주문, 취소, 청산, risk gate, sizing, kill switch, threshold, 설정 변경, `LIVE_REAL` 판단에 관여하지 않는다.

## 기본 안전값

기본값에서는 외부 호출이 발생하지 않는다.

```env
AI_CANDIDATE_SCORER_ENABLED=false
AI_CANDIDATE_SCORER_PROVIDER=mock
AI_EXTERNAL_LLM_ENABLED=false
AI_EXTERNAL_LLM_PROVIDER=none
AI_EXTERNAL_LLM_ALLOW_NETWORK=false
AI_EXTERNAL_LLM_STORE_REQUEST=false
AI_EXTERNAL_LLM_STORE_RESPONSE=false
AI_EXTERNAL_LLM_REDACT_PROMPT=true
AI_EXTERNAL_LLM_FAIL_OPEN=true
```

외부 호출 조건:

```env
AI_CANDIDATE_SCORER_ENABLED=true
AI_CANDIDATE_SCORER_PROVIDER=external_http
AI_EXTERNAL_LLM_ENABLED=true
AI_EXTERNAL_LLM_PROVIDER=external_http
AI_EXTERNAL_LLM_MODEL=...
AI_EXTERNAL_LLM_API_KEY_ENV=OPENAI_API_KEY
AI_EXTERNAL_LLM_BASE_URL=https://.../v1/chat/completions
AI_EXTERNAL_LLM_ALLOW_NETWORK=true
```

그리고 API/CLI에서 `allow_external=true` 또는 `--allow-external`을 명시해야 한다.

## Provider Interface

```text
score_candidates(context: CandidateScoringContext) -> AiProviderRawResult
```

`AiProviderRawResult` 주요 필드:

- `provider`, `model`, `status`
- `raw_text`, `parsed_json`
- `latency_ms`
- `token_usage`
- `error_message`
- `finish_reason`
- `request_id`
- `external_call_enabled`
- `external_call_attempted`

`services/ai_advisory/providers/mock.py`는 deterministic local fallback이다. `services/ai_advisory/providers/external_http.py`는 OpenAI-compatible chat/completions JSON schema 요청을 만든다.

## Prompt Redaction

Prompt context에서는 다음을 제거한다.

- `account_id`, account number, broker account
- raw Gateway payload
- command payload
- API key, token, secret 계열 key
- local user path 문자열

외부 LLM에 보내는 prompt는 `AI_CANDIDATE_SCORER_MAX_PROMPT_CHARS`를 넘지 않게 축약한다. 축약되면 scoring run에 `prompt_truncated=true`가 저장된다.

## Strict Schema

필수 root field:

```text
selected
analysis
score
confidence
risk_reward
candidate_flags
avoid
summary
no_trade_reason
```

root unknown field는 invalid schema다. `selected`는 후보 code만 허용한다. unknown code는 제거하고 warning으로 남긴다. score/confidence는 0~100만 허용한다.

금지 token:

```text
BUY_NOW
SELL_NOW
SEND_ORDER
CANCEL_ORDER
MODIFY_ORDER
OrderIntent
GatewayCommand
LiveSimIntent
LiveSimOrderRecord
KILL_SWITCH
account_id
quantity override
```

schema 실패는 `ai_advisory_errors`에 저장되고 trading pipeline을 중단하지 않는다.

## Storage

기존 테이블을 재사용한다.

- `ai_candidate_scoring_runs`
- `ai_candidate_scores`
- `ai_risk_reward_suggestions`
- `ai_advisory_errors`

추가 감사 필드:

- `external_call_enabled`
- `external_call_attempted`
- `latency_ms`
- `request_id`
- `token_usage_json`
- `prompt_hash`
- `raw_response_hash`
- `raw_response_stored`
- `prompt_redacted`
- `prompt_truncated`
- `error_category`
- `fallback_provider`

raw response 저장은 기본 false다. 저장하더라도 redaction 이후 저장한다.

## Error / Fallback

| 상황 | category | 동작 |
| --- | --- | --- |
| external disabled | `DISABLED` | 외부 호출 없음 |
| network disabled | `DISABLED_NETWORK` | 외부 호출 없음 |
| API key missing | `CONFIG_ERROR` | 외부 호출 없음 |
| timeout | `TIMEOUT` | retry 후 fail-open |
| HTTP 5xx | `PROVIDER_ERROR` | retry 후 fail-open |
| HTTP 4xx | `CONFIG_OR_REQUEST_ERROR` | fail-open |
| invalid JSON/schema | `INVALID_SCHEMA` | fail-open |

service 기본 동작은 외부 실패를 `ai_advisory_errors`에 남기고 mock fallback advisory를 저장하는 것이다. 이 fallback은 주문 허용이 아니다.

## API

```powershell
POST /api/ai-advisory/score-candidates?provider=mock&dry_run=true
POST /api/ai-advisory/score-candidates?provider=external_http&dry_run=true
POST /api/ai-advisory/score-candidates?provider=external_http&allow_external=true
```

POST는 local token protected다. read-only API는 command를 만들지 않는다.

## CLI

```powershell
python -m tools.score_ai_candidates --dry-run
python -m tools.score_ai_candidates --provider mock
python -m tools.score_ai_candidates --provider external --dry-run
python -m tools.score_ai_candidates --provider external --allow-external
python -m tools.inspect_ai_candidate_scores --latest
python -m tools.inspect_ai_advisory_errors --limit 50
```

CLI 기본은 외부 호출 없음이다.

## Rollback

```powershell
$env:AI_CANDIDATE_SCORER_ENABLED = "false"
$env:AI_CANDIDATE_SCORER_PROVIDER = "mock"
$env:AI_EXTERNAL_LLM_ENABLED = "false"
$env:AI_EXTERNAL_LLM_ALLOW_NETWORK = "false"
```

Rollback은 AI advisory만 끈다. LIVE_SIM/DRY_RUN rule-based pipeline은 독립적으로 계속 동작한다.

## 운영상 주의

- AI score는 주문 승인, risk 승인, 수익 확률이 아니다.
- AI risk_reward는 자동 적용하지 않는다.
- invalid schema, timeout, provider error는 trading pipeline 장애가 아니다.
- Dashboard에는 AI 추천 매수/매도/취소 버튼을 추가하지 않는다.
# No-Buy Sentinel 연동

외부 LLM timeout, invalid schema, provider error는 `AI_UNAVAILABLE`로 분리한다. 이 상태는
시스템 무매수 원인이 아니며, LIVE_SIM safety/config/reconcile block을 덮어쓰지 않는다.
외부 LLM의 risk_reward 제안은 Exit policy에 자동 적용하지 않는다.
