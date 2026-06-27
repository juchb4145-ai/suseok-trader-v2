# Dashboard AI Explanation Cards

## 목적

PR AI-4는 Dashboard V1에 AI Sidecar와 RCA 결과를 운영자가 바로 읽을 수 있는 카드로 표시한다.
카드는 저장된 결과를 정규화해 보여주는 read-only display layer이며, Strategy/Risk/OMS 자동 판단
입력이 아니다.

## Read-only 원칙

- Dashboard AI Explanation Cards는 GET 조회만 사용한다.
- Dashboard는 AI run API를 호출하지 않는다.
- Dashboard는 RCA build POST API를 호출하지 않는다.
- Dashboard는 OpenAI API를 직접 호출하지 않는다.
- 카드에는 실행 버튼, RCA 생성 버튼, 주문 관련 버튼이 없다.
- 모든 카드는 `observe_only=true`, `no_trading_side_effects=true`,
  `actions_available=false`, `execution_controls_available=false`를 포함한다.

## 표시 데이터 출처

- `ai_rca_reports`
- `ai_rca_sections`
- `ai_insights`
- `ai_requests`
- `ai_context_build_errors`
- `ai_rca_report_errors`

카드는 이 테이블을 읽기만 한다. 카드 생성 중 금지 action-like output이나 read-only 허용 목록 밖의
`operator_action`이 보이면 `POLICY_WARNING`을 붙이고 실행 액션은 노출하지 않는다.

## 카드 종류

- `NO_TRADE_RCA`: 매수/주문 없음 RCA
- `CANDIDATE_BLOCK_RCA`: 후보 차단 RCA
- `AI_INSIGHT`: AI 인사이트
- `AI_REQUEST_FAILURE`: AI 요청 오류
- `AI_CONTEXT_WARNING`: AI 컨텍스트/RCA build 경고

공통 필드는 `card_id`, `card_type`, `title`, `subtitle`, `status`, `severity`,
`root_cause_category`, `root_cause`, `summary`, `suggested_checks`, `warnings`,
`related_entity_type`, `related_entity_id`, `trade_date`, `generated_at`, `ai_request_id`,
`ai_insight_id`, `rca_report_id`, `context_id`, `source`이다.

## 상태와 심각도 라벨

상태 예시는 `COMPLETED`, `PARTIAL`, `FAILED`, `AI_DISABLED`, `AI_UNAVAILABLE`,
`CLIENT_UNAVAILABLE`, `API_KEY_MISSING`, `TIMEOUT`, `MODEL_ERROR`, `AI_OUTPUT_INVALID`,
`POLICY_REJECTED`, `CONTEXT_ERROR`이다.

심각도는 `INFO`, `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`이다. UI는 상태와 심각도를 한글 label과
badge 색상으로 표시한다.

## Invalid/Error/Timeout 표시 정책

- `API_KEY_MISSING`: API key가 없어 AI insight는 생성되지 않았지만 deterministic RCA report는 유지된다.
- `AI_OUTPUT_INVALID`: 모델 출력이 schema validation을 통과하지 못해 insight로 저장되지 않았다.
- `POLICY_REJECTED`: 금지된 action-like output이 감지되어 insight 저장이 차단되었다.
- `TIMEOUT`/`MODEL_ERROR`/`CLIENT_UNAVAILABLE`: 요청 실패 카드로 표시하고 정상 insight 카드로 승격하지 않는다.
- `CONTEXT_ERROR`: context build error 또는 context load 실패를 경고 카드로 표시한다.

## Deep Link 정책

Candidate 카드는 `related_entity_type=candidate`, `related_entity_id={candidate_instance_id}`를 가진다.
Dashboard API는 `GET /api/dashboard/ai-explanations/candidate/{candidate_instance_id}`로 관련 카드를
조회한다.

No-trade 카드는 `trade_date`를 가진다. Dashboard API는
`GET /api/dashboard/ai-explanations/no-trade/{trade_date}`로 관련 카드를 조회한다.

UI 링크는 anchor/hash 또는 ID 표시만 사용한다. Deep link는 조회 전용이며 report 생성, AI 실행,
state mutation을 하지 않는다.

## API

- `GET /api/dashboard/ai-explanations`
- `GET /api/dashboard/ai-explanations/status`
- `GET /api/dashboard/ai-explanations/candidate/{candidate_instance_id}`
- `GET /api/dashboard/ai-explanations/no-trade/{trade_date}`

필터는 `card_type`, `severity`, `status`, `related_entity_type`, `related_entity_id`, `limit`을
지원한다. 모든 endpoint는 token 없이 기존 Dashboard GET policy를 따른다.

## Dashboard에 버튼이 없는 이유

AI/RCA 결과는 operator review artifact다. 같은 화면에서 실행 controls를 제공하면 실패 상태 표시와
manual execution이 섞여 운영자가 “카드가 자동 판단으로 이어진다”고 오해할 수 있다. PR AI-4는
표시 계층만 추가하며 RCA 생성은 PR AI-3 CLI/API에서 수동으로 수행한다.

## 금지 범위

- Dashboard AI 실행 버튼
- Dashboard RCA 생성 버튼
- Dashboard JavaScript의 AI/RCA POST 호출
- OpenAI tools/function calling
- order tools
- OMS, OrderIntent, GatewayCommand 생성
- Candidate/Strategy/Risk mutation
- LIVE_SIM/LIVE_REAL flag mutation
- AI/RCA output을 자동 trading input으로 사용하는 경로

## 테스트/운영 확인

```powershell
python -m pytest tests/test_dashboard_ai_explanations.py
python -m pytest
python -m ruff check .
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/ai-explanations
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/ai-explanations/status
```

운영 확인 시 실패 request, invalid output, timeout, policy rejection, context error가 카드로 보이는지
확인한다. Dashboard HTML에는 AI 설명 카드 섹션이 있어야 하고, Dashboard JavaScript에는 AI/RCA
생성 POST 호출이 없어야 한다.
