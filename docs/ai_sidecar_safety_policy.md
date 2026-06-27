# AI Sidecar Safety Policy

## 요약

AI Sidecar는 기본 disabled이며 review-only다. OpenAI API key, SDK, model call이 없어도 Core API는 정상 동작해야 한다. 이 문서의 기능은 실계좌 주문을 의미하지 않는다.

## Default Posture

- `AI_SIDECAR_ENABLED=false`가 기본이다.
- Context preview는 모델 호출이 아니다.
- AI execution은 수동 API/CLI에서만 가능하다.
- Dashboard에는 AI 실행 버튼이 없다.
- AI failure는 non-trading failure state로 남아야 한다.

## Forbidden Actions

AI Sidecar는 다음을 하면 안 된다.

- `OrderIntent` 생성
- `GatewayCommand` 생성
- `send_order`, `cancel_order`, `modify_order` 호출 또는 연결
- trading mode 변경
- live flag 변경
- strategy threshold 변경
- risk limit 변경
- position size 변경
- order enqueue 또는 tool-like trading command 생성
- Sidecar output을 Strategy/Risk/OMS 자동 결정에 주입

이 제한은 `OBSERVE`, `LIVE_SIM`, `LIVE_REAL` 명칭이 문서나 설정에 등장하더라도 동일하다. `LIVE_REAL`은 현재 구현되어 있지 않다.

## Allowed Actions

허용되는 output은 read-only operator review artifact다.

- pre-market/post-market summary
- theme brief
- candidate block explanation
- no-trade RCA
- trade review
- ops incident summary
- human-copyable Codex prompt draft
- LIVE_SIM review report

허용 surface는 Dashboard, Report, Operator Review, Codex Prompt Draft다.

## Schema Validation

각 task output은 task-specific schema를 통과해야 한다. schema에는 실행 field가 없어야 한다.

금지 field 예:

- `order_intent`
- `gateway_command`
- `send_order`
- `cancel_order`
- `modify_order`
- tool-like `function_name`
- 실행형 `command`

자연어 안전 설명은 가능하다. 예를 들어 “`send_order`는 금지된다”는 허용된다. 하지만 실행 가능한 tool/action shape는 금지된다.

## Timeout / Error Handling

다음 실패는 Core health/status와 trading state를 변경하지 않는다.

- `AI_DISABLED`
- `API_KEY_MISSING`
- `CLIENT_UNAVAILABLE`
- `TIMEOUT`
- `MODEL_ERROR`
- `AI_OUTPUT_INVALID`
- `POLICY_REJECTED`
- `CONTEXT_ERROR`

실패는 `ai_requests` 등에 기록될 수 있으나 정상 insight나 주문 입력이 아니다.

## API Key Handling

API key는 optional이다. 없으면 `openai_client_available=false`가 되어야 하며 startup, tests, context preview, Dashboard status가 깨지면 안 된다.

## 운영자 체크포인트

- AI가 무언가를 “추천”처럼 설명해도 시스템 실행 권한이 없다.
- `run_ai=true`는 분석 실행 요청이지 주문 요청이 아니다.
- AI/RCA/Codex output은 자동 주문 입력이 아니다.
- 실패 상태를 보고 수동으로 코드나 설정을 바꿀 때도 기존 safety gate를 유지한다.
