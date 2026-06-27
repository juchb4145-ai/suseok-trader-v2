# Dashboard V1

## 목적

Dashboard V1은 Gateway → Market Data → Theme → Candidate → Strategy → Risk observe-only
파이프라인을 운영자가 한 화면에서 확인하는 관찰판이다. 핵심 질문은 “지금 무엇이 들어왔고,
어느 단계에서 막혔고, 다음에 무엇을 봐야 하는가”이다.

## Read-only 원칙

- Dashboard는 Core API의 read-only GET endpoint만 사용한다.
- Dashboard API에는 POST endpoint가 없다.
- Dashboard UI에는 주문, 매수, 매도, 취소, 정정, rebuild, evaluate, import 실행 버튼이 없다.
- Dashboard는 `OrderIntent`, OMS, `GatewayCommand`, `send_order`, `cancel_order`,
  `modify_order`를 만들거나 전송하지 않는다.
- Dashboard는 Strategy, Risk, Candidate 상태를 변경하지 않는다.
- Dashboard는 `LIVE_SIM`, `LIVE_REAL` flag를 변경하지 않는다.
- Dashboard는 OpenAI API를 호출하지 않고 AI 실행 POST도 보내지 않는다.

## 화면 구성

- Safety Banner: OBSERVE 모드, live flag, order routing disabled, AI execution disabled,
  read-only 경고를 상단에 고정 표시한다.
- System Status: `/api/status`, Gateway, AI Sidecar, command count, token 필요 여부를 요약한다.
- Pipeline Summary: Gateway Events → Market Ticks → Theme Snapshots → Candidates →
  Strategy Observations → Risk Observations funnel을 표시한다.
- Theme Board: 최신 theme snapshot, state/quality/leader/거래대금/상승 비율을 표시한다.
- Candidate Funnel: candidate state count와 후보 관찰 episode 목록을 표시한다.
- Strategy Observations: latest strategy observation과 setup classifier 결과를 표시한다.
- Risk Notes: latest risk observation, severity, block/caution/pass count, reason code를 표시한다.
- Recent Events / Errors: Gateway recent events와 각 projection/evaluation error를 최근 N개 표시한다.
- AI Sidecar Insight: 저장된 request/insight 상태를 표시하되 실행 컨트롤은 제공하지 않는다.
- AI RCA Reports: PR AI-3부터 최근 RCA report/error를 read-only로 표시할 수 있다.
- AI Explanation Cards: PR AI-4부터 RCA report, AI insight, failed AI request,
  context/RCA build error를 한글 친화적인 카드로 표시한다.

## Snapshot API

`GET /api/dashboard/snapshot`

Query:

- `detail`: `summary` 또는 `full`, 기본값 `summary`
- `limit`: 기본값 `DASHBOARD_SNAPSHOT_DEFAULT_LIMIT`, 최대 `DASHBOARD_MAX_LIMIT`

Top-level sections:

- `generated_at`
- `safety`
- `system`
- `gateway`
- `market_data`
- `themes`
- `candidates`
- `strategy`
- `risk`
- `ai_sidecar`
- `ai_explanations`
- `recent_events`
- `errors`
- `pipeline_summary`

Additional endpoints:

- `GET /api/dashboard/status`
- `GET /api/dashboard/funnel`
- `GET /api/dashboard/errors`
- `GET /api/dashboard/ai-explanations`
- `GET /api/dashboard/ai-explanations/status`
- `GET /api/dashboard/ai-explanations/candidate/{candidate_instance_id}`
- `GET /api/dashboard/ai-explanations/no-trade/{trade_date}`
- `GET /`
- `GET /dashboard`

## Safety Banner 의미

Safety section은 다음 값을 항상 명시한다.

- `trading_mode`
- `live_sim_allowed`
- `live_real_allowed`
- `order_routing_enabled=false`
- `order_controls_available=false`
- `gateway_order_commands_allowed=false`
- `ai_sidecar_enabled`
- `ai_execution_available=false`
- `openai_client_available`
- `observe_only_pipeline=true`

필수 경고 문구:

- 현재 Dashboard는 읽기 전용이며 주문 기능이 없습니다.
- `OBSERVE_PASS`는 주문 승인이 아닙니다.
- `MATCHED_OBSERVATION`은 매수 신호가 아닙니다.

## Strategy와 Risk 주의

`MATCHED_OBSERVATION`은 strategy setup classifier가 관찰 규칙에 맞았다는 뜻이다. 매수 신호,
entry plan, risk approval, position sizing이 아니다.

`OBSERVE_PASS`는 Risk Gate 관찰 결과다. 주문 승인, 주문 전송, 주문 예약, Gateway command 생성이
아니다.

## AI Sidecar 표시 정책

Dashboard는 `/api/ai-sidecar/status` 성격의 상태와 `ai_insights` 저장 row를 보여준다.
PR AI-2부터 Dashboard snapshot은 AI Context Builder status, OpenAI client availability,
recent request counts, recent request rows, recent insight rows, and last error metadata를 표시할 수 있다.
표시 가능한 값은 `context_builder_available=true`, `openai_client_available=false`,
`execution_api_available=true`, `tools_enabled=false`, `order_tools_enabled=false`,
`order_context_allowed=false` 같은 상태 신호다.
AI 실행 버튼, OpenAI client 직접 호출, 자동 판단 연결은 여전히 없다.
AI Sidecar insight는 운영자 참고용 표시 데이터이며 Strategy/Risk/OMS 자동 input으로 쓰지 않는다.

PR AI-3부터 Dashboard snapshot의 `ai_sidecar` section은 `rca_available`, `rca_report_count`,
`latest_rca_reports`, `latest_rca_errors`를 포함할 수 있다. Dashboard는 이 데이터를 표시만 하며
`/api/ai-sidecar/rca/*` POST endpoint를 호출하지 않는다. RCA 실행 버튼도 없다.

PR AI-4부터 Dashboard snapshot은 `ai_explanations` section을 포함한다. 이 section은 저장된
`ai_rca_reports`, `ai_rca_sections`, `ai_insights`, `ai_requests`, `ai_context_build_errors`,
`ai_rca_report_errors`를 카드로 정규화한다. 카드 종류는 `NO_TRADE_RCA`,
`CANDIDATE_BLOCK_RCA`, `AI_INSIGHT`, `AI_REQUEST_FAILURE`, `AI_CONTEXT_WARNING`이며,
`COMPLETED`, `PARTIAL`, `FAILED`, `AI_DISABLED`, `API_KEY_MISSING`, `TIMEOUT`,
`MODEL_ERROR`, `AI_OUTPUT_INVALID`, `POLICY_REJECTED`, `CONTEXT_ERROR` 같은 상태를 표시한다.
모든 카드는 `observe_only=true`, `no_trading_side_effects=true`,
`actions_available=false`, `execution_controls_available=false`를 가진다.

Candidate deep link는 `GET /api/dashboard/ai-explanations/candidate/{candidate_instance_id}`와
hash/ID 표시만 사용한다. No-trade deep link는
`GET /api/dashboard/ai-explanations/no-trade/{trade_date}`와 trade date 표시만 사용한다. 두 경로
모두 read-only 조회이며 report 생성이나 AI 실행을 하지 않는다.

## 금지 범위

- Kiwoom OpenAPI+ 실제 구현 없음
- PyQt5/QAxWidget import 없음
- 32-bit ActiveX 코드 없음
- OMS 구현 없음
- `OrderIntent`, `EntryPlan`, `PositionSizing`, Position/Portfolio 구현 없음
- `send_order`, `cancel_order`, `modify_order` 구현 없음
- `POST /api/orders/enqueue` 없음
- Dashboard에서 Gateway command 생성/전송 없음
- Dashboard에서 rebuild/evaluate/import 실행 버튼 없음
- OpenAI API 직접 호출 없음
- AI Sidecar request/insight status 표시만 가능
- AI RCA report/error read-only 표시만 가능
- AI Explanation Card read-only 표시만 가능
- Dashboard AI 실행 버튼 없음
- Dashboard RCA 실행 버튼 없음
- Dashboard JavaScript의 AI 실행 POST 없음
- Dashboard JavaScript의 RCA 실행 POST 없음
- Dashboard 근거 자동 주문/자동 매수 판단 없음

## Local Runbook

Core API 실행:

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

브라우저:

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/dashboard`

Mock observe flow:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_candidates
python -m tools.rebuild_candidates
python -m tools.evaluate_strategy
python -m tools.evaluate_risk
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/snapshot
```

## Troubleshooting

- Dashboard가 비어 있으면 먼저 `/api/gateway/status`와 `/api/market-data/status`의 count를 확인한다.
- Theme가 비어 있으면 theme membership import와 snapshot rebuild를 실행했는지 확인한다.
- Candidate가 `DATA_WAIT`이면 latest tick, 1m bar, VWAP readiness를 확인한다.
- Strategy가 `DATA_WAIT`이면 Candidate context와 Market Data stale 설정을 확인한다.
- Risk가 비어 있으면 Strategy latest observation이 존재하는지 확인한다.
- AI insight가 비어 있어도 정상이다. AI 실행은 별도 local-token 보호 API에서 수동으로만 수행한다.
