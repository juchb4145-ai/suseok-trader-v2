# Glossary KO

## 요약

이 문서는 `suseok-trader-v2` 운영자가 용어를 오해하지 않도록 정리한 한글 용어 사전이다. 코드 식별자, API endpoint, DB table, enum, 환경변수 이름은 원문 그대로 유지한다.

## 공통 용어

| 영문 용어 | 한글 표현 | 의미 | 주의사항 |
| --- | --- | --- | --- |
| Gateway | 게이트웨이 | broker와 통신하는 별도 process 경계 | 전략/리스크 판단을 하지 않는다 |
| Core | 코어 | API, 저장소, projection, policy를 담당하는 process | PyQt/QAxWidget/Kiwoom concrete import 금지 |
| Event Store | 이벤트 저장소 | Gateway event 원본과 정규화 event 저장 | source of truth |
| Market Data Service | 시장 데이터 서비스 | tick/latest/bar/VWAP/readiness projection | 주문 판단 없음 |
| Theme Service | 테마 서비스 | theme membership과 snapshot 관리 | 투자 추천 아님 |
| Candidate FSM | 후보 상태머신 | 후보 관찰 에피소드 lifecycle 관리 | Candidate는 매수 후보가 아님 |
| Candidate | 후보 관찰 에피소드 | 특정 trade date/code에 대한 관찰 묶음 | `CONTEXT_READY`도 매수 준비가 아님 |
| Strategy Observation | 전략 관측 결과 | setup classifier 관측 결과 | 주문 경로가 아님 |
| Risk Observation | 리스크 관측 결과 | risk check 관측 결과 | 주문 승인 아님 |
| Dashboard | 운영 대시보드 | read-only 운영 화면 | 실행 버튼 없음 |
| AI Sidecar | AI 보조 분석기 | context, insight, RCA, review 보조 | review-only |
| RCA | 원인 분석 | no-trade/candidate block 등의 원인 report | 주문 입력 아님 |
| Codex Prompt Draft | Codex 작업 프롬프트 초안 | 사람이 복사하는 prompt draft | 자동 branch/commit/push/PR 없음 |
| OMS | 주문관리 시스템 | 현재는 DRY_RUN 내부 회계 중심 | broker routing과 분리 |
| DRY_RUN | 내부 모의 회계 | SQLite ledger/paper position simulation | 브로커 주문 아님 |
| LIVE_SIM | 키움 모의투자 주문 | simulation-account-only safety-gated path | 실계좌 주문 아님 |
| LIVE_REAL | 실계좌 주문 | future safety project | 현재 미구현/금지 |
| GatewayCommand | 게이트웨이 명령 | Core에서 Gateway로 전달되는 command envelope | safety gate 없이는 생성 금지 |
| OrderIntent | 주문 의도 | future/OMS 계층의 의도 모델 | 현재 live order approval 아님 |
| BrokerOrderRequest | 브로커 주문 요청 | broker-neutral payload 모양 | 이것만으로 실행되지 않음 |
| Structured Output | 구조화 출력 | OpenAI 결과를 JSON Schema로 제한하는 방식 | tools/function calling 아님 |
| Read-only | 읽기 전용 | 조회와 표시만 가능 | POST/실행 버튼 없음 |
| Review-only | 검토 전용 | 사람이 참고하는 artifact | 자동 판단 입력 아님 |
| Safety Gate | 안전 게이트 | 위험 조건을 차단하는 정책 검사 | 통과해도 LIVE_REAL 승인 아님 |
| Kill Switch | 긴급 차단 스위치 | LIVE_SIM 등 위험 경로를 즉시 막는 flag | default safe value 유지 |
| Reconcile | 정합성 확인 | local state와 command/broker snapshot 비교 | PR12는 local-only 중심 |

## 상태/Enum

| 원문 | 한글 설명 | 주의사항 |
| --- | --- | --- |
| `OBSERVE` | 관찰 모드 | 기본 posture |
| `CONTEXT_READY` | Strategy 평가 입력 준비 | 매수 준비 아님 |
| `MATCHED_OBSERVATION` | 전략 조건 관측 일치 | 매수 신호 아님 |
| `OBSERVE_PASS` | 리스크 관측상 통과처럼 보임 | 주문 승인 아님 |
| `OBSERVE_CAUTION` | 리스크 주의 관측 | 운영자 검토 note |
| `OBSERVE_BLOCK` | 리스크 block 관측 | 실행 명령이나 주문 취소 아님 |
| `DATA_WAIT` | 필요한 데이터 대기 | 주문 상태 아님 |
| `STALE_CONTEXT` | context freshness 초과 | 데이터 갱신 필요 |
| `INVALID_CONTEXT` | context 불일치 | 데이터/저장 상태 점검 필요 |
| `LEADING` | theme 주도 관측 | 투자 추천 아님 |
| `SPREADING` | theme 확산 관측 | 주문 지시 아님 |
| `DRY_RUN` | 내부 모의 회계 | 브로커 주문 아님 |
| `LIVE_SIM` | 키움 모의투자 전용 | 실계좌 주문 아님 |
| `LIVE_REAL` | 실계좌 주문 | 현재 미구현/금지 |

## 주문 관련 안전 용어

| 원문 | 한글 설명 | 주의사항 |
| --- | --- | --- |
| `send_order` | 주문 전송 command type | LIVE_SIM service safety-gated path 외 금지 |
| `cancel_order` | 주문 취소 command type | LIVE_SIM 미체결 BUY TTL 취소 전용 외 금지 |
| `modify_order` | 주문 정정 command type | 현재 disabled |
| `POST /api/orders/enqueue` | generic order enqueue API | 존재하지 않음 |
| `live_sim_only=true` | LIVE_SIM 전용 metadata | 실계좌 허용이 아님 |
| `live_real_allowed=false` | LIVE_REAL 금지 metadata | 항상 유지해야 하는 안전 신호 |
| `broker_order_sent=false` | 브로커 주문 미전송 | DRY_RUN safety flag |

## AI 관련 용어

| 원문 | 한글 설명 | 주의사항 |
| --- | --- | --- |
| `ai_context_packets` | AI 입력자료 packet 저장 table | 모델 호출 아님 |
| `ai_requests` | AI 실행 시도/실패 기록 | 실패해도 trading side effect 없음 |
| `ai_insights` | validated AI insight 저장 | Strategy/Risk/OMS input 아님 |
| `AI_OUTPUT_INVALID` | schema validation 실패 | 정상 insight 아님 |
| `POLICY_REJECTED` | forbidden action output 차단 | 정상 insight 아님 |
| `CODEX_PROMPT_DRAFT` | Codex prompt draft task | Codex 실행 아님 |

## 운영자 체크포인트

- 원문 식별자는 번역하지 않는다.
- `MATCHED_OBSERVATION`, `OBSERVE_PASS`, `DRY_RUN`, `LIVE_SIM`을 주문 가능 상태로 해석하지 않는다.
- AI/RCA/Codex 용어는 review-only로 이해한다.
- `LIVE_REAL`은 현재 구현되어 있지 않다.
