# Roadmap

## 요약

이 로드맵은 `suseok-trader-v2`가 어떤 순서로 안전한 관찰 파이프라인을 쌓아 왔는지 보여준다. PR12까지 진행되었지만, 이것이 `LIVE_REAL` 준비 완료를 뜻하지 않는다. `LIVE_REAL`은 현재 구현되어 있지 않으며 별도 future safety project다.

## 공통 안전 문구

- 기본은 `OBSERVE`다.
- Dashboard는 읽기 전용이며 실행 버튼이 없다.
- `MATCHED_OBSERVATION`은 매수 신호가 아니다.
- `OBSERVE_PASS`는 주문 승인이 아니다.
- `DRY_RUN`은 내부 모의 회계이며 브로커 주문이 아니다.
- `LIVE_SIM`은 모의투자 전용이며 실계좌 주문이 아니다.
- AI/RCA/Codex 결과는 Strategy/Risk/OMS 자동 입력이 아니다.

## 완료된 PR

| PR | 완료 여부 | 한글 설명 | 안전상 의미 |
| --- | --- | --- | --- |
| PR 0 Bootstrap | Done | project layout, FastAPI Core, settings, SQLite 초기화 | `TRADING_MODE=OBSERVE`, live flag disabled 기본값 확립 |
| PR 1 Broker-neutral Contract | Done | `GatewayEvent`, `GatewayCommand`, `BrokerPriceTick`, `BrokerOrderRequest` 등 계약 모델 추가 | 계약은 데이터 모양이다. 실행 경로가 아니다. |
| PR 2A Roadmap Reset + AI Sidecar Read-Only Contract | Done | AI Sidecar를 read-only/review-only 계약으로 제한 | OpenAI 호출이나 주문 연결 없이 안전 정책부터 고정 |
| PR 2B Event Store + Gateway Transport Surface | Done | Gateway event ingest와 command polling surface 추가 | public order enqueue endpoint 없음 |
| PR 3 Mock Gateway + Gateway Adapter Skeleton | Done | mock Gateway와 future Kiwoom Gateway skeleton 추가 | 실제 broker 없이 transport 검증 |
| PR 4 Market Data Service | Done | tick/latest/bar/VWAP/readiness projection 추가 | 시장 데이터는 관찰 데이터이며 주문 판단이 아님 |
| PR 5 Theme Membership + Theme Snapshot | Done | theme membership과 snapshot projection 추가 | theme state는 Candidate source input일 뿐 |
| PR 6 Candidate FSM | Done | observe-only 후보 관찰 에피소드 상태머신 추가 | Candidate는 매수 후보가 아니라 관찰 에피소드 |
| PR 7 Strategy Engine observe-only | Done | deterministic setup observation 추가 | `MATCHED_OBSERVATION`은 classifier 결과 |
| PR 8 Risk Gate observe-only | Done | deterministic risk observation 추가 | `OBSERVE_PASS`는 주문 승인 아님 |
| PR 9 Dashboard V1 | Done | read-only 운영 대시보드 추가 | 화면에서 실행하거나 주문하지 않음 |
| PR AI-1 LLM Context Builder | Done | redacted context packet builder 추가 | 모델 호출 없이 AI 입력자료만 구성 |
| PR AI-2 OpenAI Client + Structured Outputs | Done | optional OpenAI Responses client와 schema validation 추가 | tools/function calling/order tools disabled |
| PR AI-3 No-trade RCA / Candidate Block RCA | Done | deterministic RCA report workflow 추가 | report artifact only |
| PR AI-4 Dashboard AI Explanation Cards | Done | AI/RCA 결과를 Dashboard card로 표시 | display-only, execution control 없음 |
| PR AI-5 Codex Prompt Generator | Done | 사람이 복사하는 Codex prompt draft 생성 | 자동 branch/commit/push/PR 생성 없음 |
| PR 10 OMS + DRY_RUN | Done | 내부 `DryRunIntent`, `DryRunOrder`, `DryRunPosition` 회계 추가 | 브로커 주문과 Gateway command 없음 |
| PR 11 Exit Engine | Done | DRY_RUN position의 simulated close accounting 추가 | 실제 매도 주문 아님 |
| PR 12 LIVE_SIM Enablement | Done | simulation-account-only LIVE_SIM path 추가 | safety-gated `send_order`만 허용, LIVE_REAL 아님 |
| PR AI-6 LIVE_SIM Review Sidecar | Done | LIVE_SIM session/order/reconcile/incident review report 추가 | 장후 복기용 review-only artifact |

## 현재 다음 후보

| 후보 | 설명 | 주의 |
| --- | --- | --- |
| PR13 LIVE_SIM Hardening | LIVE_SIM safety gate와 acceptance runbook 강화 | 모의투자 전용 범위를 유지해야 함 |
| Broker Reconcile Pilot | simulation-account snapshot 정합성 확인 | 실계좌 reconcile과 혼동 금지 |
| Operator Kill Switch Drill | kill switch 절차와 운영 훈련 정리 | 버튼/자동 실행보다 확인 절차가 먼저 |

## Future: LIVE_REAL Safety Project

`LIVE_REAL`은 이 로드맵의 다음 자동 단계가 아니다. 별도 프로젝트로 다음 조건을 독립 검토해야 한다.

- 실계좌 account guard
- 규제/운영 확인 절차
- operator confirmation
- kill switch drill
- broker reconcile
- cancel/modify policy
- failure recovery
- audit trail

PR12 이후에도 바로 실계좌로 가면 안 된다.

## 운영자 체크포인트

- “Done”은 해당 관찰/검토 기능이 구현되었다는 뜻이지 실계좌 준비 완료가 아니다.
- PR7/PR8 결과는 `MATCHED_OBSERVATION`, `OBSERVE_PASS`라는 관측 상태다.
- PR10 `DRY_RUN`과 PR12 `LIVE_SIM`은 서로 다른 안전 경계다.
- `LIVE_REAL`은 현재 미구현이며 별도 future safety project로만 다룬다.
