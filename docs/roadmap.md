# Roadmap

## 요약

이 로드맵은 `suseok-trader-v2`가 어떤 순서로 안전한 관찰 파이프라인을 쌓아 왔는지 보여준다. PR13까지 진행되었지만, 이것이 `LIVE_REAL` 준비 완료를 뜻하지 않는다. `LIVE_REAL`은 현재 구현되어 있지 않으며 별도 future safety project다.

`codex/structural-audit-completion` 병합 이후의 실제 실행 순서, 승급 기준과 중단 조건은
[FAST Track LIVE_SIM 수익모델 로드맵](fast_track_live_sim_roadmap_2026-07_ko.md)을 source of truth로 사용한다.
Master tracking issue는 GitHub Issue `#7`이다.

## 공통 안전 문구

- 기본은 `OBSERVE`다.
- Dashboard는 읽기 전용이며 실행 버튼이 없다.
- `MATCHED_OBSERVATION`은 매수 신호가 아니다.
- `OBSERVE_PASS`는 주문 승인이 아니다.
- `DRY_RUN`은 내부 모의 회계이며 브로커 주문이 아니다.
- `LIVE_SIM`은 모의투자 전용이며 실계좌 주문이 아니다.
- AI/RCA/Codex 결과는 Strategy/Risk/OMS 자동 입력이 아니다.

## 현재 실행형 로드맵

| 순서 | 단계 | 현재 상태 | 목적 |
| ---: | --- | --- | --- |
| 0 | FAST-0 Post-Merge Qualification | RETIRED_HISTORICAL | 과거 개발단계 evidence는 보존하되 현재 gate에서 재검증하지 않음 |
| 1 | FAST-1 Pure Preview | DONE | 현재 거래일의 시장 입력과 통합 LIVE_SIM 자격을 strict read-only로 계산 |
| 2 | Operational Gate C1 | BLOCKED_BY_CURRENT_MARKET_DATA | 오늘 tick/index/context와 canary가 준비된 뒤 수동 1건 lifecycle 검증 |
| 3 | FAST-2A/2B Alpha Replay + Profit Lab | DONE | 미래 데이터 누수 없는 비용 후 수익성 검증 |
| 4 | FAST-3 Parallel Shadow | DONE | shadow와 모의체결 괴리 측정 |
| 5 | FAST-4 Broker Snapshot Reconcile | NEXT | 키움 모의계좌와 Core local truth 대사 |
| 6 | FAST-5 Automatic Canary | BLOCKED | 일 1~2건 제한 자동 LIVE_SIM |
| 7 | FAST-6 Champion/Challenger | BLOCKED | 검증된 전략 1개와 challenger 운영 |

Append-only 연속 10거래일 evidence는 inline 제거를 위한 장기 병렬 트랙이며,
수동 LIVE_SIM 1건과 Alpha Replay 개발을 자동으로 차단하지 않는다.

FAST-1의 `DONE`은 무기록 Preview 코드와 fixture 검증 완료를 뜻한다. 과거 FAST-0 evidence는
`PASS`로 바꾸지 않고 감사 이력으로만 보존한다. 현재 거래일 tick/index/context가 없거나 canary가
false이면 Operational Gate C1과 주문 실행은 계속 차단한다.

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
| PR 13 Kiwoom Gateway Real Adapter | Done | suseok_ai의 Kiwoom OpenAPI+ client/gateway 자산을 v2 Gateway 계약으로 이식 | 32-bit Gateway에서 실제 조건검색/실시간/TR/LIVE_SIM command를 처리하되 LIVE_REAL 금지 |

## 이전 다음 후보 — 역사적 참고

아래 목록은 FAST Track 문서 도입 이전 후보이며, 신규 작업 순서는 FAST Track 문서가 우선한다.

| 후보 | 설명 | 주의 |
| --- | --- | --- |
| PR14 Market Open OBSERVE Stabilization | 실제 장중 Kiwoom Gateway heartbeat, condition, tick, projection 안정화 | 구조 감사 completion에서 상당 부분 완료됨 |
| PR15 LIVE_SIM Pilot Day Runbook | 모의투자 전용 하루 pilot 절차와 evidence/reconcile 강화 | FAST-1 및 Gate C1로 재정의 |
| Broker Reconcile Pilot | simulation-account snapshot 정합성 확인 | FAST-4로 재정의 |
| Operator Kill Switch Drill | kill switch 절차와 운영 훈련 정리 | 자동 canary 전 필수 운영 항목 |
| No-Buy Recovery Roadmap | 무매수 funnel 계측, LIVE_SIM/DRY_RUN 데드락 해소, profile/admission 정리 | [No-Buy Recovery Roadmap](no_buy_recovery_roadmap_ko.md)를 참고하되 FAST Track gate를 우선 |
| Redesign Roadmap 2026-07 | 파일럿 실가동 기준 재설계 점검: 브로커 reconcile, dead-man cancel, god 모듈 분해, 백테스터 | [재설계 로드맵(2026-07)](redesign_roadmap_2026-07_ko.md)의 구조 부채 항목은 FAST Track과 병행 여부를 별도 판단 |

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
- 신규 FAST 작업은 한 단계의 evidence와 acceptance criteria를 닫은 뒤 다음 단계로 이동한다.
