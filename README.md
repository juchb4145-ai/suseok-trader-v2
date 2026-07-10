# suseok-trader-v2

Kiwoom OpenAPI+ 기반 국내 주식 자동매매 시스템을 위한 **관찰 우선 Core/Gateway 기반**입니다.

이 저장소의 현재 목적은 주문을 자동으로 내는 것이 아니라, Gateway에서 들어온 시장 관측값을 Core에 저장하고 Market Data, Theme, Candidate, Strategy, Risk, Dashboard, AI Sidecar, DRY_RUN, LIVE_SIM 단계로 안전하게 분리해 운영자가 상태를 이해할 수 있게 만드는 것입니다.

## 요약

- 기본 자세는 `OBSERVE`입니다.
- Dashboard는 **Read-only(읽기 전용)** 화면입니다. 실행 버튼이 없습니다.
- `MATCHED_OBSERVATION`은 **매수 신호가 아닙니다**.
- `OBSERVE_PASS`는 **주문 승인이 아닙니다**.
- `DRY_RUN`은 내부 모의 회계입니다. 브로커 주문이 아닙니다.
- `LIVE_SIM`은 키움 모의투자 전용입니다. 실계좌 주문이 아닙니다.
- `LIVE_REAL`은 현재 구현되어 있지 않으며 별도 future safety project입니다.
- AI/RCA/Codex 결과는 Strategy/Risk/OMS 자동 입력이 아닙니다.

## 현재 지원 범위

| 영역 | 현재 상태 | 운영자 관점 의미 |
| --- | --- | --- |
| Core API | 지원 | FastAPI 기반 상태, 조회, projection API를 제공합니다. |
| Gateway Transport | 지원 | `POST /api/gateway/events`, `GET /api/gateway/commands`로 Core와 Gateway를 분리합니다. |
| Mock Gateway | 지원 | 실제 Kiwoom 없이 heartbeat, price tick, condition event 흐름을 검증합니다. |
| Real Kiwoom Gateway Adapter | 지원 | 32-bit Python process에서 Kiwoom login, 조건검색, 실시간 tick, TR, LIVE_SIM command를 Core 계약으로 변환합니다. |
| Event Store | 지원 | Gateway event를 원본과 정규화 형태로 저장합니다. |
| Market Data Service | 지원 | exchange-aware tick/latest/bar/VWAP/readiness projection을 제공합니다. |
| NXT 관찰 지원 | opt-in 관찰 | NXT tick 분리 저장, premarket snapshot, KRX/NXT cross-exchange 관측을 제공한다. 관찰 데이터 자체는 주문 라우팅이나 매수 신호가 아니다. |
| Theme Service | 지원 | theme membership과 snapshot을 관찰 데이터로 만듭니다. |
| Candidate FSM | 지원 | 후보 관찰 에피소드를 상태머신으로 추적합니다. |
| Strategy Engine | observe-only | setup 관측 결과를 저장합니다. 주문 판단이 아닙니다. |
| Risk Gate | observe-only | 리스크 관측 결과를 저장합니다. 주문 승인이 아닙니다. |
| Dashboard V1 | read-only | 어디서 막혔는지 보는 운영 화면입니다. |
| AI Sidecar / RCA / Codex Draft | review-only | 분석과 프롬프트 초안 표시용입니다. 자동 실행 입력이 아닙니다. |
| DRY_RUN OMS | 내부 모의 회계 | `DryRunIntent`, `DryRunOrder`, `DryRunPosition`을 내부 시뮬레이션으로 기록합니다. |
| DRY_RUN Exit Engine | 내부 모의 청산 회계 | paper position의 simulated close accounting만 수행합니다. |
| LIVE_SIM | 모의투자 전용 | safety gate를 통과한 run_once 경로에서 키움 모의투자용 BUY, close-only SELL exit, 미체결 BUY cancel command를 제한적으로 queue합니다. |
| LIVE_SIM NXT/SOR 라우팅 준비 | preflight gated | 기본은 `KRX`이며 `NXT`/`SOR`는 `LIVE_SIM_NXT_SUPPORT_CONFIRMED=true` 전까지 preflight와 safety gate에서 차단됩니다. |

## 아직 지원하지 않는 것

- `LIVE_REAL` 실계좌 주문
- 실계좌 잔고/보유/체결 기반 운영
- Dashboard 주문 버튼
- generic `POST /api/orders/enqueue`
- generic `cancel_order` / `modify_order` 경로. 단, `cancel_order`는 LIVE_SIM 미체결 BUY TTL 취소 전용 예외만 존재합니다.
- Strategy/Risk 결과에서 바로 주문으로 이어지는 경로
- AI/RCA/Codex 결과를 주문 입력으로 쓰는 경로
- Core 내부의 PyQt5, QAxWidget, Kiwoom OpenAPI+ concrete client import

## 전체 아키텍처

```text
Gateway
  -> Event Store
  -> Market Data Service
  -> Theme Service
  -> Candidate FSM
  -> Strategy Observation
  -> Risk Observation
  -> Dashboard
  -> AI Sidecar / RCA / Codex Prompt Draft
  -> DRY_RUN OMS
  -> DRY_RUN Exit Engine
  -> LIVE_SIM
```

### 프로세스 경계

| 프로세스 | 담당 | 금지 |
| --- | --- | --- |
| Gateway | broker 통신, 이벤트 수집, command polling | 전략 판단, 리스크 판단, 임의 주문 생성 |
| Core/API/Dashboard | 상태 저장, projection, 정책, read-only 화면 | PyQt/QAxWidget/Kiwoom concrete import |
| AI Sidecar | 분석 보조, RCA, 리뷰, 프롬프트 초안 | Strategy/Risk/OMS 자동 입력, 주문 도구 |

자세한 구조는 [docs/architecture.md](docs/architecture.md)와 [docs/operator_overview_ko.md](docs/operator_overview_ko.md)를 참고하세요.

## 안전 원칙

| 원칙 | 설명 |
| --- | --- |
| 기본은 `OBSERVE` | 관찰과 저장이 기본이며 주문 실행이 기본값이 아닙니다. |
| Dashboard는 read-only | 화면은 조회만 합니다. 실행 버튼이 없습니다. |
| AI Sidecar는 review-only | AI 결과는 운영자 참고 자료입니다. |
| Strategy/Risk는 observation | `MATCHED_OBSERVATION`, `OBSERVE_PASS`는 주문 경로가 아닙니다. |
| DRY_RUN은 broker와 분리 | 내부 ledger와 paper position만 다룹니다. |
| LIVE_SIM은 모의투자 전용 | `LIVE_REAL`과 분리된 simulation-account-only 경로입니다. |
| LIVE_REAL은 future safety project | 현재 구현되어 있지 않으며 사용할 수 없습니다. |
| Safety Gate 우선 | `GatewayCommand`와 주문 관련 command는 통제된 경로 외 생성 금지입니다. |

상세 원칙은 [docs/safety_principles_ko.md](docs/safety_principles_ko.md)에 정리되어 있습니다.

## 로컬 실행 방법

Core API 실행:

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

또는 helper:

```powershell
.\tools\start_core.ps1
```

Mock Gateway 1회 실행:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
```

또는 helper:

```powershell
.\tools\start_mock_gateway_once.ps1
```

Real Kiwoom Gateway OBSERVE 실행:

```powershell
# 32-bit Python + Kiwoom OpenAPI+ + 32-bit PyQt5 환경에서 실행
python -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --auto-login `
  --no-threaded-login `
  --condition-name "YOUR_CONDITION_NAME" `
  --condition-realtime
```

조건검색 없이 지정 종목만 실시간 등록:

```powershell
python -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --no-threaded-login `
  --realtime-codes 005930,000660 `
  --realtime-exchange nxt `
  --observe-only
```

`SetRealReg result_code=0`은 실시간 등록 요청 성공이며 tick 수신 보장이 아닙니다. Gateway status의 `realtime_registration_success_count`, `realtime_callback_count`, `latest_realtime_callback_at`, `realtime_subscription_health`를 함께 확인하세요. `GetConditionLoad()` 이후 `OnReceiveConditionVer`도 오지 않고 `OnReceiveRealData`도 0건이면 ActiveX callback, Qt event loop, thread 문제를 우선 의심합니다.
NXT 애프터마켓 smoke에서는 `--realtime-exchange nxt`를 사용합니다. Gateway는 Kiwoom에 `005930_NX`를 등록하고 Core payload에는 base code `005930`과 metadata `exchange=NXT`를 남깁니다.

시장 데이터 projection 재생:

```powershell
python -m tools.rebuild_market_data_projection --clear-projection
```

Theme membership import:

```powershell
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
```

Theme snapshot rebuild:

```powershell
python -m tools.rebuild_theme_snapshots
```

Candidate rebuild:

```powershell
python -m tools.rebuild_candidates
```

Strategy/Risk evaluate:

```powershell
python -m tools.evaluate_strategy
python -m tools.evaluate_risk
```

이 절차는 관찰 파이프라인 확인용입니다. 실제 주문이 나가지 않습니다.

장중 OBSERVE profile과 Core/Gateway token 정렬:

```powershell
.\tools\start_market_open_observe.ps1
```

장중 운영 RCA:

```powershell
python -m tools.ops_market_open_rca `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --trade-date 2026-06-29 `
  --out-dir reports/market_open_rca
```

observe-only end-to-end cycle:

```powershell
python -m tools.run_market_open_observe_cycle `
  --trade-date 2026-06-29 `
  --out-dir reports/market_open_observe_cycle
```

이 cycle은 Theme, Candidate, Strategy, Risk, EntryTiming을 run_once로 묶고 LIVE_SIM은
preflight/status만 읽습니다. `queue_commands=false` 기본값을 바꾸지 않으며
`GatewayCommand(send_order)`를 만들지 않아야 정상입니다. RCA는
`reports/market_open_rca/<trade_date>/summary.md`, observe cycle은
`reports/market_open_observe_cycle/<trade_date>/run_<timestamp>.md`를 남깁니다.
자세한 단계별 확인 순서는
[docs/runbook_market_open_observe_stepwise_ko.md](docs/runbook_market_open_observe_stepwise_ko.md)를 참고하세요.

event-store replay observe 평가:

```powershell
python -m tools.replay_observe_pipeline `
  --trade-date 2026-06-29 `
  --db-path storage\replay\suseok-trader-v2-2026-06-29.sqlite3 `
  --speed 0
```

`--db-path`는 replay용 sqlite 복사본이어야 하며, 현재 설정된 운영 DB와 같은 경로면
실행을 거부합니다. 도구는 event store의 `price_tick`/`condition_event`를 시간순으로
재생해 Theme→Candidate→Strategy→Risk→EntryTiming을 평가하고
`reports/replay/<trade_date>/summary.md`를 씁니다. `GatewayCommand`,
`LIVE_SIM`, `DRY_RUN` 계열 테이블은 SQLite authorizer로 쓰기를 차단하며 summary에
전후 row count delta를 남깁니다. `--speed 0`은 최대속도이고, 양수는 이벤트 간격을
해당 배율로 재생합니다.

market-data inline/worker 격리 parity:

```powershell
python -m tools.ops_projection_replay export `
  --source-db storage\suseok-trader-v2.sqlite3 `
  --trade-date 2026-07-10 `
  --bundle-dir reports\projection_replay_bundles\2026-07-10

python -m tools.ops_projection_replay parity `
  --bundle-dir reports\projection_replay_bundles\2026-07-10 `
  --operational-db storage\suseok-trader-v2.sqlite3 `
  --batch-size 10
```

export는 source DB를 read-only로 열고, parity는 같은 event order hash를 가진 새 격리
DB 두 개에서 inline apply와 worker apply를 비교합니다. 주문/DRY_RUN/LIVE_SIM 및
candidate/incremental side effect table 쓰기는 SQLite authorizer가 차단합니다. 자세한
절차와 KRX/NXT evidence 규칙은
[docs/runbook_market_data_replay_verification_ko.md](docs/runbook_market_data_replay_verification_ko.md)를
참고하세요.

## Dashboard 확인 방법

Core 실행 후 브라우저에서 확인합니다.

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/dashboard`

API로 확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/snapshot
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/funnel
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/errors
```

Dashboard는 “주문하는 화면”이 아니라 “어느 단계에서 데이터가 멈췄는지 보는 화면”입니다.

## 주요 API 요약

### Gateway

- `POST /api/gateway/events`
- `GET /api/gateway/commands`
- `GET /api/gateway/status`
- `GET /api/gateway/events/recent`
- `GET /api/gateway/commands/status`

### Market Data

- `GET /api/market-data/status`
- `GET /api/market-data/ticks/latest?exchange=KRX|NXT|ALL`
- `GET /api/market-data/ticks/{code}?exchange=KRX|NXT|ALL`
- `GET /api/market-data/bars/{code}?exchange=KRX|NXT|ALL&interval_sec=60`
- `GET /api/market-data/readiness/{code}?exchange=KRX|NXT|ALL`
- `GET /api/market-data/premarket/{trade_date}`
- `GET /api/market-data/cross-exchange/{code}`
- `GET /api/market-data/conditions/recent`
- `GET /api/market-data/tr-snapshots/recent`
- `GET /api/market-data/projection-errors`

### Theme / Candidate / Strategy / Risk

- `GET /api/themes/status`
- `POST /api/themes/import`
- `POST /api/themes/snapshots/rebuild`
- `GET /api/candidates/status`
- `POST /api/candidates/rebuild`
- `GET /api/strategy/status`
- `POST /api/strategy/evaluate`
- `GET /api/risk/status`
- `POST /api/risk/evaluate`
- `GET /api/entry-timing/status`
- `GET /api/entry-timing/plans/latest`
- `POST /api/entry-timing/evaluate`
- `POST /api/operator/observe-cycle/run-once`
- `GET /api/operator/observe-cycle/runs/latest`

POST endpoint는 projection/rebuild/evaluate 용도입니다. 주문 endpoint가 아닙니다.

### AI Sidecar / RCA / Codex Prompt Draft

- `GET /api/ai-sidecar/context/status`
- `GET /api/ai-sidecar/context/preview`
- `GET /api/ai-sidecar/execution/status`
- `POST /api/ai-sidecar/run`
- `GET /api/ai-sidecar/rca/status`
- `POST /api/ai-sidecar/rca/no-trade/{trade_date}?run_ai=false`
- `POST /api/ai-sidecar/rca/candidate/{candidate_instance_id}?run_ai=false`
- `GET /api/ai-sidecar/codex-prompts/status`
- `POST /api/ai-sidecar/codex-prompts/from-no-trade/{trade_date}?run_ai=false`

AI/RCA/Codex POST endpoint는 분석 artifact 생성용입니다. 주문 입력이 아닙니다.

### DRY_RUN

- `GET /api/dry-run/status`
- `GET /api/dry-run/eligibility`
- `POST /api/dry-run/evaluate`
- `POST /api/dry-run/intents/from-candidate/{candidate_instance_id}`
- `POST /api/dry-run/orders/from-intent/{dry_run_intent_id}`
- `POST /api/dry-run/orders/{dry_run_order_id}/simulate-fill`

DRY_RUN은 내부 모의 회계이며 브로커 주문이 아닙니다.

### DRY_RUN Exit

- `GET /api/dry-run/exits/status`
- `POST /api/dry-run/exits/evaluate`
- `POST /api/dry-run/exits/intents/from-position/{dry_run_position_id}`
- `POST /api/dry-run/exits/orders/from-intent/{dry_run_exit_intent_id}`
- `POST /api/dry-run/exits/orders/{dry_run_exit_order_id}/simulate-fill`

DRY_RUN Exit signal은 실제 매도 주문이 아닙니다.

### LIVE_SIM

- `GET /api/live-sim/status`
- `GET /api/live-sim/intents`
- `GET /api/live-sim/orders`
- `GET /api/live-sim/executions`
- `GET /api/live-sim/rejections`
- `GET /api/live-sim/reconcile`
- `GET /api/live-sim/reconcile/latest`
- `GET /api/live-sim/positions`
- `GET /api/live-sim/exit-signals`
- `GET /api/live-sim/cancel-intents`
- `GET /api/live-sim/lifecycle-events`
- `POST /api/live-sim/evaluate`
- `POST /api/live-sim/intents/from-candidate/{candidate_instance_id}`
- `POST /api/live-sim/orders/from-intent/{live_sim_intent_id}`
- `POST /api/live-sim/reconcile`
- `POST /api/live-sim/cancel/run-once`
- `POST /api/live-sim/exit/run-once`

LIVE_SIM은 모의투자 전용이며 실계좌 주문이 아닙니다.

Real Kiwoom Gateway에서 `send_order`가 처리되는 경우도 PR12/PR13 LIVE_SIM safety metadata와 simulation server 확인을 모두 통과해야 합니다. Kiwoom `GetServerGubun`이 REAL로 감지되면 Gateway adapter가 주문 command를 거부합니다.

LIVE_SIM 주문 exchange는 `LIVE_SIM_ORDER_EXCHANGE=KRX`가 기본입니다. `NXT`/`SOR`는 Kiwoom mock/모의투자 지원을 운영자가 확인하고 `LIVE_SIM_NXT_SUPPORT_CONFIRMED=true`를 설정하기 전까지 preflight `nxt_order_support_verified`와 safety gate에서 차단됩니다.

## PR별 진행 현황

| PR | 상태 | 한글 설명 | 안전상 의미 |
| --- | --- | --- | --- |
| PR 0 | Done | 프로젝트 bootstrap | 기본 `OBSERVE`, live flag disabled |
| PR 1 | Done | broker-neutral contract | Kiwoom concrete runtime과 Core 분리 |
| PR 2A | Done | AI Sidecar read-only contract | AI를 review-only로 제한 |
| PR 2B | Done | Event Store + Gateway transport | public order enqueue 없음 |
| PR 3 | Done | Mock Gateway | 실제 broker 없이 transport 검증 |
| PR 4 | Done | Market Data Service | read-only projection |
| PR 5 | Done | Theme Service | theme snapshot은 관찰 입력 |
| PR 6 | Done | Candidate FSM | Candidate는 관찰 에피소드 |
| PR 7 | Done | Strategy observe-only | `MATCHED_OBSERVATION`은 매수 신호가 아님 |
| PR 8 | Done | Risk observe-only | `OBSERVE_PASS`는 주문 승인이 아님 |
| PR 9 | Done | Dashboard V1 | read-only 운영 화면 |
| PR AI-1 | Done | AI Context Builder | 모델 호출 없이 입력자료 구성 |
| PR AI-2 | Done | OpenAI structured output | 선택적 수동 실행, tools disabled |
| PR AI-3 | Done | RCA workflows | report artifact only |
| PR AI-4 | Done | Dashboard AI Cards | 표시 전용 |
| PR AI-5 | Done | Codex Prompt Generator | 사람이 복사하는 초안 |
| PR 10 | Done | OMS + DRY_RUN | 내부 모의 회계 |
| PR 11 | Done | DRY_RUN Exit Engine | simulated close accounting |
| PR 12 | Done | LIVE_SIM Enablement | 키움 모의투자 전용 safety-gated path |
| PR AI-6 | Done | LIVE_SIM Review Sidecar | 장후 복기용 review-only report |
| PR 13 | Done | Real Kiwoom Gateway Adapter | 32-bit Gateway에서 실제 Kiwoom event를 Core 계약으로 전달, LIVE_REAL 금지 유지 |

PR12 이후에도 바로 `LIVE_REAL`로 넘어가면 안 됩니다. `LIVE_REAL`은 독립적인 account guard, regulatory check, operator confirmation, kill switch drill이 필요한 future safety project입니다.

## LIVE_SIM / LIVE_REAL 차이

| 구분 | LIVE_SIM | LIVE_REAL |
| --- | --- | --- |
| 목적 | 키움 모의투자 acceptance test | 실계좌 주문 |
| 현재 상태 | 제한적으로 구현, 기본 disabled | 미구현, 금지 |
| 계좌 | simulation account only | 현재 지원하지 않음 |
| command | safety-gated BUY `send_order`, close-only SELL exit, 미체결 BUY `cancel_order`만 가능 | 없음 |
| cancel/modify | BUY TTL cancel 전용 / modify disabled | 없음 |
| Dashboard 버튼 | 없음 | 없음 |
| AI/RCA/Codex 입력 | 금지 | 금지 |

## DRY_RUN / LIVE_SIM 차이

| 구분 | DRY_RUN | LIVE_SIM |
| --- | --- | --- |
| 의미 | 내부 모의 회계 | 키움 모의투자 주문 |
| broker 연결 | 없음 | Gateway command queue 사용 가능 |
| `GatewayCommand` | 생성하지 않음 | safety gate 통과 시 `source=live_sim` BUY/SELL-close `send_order`와 BUY `cancel_order`만 가능 |
| 결과 | ledger, paper position | mock/simulation account order record |
| 실계좌 여부 | 아님 | 아님 |

## 자주 오해하는 용어

| 용어 | 올바른 의미 |
| --- | --- |
| `Candidate` | 후보 관찰 에피소드입니다. 매수 후보가 아닙니다. |
| `CONTEXT_READY` | Strategy 평가 입력이 준비되었다는 뜻입니다. 매수 준비가 아닙니다. |
| `MATCHED_OBSERVATION` | 전략 관측 조건이 맞았다는 뜻입니다. 매수 신호가 아닙니다. |
| `OBSERVE_PASS` | 리스크 관측상 block/caution이 없다는 뜻입니다. 주문 승인이 아닙니다. |
| `DRY_RUN` | 내부 모의 회계입니다. 브로커 주문이 아닙니다. |
| `LIVE_SIM` | 키움 모의투자 전용입니다. 실계좌 주문이 아닙니다. |
| `LIVE_REAL` | 현재 구현되어 있지 않으며 금지입니다. |
| `AI Insight` | 운영자 검토용 설명입니다. 자동 주문 입력이 아닙니다. |
| `Codex Prompt Draft` | 사람이 복사해서 검토하는 프롬프트 초안입니다. 자동 branch/commit/push/PR 생성이 아닙니다. |

더 긴 용어 사전은 [docs/glossary_ko.md](docs/glossary_ko.md)에 있습니다.

## 주요 문서

- [docs/operator_overview_ko.md](docs/operator_overview_ko.md): 운영자용 전체 흐름
- [docs/safety_principles_ko.md](docs/safety_principles_ko.md): 안전 원칙
- [docs/runbook_local_observe_ko.md](docs/runbook_local_observe_ko.md): 로컬 관찰 runbook
- [docs/runbook_market_open_observe_stepwise_ko.md](docs/runbook_market_open_observe_stepwise_ko.md): 장중 OBSERVE 단계별 점검 runbook
- [docs/market_open_stepwise_gap_report.md](docs/market_open_stepwise_gap_report.md): 장중 운영 gap table
- [docs/roadmap.md](docs/roadmap.md): PR별 진행 현황
- [docs/architecture.md](docs/architecture.md): 구조와 경계
- [docs/event_contract.md](docs/event_contract.md): broker-neutral 계약
- [docs/live_sim_enablement.md](docs/live_sim_enablement.md): LIVE_SIM safety gate
- [docs/nxt_rollout_ko.md](docs/nxt_rollout_ko.md): NXT 관찰 및 LIVE_SIM 라우팅 준비 rollout
- [docs/kiwoom_gateway_adapter.md](docs/kiwoom_gateway_adapter.md): Real Kiwoom Gateway adapter
- [docs/runbook_market_open_live_observe.md](docs/runbook_market_open_live_observe.md): 장중 OBSERVE 확인 절차
- [docs/runbook_live_sim_pilot.md](docs/runbook_live_sim_pilot.md): LIVE_SIM pilot 절차
- [docs/oms_dry_run.md](docs/oms_dry_run.md): DRY_RUN OMS

## Local Token

`TRADING_CORE_TOKEN`을 설정하면 Gateway write/poll endpoint와 일부 수동 POST endpoint에 local token이 필요합니다.

```powershell
$env:TRADING_CORE_TOKEN = "change-me-local-token"
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/events `
  -Method Post `
  -Headers @{"X-Core-Token" = "change-me-local-token"} `
  -ContentType "application/json" `
  -Body '{"event_type":"heartbeat","source":"local-gateway","payload":{"status":"ok"}}'
```

`GET /health`, `GET /api/status`, Dashboard read-only 조회 endpoint는 token 없이 확인할 수 있습니다.

## 테스트 실행 방법

```powershell
python -m pytest
python -m ruff check .
```

## 운영자 체크포인트

- 먼저 `/api/status`, `/api/gateway/status`, `/api/dashboard/snapshot`을 봅니다.
- Dashboard에서 막힌 단계가 Market Data인지 Theme인지 Candidate인지 Strategy/Risk인지 확인합니다.
- `MATCHED_OBSERVATION`과 `OBSERVE_PASS`를 주문 가능 상태로 해석하지 않습니다.
- DRY_RUN 결과를 broker 주문으로 해석하지 않습니다.
- LIVE_SIM을 실계좌 주문으로 해석하지 않습니다.
- `LIVE_REAL`은 현재 구현되어 있지 않으며 별도 future safety project로만 다룹니다.
