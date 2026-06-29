# Market Open Runtime Gap Report

PR 이름: Market Open Runtime Wiring RCA & Observe Cycle Recovery

저장소: `suseok-trader-v2`  
확인 브랜치: `main`  
기본 Core 실행 스크립트: `tools/start_core.ps1`  
장중 OBSERVE helper: `tools/start_market_open_observe.ps1`

## 핵심 결론

현재 저장소에는 Core/Gateway, market-data projection, Theme, Candidate FSM, Strategy,
Risk, EntryTiming, LIVE_SIM read-only/operator 상태 API가 대부분 존재한다. 다만 기존
`suseok_ai`의 장중 운영자가 기대했던 "Gateway event가 들어오면 후보/전략/리스크/진입
타이밍까지 자동으로 한 번 이어지는 흐름"은 기본 uvicorn 실행만으로는 보장되지 않는다.

이번 변경은 수익률 개선이나 threshold 완화가 아니라 다음 두 가지를 추가한다.

- `tools/ops_market_open_rca.py`: Core API를 읽어 stage별 PASS/WARN/BLOCK/UNKNOWN과 reason
  code를 보고서로 남긴다.
- `market_open_observe_cycle`: Theme snapshot/leadership, Candidate, Strategy, Risk,
  EntryTiming을 run_once로 묶되 주문 command를 만들지 않는다.

## Gap Table

| 이관 기대 요소 | 현재 실제 연결 | 자동/수동 | Gap | 이번 조치 |
| --- | --- | --- | --- | --- |
| Reboot V2 observe startup | `tools/start_core.ps1`은 uvicorn만 실행 | 수동 | 장중 OBSERVE env/profile이 명확하지 않음 | `tools/start_market_open_observe.ps1` 추가 |
| Theme Core V3 / leadership | `services/theme_service.py`, `services/theme_leadership/*` 존재 | 수동 rebuild | Gateway event 후 자동 leadership cycle 없음 | observe cycle에서 snapshot + leadership run_once |
| candidate ingest / FSM | `services/candidate_service.py` 존재 | 수동 rebuild | 조건식/테마 source ingest를 운영자가 직접 호출해야 함 | observe cycle에서 candidate rebuild |
| StrategyContext / evaluate | `services/strategy_engine.py`, `/api/strategy/evaluate` 존재 | 수동 evaluate | Candidate 이후 자동 실행 없음 | observe cycle에서 evaluate |
| Risk evaluate | `services/risk_gate.py`, `/api/risk/evaluate` 존재 | 수동 evaluate | Strategy 이후 자동 실행 없음 | observe cycle에서 evaluate |
| EntryTiming / order plan draft | `services/entry_timing/*` 존재 | 수동 evaluate | Risk 이후 자동 실행 없음 | observe cycle에서 observe-only draft 생성 |
| Dashboard V2 / funnel | Dashboard V1과 `pipeline_summary` 존재 | 자동 조회 | stage별 PASS/WARN/BLOCK가 부족 | pipeline stage summary 최소 추가 |
| ops_check / alerts | No-Buy Sentinel, dashboard errors, RCA workflows 존재 | 수동 조회 | 장중 어디서 끊겼는지 단일 보고서 부족 | `ops_market_open_rca.py` 추가 |
| Naver theme sync/import | `tools/import_naver_themes.py`, importer 존재 | 수동 import | 장중 자동 sync 아님 | 문서에 장전/수동 작업으로 명시 |
| LIVE_SIM observe/run_once/preflight | `/api/live-sim/operator/*`, CLI 존재 | 수동 run_once | 주문 queue와 observe 진단이 섞여 보일 수 있음 | observe cycle은 LIVE_SIM preflight/status만 read-only 수집 |

## 자동으로 도는 단계와 수동 단계

자동으로 도는 단계:

- Core API lifespan에서 DB schema 초기화
- Gateway `POST /api/gateway/events` 수신 시 raw/gateway event 저장
- 수신 event가 `price_tick`, `condition_event`, `tr_response`이면 market-data projection 적용
- Dashboard snapshot 조회 시 read-only 상태 집계

수동 run_once/rebuild가 필요한 단계:

- Theme membership import 또는 Naver theme import
- Theme snapshot rebuild
- Theme leadership/watchset rebuild
- Candidate rebuild
- Strategy evaluate
- Risk evaluate
- EntryTiming evaluate
- LIVE_SIM operator/pilot/cancel/exit run_once

이번 추가 run_once:

```powershell
python -m tools.run_market_open_observe_cycle --trade-date 2026-06-29
```

Core API에서 실행:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/observe-cycle/run-once" `
  -Headers @{"X-Core-Token" = $env:TRADING_CORE_TOKEN}
```

## 운영자가 착각하기 쉬운 용어

| 용어 | 뜻 | 아닌 것 |
| --- | --- | --- |
| `MATCHED_OBSERVATION` | Strategy 관찰 조건이 맞았다는 기록 | 매수 신호 아님 |
| `OBSERVE_PASS` | Risk 관찰상 큰 차단 사유가 없다는 기록 | 주문 승인 아님 |
| `OrderPlanDraft` | EntryTiming의 관찰용 주문 계획 초안 | 주문 의도나 주문 명령 아님 |
| `PLAN_READY` | 초안이 형태상 준비됨 | LIVE_SIM 주문 승인 아님 |
| `LIVE_SIM` | 키움 모의투자 전용 경로 | 실계좌 주문 아님 |
| `queue_commands=false` | GatewayCommand queue를 만들지 않음 | 기능 장애가 아님 |
| AI advisory | 운영자 참고 annotation | Strategy/Risk/Entry 자동 입력 아님 |

## 정상 실행 순서

1. 64-bit Core 실행

```powershell
.\tools\start_market_open_observe.ps1
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

2. 32-bit Kiwoom Gateway 실행

```powershell
.\venv_32\Scripts\python.exe -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:GATEWAY_CORE_TOKEN `
  --observe-only `
  --auto-login `
  --condition-name "조건식명" `
  --condition-realtime
```

3. RCA 수집

```powershell
python -m tools.ops_market_open_rca `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --trade-date 2026-06-29
```

4. observe-only cycle 실행

```powershell
python -m tools.run_market_open_observe_cycle --trade-date 2026-06-29
```

5. Dashboard 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/funnel
Invoke-RestMethod http://127.0.0.1:8000/api/operator/observe-cycle/runs/latest
```

## 문제 발생 시 확인 순서

1. `CORE_DOWN`, `CORE_STATUS_ERROR`: `/health`, `/api/status`, DB path 확인
2. `GATEWAY_AUTH_FAILED`: `TRADING_CORE_TOKEN`과 `GATEWAY_CORE_TOKEN` 값 일치 확인
3. `GATEWAY_HEARTBEAT_MISSING`: Gateway process, Kiwoom login, heartbeat event 확인
4. `TICK_MISSING`, `TICK_STALE`: 실시간 등록, condition hit, tick projection 확인
5. `THEME_MEMBERSHIP_EMPTY`: theme import 또는 Naver theme import 확인
6. `THEME_SNAPSHOT_NOT_BUILT`: theme snapshot rebuild 또는 observe cycle 실행
7. `CANDIDATE_EMPTY`: condition event, theme source state/role, candidate rebuild 확인
8. `STRATEGY_EVALUATE_NOT_RUN`, `RISK_EVALUATE_NOT_RUN`: observe cycle 또는 개별 evaluate 실행
9. `ORDER_PLAN_EMPTY`: EntryTiming input, stale tick, risk/strategy status 확인
10. `LIVE_SIM_DISABLED_EXPECTED`, `LIVE_SIM_KILL_SWITCH_ON_EXPECTED`: observe-only 기본 안전 상태로 해석

## 안전 정책

- `LIVE_REAL`은 금지이며 이번 변경에서 구현하지 않는다.
- 실계좌 주문 enable flag를 켜지 않는다.
- `queue_commands` 기본값은 계속 `false`다.
- 실제 키움 모의투자 주문 연결은 별도 승인 전까지 자동 queue하지 않는다.
- observe cycle은 `send_order` GatewayCommand를 만들면 실패로 기록한다.
- AI/External LLM 결과는 advisory-only이며 Strategy/Risk/EntryTiming/LIVE_SIM 자동 입력이 아니다.
