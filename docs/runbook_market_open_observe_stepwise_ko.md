# 장중 OBSERVE 단계별 점검 Runbook

이 runbook은 장중에 Core/Gateway/MarketData/Theme/Candidate/Strategy/Risk/EntryTiming이 어디까지 연결되는지 확인하기 위한 절차다. 목표는 주문이 아니라 관찰 파이프라인 복구와 원인 분리다.

## 절대 안전 원칙

- `LIVE_REAL`은 금지다. 실계좌 주문 구현, enable flag, 주문 버튼을 추가하지 않는다.
- `queue_commands=false`가 기본이다. OBSERVE 점검 중 true로 바꾸지 않는다.
- `LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=false`, `LIVE_SIM_GATEWAY_COMMAND_ENABLED=false`를 유지한다.
- `OrderPlanDraft`는 주문이 아니다. `PLAN_READY`도 주문 승인이나 매수 신호가 아니다.
- AI/External LLM 결과는 advisory-only이며 Strategy/Risk/EntryTiming/LIVE_SIM 입력으로 자동 반영하지 않는다.

## 정상 실행 순서

1. 64-bit Core OBSERVE 실행

```powershell
.\tools\start_market_open_observe.ps1
```

별도 터미널에서 직접 실행할 경우:

```powershell
$env:TRADING_MODE = "OBSERVE"
$env:TRADING_ALLOW_LIVE_REAL = "false"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "false"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED = "false"
$env:LIVE_SIM_KILL_SWITCH = "true"
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000
```

2. 32-bit Kiwoom Gateway 실행

32-bit Python, Kiwoom OpenAPI+, 32-bit PyQt5 환경에서 실행한다.

```powershell
.\venv_32\Scripts\python.exe -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:GATEWAY_CORE_TOKEN `
  --observe-only `
  --auto-login `
  --no-threaded-login `
  --condition-name "조건식명" `
  --condition-realtime
```

조건식 없이 지정 종목만 확인할 때:

```powershell
.\venv_32\Scripts\python.exe -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:GATEWAY_CORE_TOKEN `
  --observe-only `
  --no-threaded-login `
  --realtime-codes 005930,000660
```

Core HTTP IO 영향을 분리할 때는 isolation mode로 실행한다. 이 모드는 Core가 꺼져 있어도 동작하며 `post_event`/`poll_commands`를 호출하지 않는다.

```powershell
.\venv_32\Scripts\python.exe -m apps.kiwoom_gateway `
  --disable-core-io `
  --observe-only `
  --auto-login `
  --no-threaded-login `
  --realtime-codes 005930,000660
```

isolation mode는 60초 동안 console에 `qt_main_thread_id`, `active_x_thread_id`, `on_event_connect_count`, `condition_ver_callback_count`, `realtime_registration_success_count`, `raw_realtime_callback_count`, `parsed_price_tick_count`, `realtime_parse_error_count`, 최신 callback 시각을 주기 출력한다.

3. RCA 실행

```powershell
python -m tools.ops_market_open_rca `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --trade-date 2026-06-29 `
  --out-dir reports/market_open_rca
```

출력:

- `reports/market_open_rca/<trade_date>/summary.json`
- `reports/market_open_rca/<trade_date>/summary.md`

4. observe-only cycle run_once 실행

```powershell
python -m tools.run_market_open_observe_cycle `
  --trade-date 2026-06-29 `
  --out-dir reports/market_open_observe_cycle
```

출력:

- `reports/market_open_observe_cycle/<trade_date>/run_<timestamp>.json`
- `reports/market_open_observe_cycle/<trade_date>/run_<timestamp>.md`

5. Dashboard 확인

브라우저:

- `http://127.0.0.1:8000/dashboard`

API:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/snapshot
Invoke-RestMethod http://127.0.0.1:8000/api/operator/observe-cycle/runs/latest
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/commands/status
```

## 문제 발생 시 확인 순서

1. Core: `/health`, `/api/status`
2. Gateway auth/token: `/api/gateway/auth/probe`, `/api/gateway/status`
3. Kiwoom login/condition/realtime: `/api/gateway/events/recent?limit=50`
4. MarketData: `/api/market-data/status`, `/api/market-data/ticks/latest`, `/api/market-data/projection-errors`
5. Theme: `/api/themes/status`, `/api/themes/snapshots/latest`, `/api/themes/projection-errors`
6. Candidate: `/api/candidates/status`, `/api/candidates`, `/api/candidates/projection-errors`
7. Strategy/Risk: `/api/strategy/status`, `/api/strategy/runs`, `/api/risk/status`, `/api/risk/runs`
8. EntryTiming: `/api/entry-timing/status`, `/api/entry-timing/plans/latest`, `/api/entry-timing/errors`
9. LIVE_SIM read-only status: `/api/live-sim/status`, `/api/live-sim/operator/status`
10. Order safety: `/api/gateway/commands/status`

## Kiwoom ActiveX callback 점검

Kiwoom Gateway는 32-bit Python, 32-bit PyQt5, Kiwoom OpenAPI+, `QAxWidget` 조합을 Qt main thread에서 실행해야 한다. 기본 실행은 `--no-threaded-login`이며 `--threaded-login`은 ActiveX 안정성 관점에서 deprecated다.

`SetRealReg result_code=0`은 실시간 등록 요청 성공이다. 실제 tick 수신 성공은 `OnReceiveRealData` callback으로만 확인한다. `GetConditionLoad()`도 호출 성공만으로 조건식 로딩 완료가 아니며, `OnReceiveConditionVer` callback이 와야 완료로 본다.

`OnReceiveConditionVer`와 `OnReceiveRealData`가 모두 안 오면 종목에 tick이 없어서라기보다 ActiveX event sink, Qt event loop, 호출 thread 문제를 우선 의심한다. 먼저 다음 값을 확인한다.

- `login_threaded`
- `condition_load_state`
- `condition_callback_health`
- `latest_condition_ver_callback_at`
- `realtime_registration_success_count`
- `realtime_callback_count`
- `raw_realtime_callback_count`
- `latest_realtime_callback_at`
- `realtime_subscription_health`
- `latest_active_x_thread_audit`
- `core_io_worker_running`
- `core_io_worker_event_queue_size`
- `core_io_worker_command_queue_size`

`realtime_subscription_health` 해석:

- `NOT_REQUESTED`: 실시간 등록 요청 전
- `REGISTERED_WAITING_CALLBACK`: 등록 요청은 성공했고 callback 대기 중
- `CALLBACK_ACTIVE`: raw `OnReceiveRealData` callback 수신 중
- `CALLBACK_TIMEOUT`: 등록 성공 후 timeout 동안 callback 없음
- `PARSE_ERROR`: raw callback은 왔지만 FID parsing 실패
- `CORE_IO_BLOCKING_SUSPECTED`: main-thread Core IO가 관측된 상태에서 callback timeout
- `ACTIVE_X_CALLBACK_SUSPECTED`: Core IO를 끈 isolation mode에서도 callback timeout

callback 미수신 시 확인 순서:

1. isolation mode 실행
2. `on_event_connect_count` 확인
3. `condition_ver_callback_count` 확인
4. `realtime_registration_success_count` 확인
5. `raw_realtime_callback_count` 확인
6. normal mode와 isolation mode 차이 비교

`SetRealReg result_code=0`은 등록 요청 성공이지 callback 수신 성공이 아니다. Kiwoom `QAxWidget`/ActiveX callback은 Qt main event loop가 막히면 들어오지 않을 수 있으므로 Core HTTP polling/post_event를 Qt main thread에서 동기 실행하지 않는다. 기본 실행은 `--no-threaded-login`이며 `--threaded-login`은 deprecated다.

## 단계별 endpoint

| Stage | Endpoint | 해석 |
| --- | --- | --- |
| Core | `GET /health`, `GET /api/status` | Core 실행, OBSERVE mode, live flags 확인 |
| Gateway | `GET /api/gateway/status`, `GET /api/gateway/events/recent?limit=50` | heartbeat, login, condition, tick event 확인 |
| OrderSafety | `GET /api/gateway/commands/status` | order-like command count가 0이어야 정상 |
| MarketData | `GET /api/market-data/status`, `GET /api/market-data/ticks/latest` | tick projection과 stale 여부 확인 |
| Theme | `GET /api/themes/status`, `GET /api/themes/snapshots/latest` | membership과 latest snapshot 확인 |
| Candidate | `GET /api/candidates/status`, `GET /api/candidates` | active candidate와 DATA_WAIT 확인 |
| Strategy | `GET /api/strategy/status`, `GET /api/strategy/runs` | evaluate 실행 여부와 observation count 확인 |
| Risk | `GET /api/risk/status`, `GET /api/risk/runs` | evaluate 실행 여부와 observe pass/caution/block 확인 |
| EntryTiming | `GET /api/entry-timing/status`, `GET /api/entry-timing/plans/latest` | `PLAN_READY/WAIT_RETRY/DATA_WAIT/NO_PLAN` 확인 |
| LiveSim | `GET /api/live-sim/status`, `GET /api/live-sim/operator/status` | disabled/kill switch/read-only 상태 확인 |
| Dashboard | `GET /api/dashboard/snapshot` | stage별 PASS/WARN/BLOCK 요약 확인 |
| ObserveCycle | `POST /api/operator/observe-cycle/run-once` | 수동 observe-only 갱신. 주문 command 생성 금지 |

## Reason code 의미

| Reason code | 의미 | 우선 확인 |
| --- | --- | --- |
| `CORE_DOWN` | Core API 연결 실패 | Core process, host/port |
| `CORE_STATUS_ERROR` | `/api/status` 이상 | mode, live flags, settings |
| `GATEWAY_AUTH_FAILED` | token 불일치 또는 누락 | `TRADING_CORE_TOKEN`, `GATEWAY_CORE_TOKEN` |
| `GATEWAY_HEARTBEAT_MISSING` | Gateway heartbeat 없음/오래됨 | Gateway process, Kiwoom login |
| `KIWOOM_LOGIN_FAILED` | Kiwoom login 실패 event | 계정/login 상태 |
| `COMM_CONNECT_NO_RETURN` | `CommConnect()` 호출 후 result가 반환되지 않음 | Kiwoom login/version UI, OpenAPI install, desktop session |
| `ON_EVENT_CONNECT_TIMEOUT` | `OnEventConnect` callback timeout | `comm_connect_state`, `latest_on_event_connect_timeout_at` |
| `KIWOOM_LOGIN_DIALOG_OR_VERSION_SUSPECTED` | login/version 처리 창 또는 OpenAPI 상태 의심 | 영웅문/OpenAPI 수동 실행, hidden window |
| `ACTIVE_X_LOGIN_BLOCKED` | ActiveX login 단계에서 block | `latest_comm_connect_call_at`, `latest_comm_connect_result_at` |
| `CONDITION_LOAD_FAILED` | 조건식 load 실패 | 조건식명/index, Kiwoom 조건식 |
| `CONDITION_VER_CALLBACK_TIMEOUT` | `GetConditionLoad` 이후 `OnReceiveConditionVer` 미수신 | ActiveX callback/thread/event loop |
| `CONDITION_LOAD_TIMEOUT` | condition load callback timeout | `latest_condition_ver_callback_at`, thread audit |
| `ACTIVE_X_CALLBACK_SUSPECTED` | ActiveX callback 미수신 의심 | `latest_active_x_thread_audit`, Qt event loop |
| `POSSIBLE_THREADING_ISSUE` | 호출 thread 문제 가능성 | `login_threaded=false`, Qt main thread 호출 여부 |
| `CONDITION_NOT_SENT` | 조건식 load 후 send 흔적 없음 | Gateway args, condition realtime |
| `NO_CONDITION_HIT` | 조건 hit 없음 | 조건식, 시장 상황, realtime 등록 |
| `REALTIME_NOT_REGISTERED` | condition ENTER 후 realtime 등록 없음 | screen/code 등록 상태 |
| `REALTIME_CALLBACK_MISSING` | `SetRealReg` 성공 후 `OnReceiveRealData` 미수신 | `realtime_subscription_health`, raw callback count |
| `TICK_MISSING` | latest tick 없음 | realtime code, projection |
| `TICK_STALE` | tick이 오래됨 | Gateway 연결, 장중 수신 상태 |
| `MARKET_PROJECTION_ERROR` | MarketData projection 실패 | projection error payload |
| `THEME_MEMBERSHIP_EMPTY` | theme membership 0 | theme import/Naver import |
| `THEME_SNAPSHOT_NOT_BUILT` | snapshot 미생성 | rebuild 또는 observe cycle |
| `CANDIDATE_REBUILD_NOT_RUN` | candidate rebuild 미실행 | observe cycle 또는 개별 rebuild |
| `CANDIDATE_EMPTY` | candidate가 없음 | condition/theme/tick source |
| `CANDIDATE_DATA_WAIT` | candidate 입력 데이터 부족 | tick/bar/vwap/readiness |
| `STRATEGY_EVALUATE_NOT_RUN` | strategy evaluate 미실행 | observe cycle 또는 개별 evaluate |
| `STRATEGY_EMPTY` | evaluate는 됐지만 observation 없음 | candidate state, stale tick |
| `RISK_EVALUATE_NOT_RUN` | risk evaluate 미실행 | strategy run 이후 risk evaluate |
| `RISK_EMPTY` | risk observation 없음 | strategy matched 여부, stale context |
| `ENTRY_TIMING_NO_INPUT` | EntryTiming 입력 없음 또는 DATA_WAIT 중심 | candidate/watchset/risk/strategy input |
| `ORDER_PLAN_EMPTY` | draft가 없음 | no setup, blocked chase/risk/stale |
| `LIVE_SIM_DISABLED_EXPECTED` | OBSERVE에서 LIVE_SIM disabled | 정상 안전 상태 |
| `LIVE_SIM_KILL_SWITCH_ON_EXPECTED` | kill switch on | 정상 안전 상태 |
| `ORDER_COMMAND_ZERO_EXPECTED` | order command 0이어야 함 | `/api/gateway/commands/status` |
| `EXTERNAL_LLM_DISABLED_EXPECTED` | external LLM off | 정상 안전 상태 |

## 운영자 해석

Candidate:

- `active_candidate_count > 0`이면 후보 관찰 에피소드가 만들어진 것이다.
- `DATA_WAIT`은 가격/분봉/VWAP/readiness가 부족하다는 뜻이지 threshold를 낮추라는 뜻이 아니다.
- candidate가 0이면 먼저 Theme membership, condition event, tick projection을 본다.

Strategy:

- `MATCHED_OBSERVATION`은 관찰 조건이 맞았다는 기록이다. 매수 신호가 아니다.
- run이 없으면 evaluate 미실행이고, run은 있는데 observation이 0이면 입력/조건 미충족이다.

Risk:

- `OBSERVE_PASS`는 리스크 관찰상 차단 사유가 없다는 뜻이다. 주문 승인이 아니다.
- caution/block은 운영자가 읽는 관찰 결과이며 자동 주문 차단/승격 로직으로 해석하지 않는다.

EntryTiming:

- `OrderPlanDraft`는 observe-only draft다. `observe_only=true`, `not_order_intent=true`가 정상이다.
- `PLAN_READY`는 LIVE_SIM safety gate 입력 후보일 뿐 주문 승인이나 매수 신호가 아니다.
- `WAIT_RETRY`, `DATA_WAIT`, `NO_PLAN`을 강제로 `PLAN_READY`로 승격하지 않는다.

## 안전 확인 체크

장중 점검 종료 전 다음이 모두 유지되어야 한다.

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/status
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/commands/status
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/status
```

확인값:

- `TRADING_MODE=OBSERVE`
- `live_real_allowed=false`
- `order_commands_allowed=false`
- `queue_commands=false`
- `order_command_count=0`
- `send_order_delta=0`
