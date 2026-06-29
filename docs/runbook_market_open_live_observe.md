# Market Open LIVE Observe Runbook

## 목적

실제 장중에 32-bit Real Kiwoom Gateway가 Core OBSERVE 파이프라인을 갱신하는지 확인한다. 이 runbook은 주문 기능을 켜지 않는다.

## 사전 조건

- `TRADING_MODE=OBSERVE`
- `TRADING_ALLOW_LIVE_REAL=false`
- `LIVE_SIM_ORDER_ROUTING_ENABLED=false`
- 32-bit Python, Kiwoom OpenAPI+, 32-bit PyQt5 준비
- Kiwoom 조건식 이름 또는 index 확인
- Core local token 확인

## 1. Core 실행

```powershell
$env:TRADING_MODE = "OBSERVE"
$env:TRADING_ALLOW_LIVE_REAL = "false"
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/status
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/status
```

## 2. Real Kiwoom Gateway 실행

32-bit Python shell에서 실행:

```powershell
python -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --auto-login `
  --no-threaded-login `
  --heartbeat-interval-sec 2 `
  --poll-wait-sec 1 `
  --condition-name "YOUR_CONDITION_NAME" `
  --condition-realtime `
  --observe-only
```

조건식 없이 smoke 확인:

```powershell
python -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --realtime-codes 005930 `
  --observe-only
```

## 3. Login 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/events/recent?limit=20
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/status
```

확인할 값:

- `heartbeat`가 반복 유입
- heartbeat payload `kiwoom_logged_in=true`
- heartbeat payload `login_threaded=false`
- `broker_name=KIWOOM`
- OBSERVE에서는 주문 관련 env가 꺼져 있음
- REAL server로 감지되더라도 OBSERVE 데이터 수집은 가능하나 주문은 금지

## 4. Condition Load 확인

최근 event에서 확인:

- `condition_load_result`
- `condition_loaded`
- configured condition send log
- `latest_condition_ver_callback_at`
- `condition_load_state != CALLBACK_TIMEOUT`

조건식 이름이 틀리면 Gateway는 tick을 받지 못할 수 있다. 이름이 불확실하면 `--condition-index`를 사용한다.
`GetConditionLoad()` 이후 10초 이상 `OnReceiveConditionVer`가 없으면 Gateway는 1회 재시도 후 `CALLBACK_TIMEOUT`으로 전환한다. 이 경우 조건식 실패보다 ActiveX callback/thread/event loop 문제를 먼저 본다.

## 5. Condition Event 확인

조건 hit 발생 시:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/conditions/recent
```

확인할 값:

- `action=ENTER` 또는 `EXIT`
- code가 6자리
- condition metadata에 `condition_index`

## 6. Price Tick 확인

condition ENTER 또는 `--realtime-codes` 등록 후:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/ticks/latest
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/readiness/005930
```

확인할 값:

- latest tick이 갱신됨
- `market_minute_bars`가 생성됨
- readiness가 `FRESH` 또는 최소 `DEGRADED`
- projection error가 증가하지 않음
- `realtime_registration_success_count > 0`
- `realtime_callback_count > 0`
- `latest_realtime_callback_at`가 갱신됨
- `realtime_subscription_health=CALLBACK_ACTIVE`

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/market-data/projection-errors
```

`SetRealReg result_code=0`은 등록 요청 성공이지 tick 수신 보장이 아니다. 등록 성공 후 정규장 중 15초 이상 `OnReceiveRealData`가 없으면 `realtime_subscription_health=CALLBACK_TIMEOUT`으로 표시된다. `OnReceiveConditionVer`도 같이 안 오면 ActiveX event sink, Qt event loop, 호출 thread를 먼저 확인한다.

## 7. Theme/Candidate/Strategy/Risk 갱신

필요 시 수동 rebuild/evaluate:

```powershell
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_candidates
python -m tools.evaluate_strategy
python -m tools.evaluate_risk
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/snapshot
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/funnel
```

## 8. 주문 기능 OFF 확인

OBSERVE run에서는 다음이 유지되어야 한다.

- `TRADING_MODE=OBSERVE`
- `LIVE_SIM_ORDER_ROUTING_ENABLED=false`
- `LIVE_SIM_GATEWAY_COMMAND_ENABLED=false`
- `/api/gateway/status`의 `order_commands_allowed=false`
- `cancel_order`, `modify_order`, generic order enqueue 없음

## 성공 기준

- Real Gateway heartbeat가 Core에 들어온다.
- Kiwoom login status를 event로 확인한다.
- condition list load가 성공한다.
- condition ENTER/EXIT가 Core projection에 반영된다.
- condition hit code 또는 지정 code의 `price_tick`이 Core에 들어온다.
- latest tick, bars, readiness, dashboard가 장중 데이터로 갱신된다.
- 주문 관련 기능은 꺼져 있다.

## 중단 기준

- Core event ingest 4xx/5xx 반복
- projection error 반복
- Kiwoom login 실패 반복
- 조건식 오동작으로 예상 밖 종목 대량 등록
- 주문 관련 env가 켜져 있음
