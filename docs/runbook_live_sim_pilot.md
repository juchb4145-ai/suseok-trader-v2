# LIVE_SIM Pilot Runbook

## 목적

키움 모의투자 계정에서만 PR12/PR13 LIVE_SIM 주문 경로를 소량 검증한다. `LIVE_REAL`은 이 runbook의 범위가 아니며 계속 금지다.

## 사전 조건

- Kiwoom Gateway가 32-bit Python에서 실행 가능
- Kiwoom `GetServerGubun`이 simulation-like
- Core heartbeat에 `broker_env=SIMULATION`, `server_mode=SIMULATION`, `account_mode=SIMULATION`
- DRY_RUN evidence가 준비됨
- 운영자가 주문 대상 candidate와 수량/금액 제한을 직접 확인함

## 1. 환경 설정

```powershell
$env:TRADING_MODE = "LIVE_SIM"
$env:TRADING_ALLOW_LIVE_SIM = "true"
$env:TRADING_ALLOW_LIVE_REAL = "false"
$env:LIVE_SIM_ENABLED = "true"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "true"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "true"
$env:LIVE_SIM_KILL_SWITCH = "false"
$env:LIVE_SIM_ACCOUNT_ID = "SIM-ACCOUNT-ID"
$env:LIVE_SIM_ACCOUNT_MODE = "SIMULATION"
$env:LIVE_SIM_BROKER_ENV = "SIMULATION"
$env:LIVE_SIM_SERVER_MODE = "SIMULATION"
```

금액 제한은 작게 둔다.

```powershell
$env:LIVE_SIM_MAX_ORDER_NOTIONAL = "100000"
$env:LIVE_SIM_MAX_DAILY_ORDER_COUNT = "1"
```

## 2. Core와 Gateway 실행

Core:

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000
```

32-bit Gateway:

```powershell
python -m apps.kiwoom_gateway `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --auto-login `
  --threaded-login `
  --condition-name "YOUR_CONDITION_NAME" `
  --condition-realtime
```

## 3. Safety Gate 확인

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/status
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/status
```

필수 확인:

- `live_real_allowed=false`
- `safety_gate.passed=true`
- `gateway_orderable=true`
- `account_mode=SIMULATION`
- `broker_env=SIMULATION`
- `server_mode=SIMULATION`
- failed/rejected gateway command history 없음

하나라도 다르면 pilot을 중단한다.

## 4. DRY_RUN Evidence 확인

```powershell
python -m tools.inspect_candidate --candidate-instance-id CANDIDATE_ID
python -m tools.inspect_strategy_observation --candidate-instance-id CANDIDATE_ID
python -m tools.inspect_risk_observation --candidate-instance-id CANDIDATE_ID
```

필수 확인:

- candidate `CONTEXT_READY`
- strategy `MATCHED_OBSERVATION`
- risk `OBSERVE_PASS`
- latest tick fresh
- DRY_RUN order/fill evidence 존재

## 5. LIVE_SIM Intent 생성

```powershell
python -m tools.evaluate_live_sim_eligibility --candidate-instance-id CANDIDATE_ID
python -m tools.create_live_sim_intent --candidate-instance-id CANDIDATE_ID
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/intents
```

생성된 intent의 `live_sim_only=true`, `live_real_allowed=false`, `idempotency_key`를 확인한다.

## 6. Gateway Command Queue

```powershell
python -m tools.queue_live_sim_order --live-sim-intent-id LIVE_SIM_INTENT_ID
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/commands/status
```

Gateway가 polling하면 최근 event에서 확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/events/recent?limit=50
```

필수 event:

- `command_started`
- `command_ack` 또는 명확한 `command_failed`

`command_failed`가 발생하면 error message를 보존하고 pilot을 중단한다.

## 7. Chejan / Execution 확인

체결 또는 주문 상태가 들어오면:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/orders
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/executions
```

확인:

- `BROKER_ACKED`, `PARTIALLY_FILLED`, `FILLED` 중 하나로 전이
- execution event metadata에 `live_sim_only=true`
- execution event metadata에 `live_real_allowed=false`
- REAL account/server metadata가 섞이지 않음

## 8. Reconcile

```powershell
python -m tools.reconcile_live_sim
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/reconcile
```

PR13의 reconcile은 local-first 확인이다. broker full snapshot reconcile은 후속 PR에서 강화한다.

## 9. Rollback

pilot 종료 후 즉시 끈다.

```powershell
$env:LIVE_SIM_KILL_SWITCH = "true"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "false"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_ENABLED = "false"
$env:TRADING_ALLOW_LIVE_SIM = "false"
```

확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/status
Invoke-RestMethod http://127.0.0.1:8000/api/gateway/commands/status
```

## 중단 기준

- Kiwoom server가 REAL로 감지됨
- safety gate 실패
- idempotency mismatch
- `metadata.live_real_allowed=true`
- `cancel_order` 또는 `modify_order` command 생성
- command가 DISPATCHED에서 오래 멈춤
- Chejan parser가 계좌/종목/주문번호를 해석하지 못함

## 성공 기준

- simulation heartbeat로 safety gate 통과
- single LIVE_SIM intent/order command 생성
- Real Kiwoom Gateway가 `SendOrder`를 한 번만 호출
- `command_ack`가 저장됨
- Chejan 또는 execution evidence가 저장됨
- pilot 후 kill switch와 routing disable 확인

