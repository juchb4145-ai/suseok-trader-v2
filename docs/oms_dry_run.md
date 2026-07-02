# OMS DRY_RUN

## 요약

PR10 `DRY_RUN` OMS는 내부 모의 회계다. `DryRunIntent`, `DryRunOrder`, `DryRunExecution`, `DryRunPosition`, ledger를 SQLite에 기록할 수 있지만 브로커 주문이 아니며 `GatewayCommand`를 만들지 않는다. 이 기능은 실계좌 주문을 의미하지 않는다.

## DRY_RUN-only 원칙

- `DRY_RUN_OMS_ENABLED=false`가 기본이다.
- `DRY_RUN_INTENT_CREATION_ENABLED=false`가 기본이다.
- `DRY_RUN_SIMULATED_FILL_ENABLED=false`가 기본이다.
- `DRY_RUN_ORDER_ROUTING_ENABLED=false`와 `DRY_RUN_GATEWAY_COMMAND_ENABLED=false`를 유지한다.
- `DRY_RUN_ALLOW_SHORT=false`를 유지한다.
- DRY_RUN record는 `dry_run_only=true`, `live_order_allowed=false`, `gateway_command_allowed=false`, `broker_order_sent=false`를 가진다.

DRY_RUN은 LIVE_SIM도 아니고 LIVE_REAL도 아니다.

예외적으로 `TRADING_PROFILE=LIVE_SIM_PILOT`, `TRADING_MODE=LIVE_SIM`,
`TRADING_ALLOW_LIVE_SIM=true`, `TRADING_ALLOW_LIVE_REAL=false` 조합에서는 DRY_RUN을
LIVE_SIM prerequisite evidence용 shadow record로 병행 생성할 수 있다. 이 경우에도
`DRY_RUN_ORDER_ROUTING_ENABLED=false`, `DRY_RUN_GATEWAY_COMMAND_ENABLED=false`를 유지하며,
DRY_RUN 자체는 broker 주문이나 Gateway command를 만들지 않는다.

## 하는 일 / 하지 않는 일

| 구분 | 내용 |
| --- | --- |
| 하는 일 | safety gate와 eligibility 통과 시 내부 `DryRunIntent` 기록 |
| 하는 일 | 수동 API/CLI로 `DryRunOrder`, simulated fill, paper position 기록 |
| 하는 일 | PR11부터 DRY_RUN Exit Engine으로 paper position reduce/close accounting |
| 하지 않는 일 | broker order submission |
| 하지 않는 일 | `GatewayCommand` 생성 |
| 하지 않는 일 | account balance, holdings, broker execution mutation |
| 하지 않는 일 | AI/RCA/Codex output을 order input으로 사용 |

## Safety Gate

`check_pr10_safety_gate(connection, settings)`는 다음을 확인한다.

- live flag와 live trading mode disabled
- DRY_RUN routing/gateway command switch disabled
- AI Sidecar tools와 order tools disabled
- Dashboard order controls unavailable
- PR AI-5 safety-review draft 존재 또는 test-only bypass setting

실패하면 intent creation이 차단되고 `dry_run_intent_rejections`에 기록된다.

## Eligibility Rule

내부 intent는 다음 조건이 모두 통과해야 생성된다.

- latest Strategy observation이 `MATCHED_OBSERVATION`
- latest Risk observation이 `OBSERVE_PASS`
- Candidate state가 `CONTEXT_READY`
- latest market tick 존재 및 fresh
- 같은 code의 active DRY_RUN position 없음
- 같은 code의 recent active DRY_RUN intent/order 없음
- daily intent limit, active position limit 통과
- DRY_RUN OMS와 intent creation explicit enabled
- PR10 safety gate 통과

이 조건을 통과해도 live readiness가 아니다. 내부 simulation record만 만든다.

## Lifecycle

1. `evaluate_dry_run_eligibility`가 `dry_run_eligibility_checks` row 저장
2. `create_dry_run_intent`가 `dry_run_intents` row 또는 rejection row 저장
3. `convert_intent_to_dry_run_order`가 `dry_run_orders` row 저장
4. `simulate_fill_dry_run_order`가 `dry_run_executions` 저장, order를 `SIMULATED_FILLED`로 변경, paper position open/update
5. `update_dry_run_positions_mark_to_market`가 latest tick으로 paper PnL 갱신
6. `dry_run_ledger`가 audit event 저장

## DB Tables

- `dry_run_intents`
- `dry_run_orders`
- `dry_run_executions`
- `dry_run_positions`
- `dry_run_ledger`
- `dry_run_eligibility_checks`
- `dry_run_intent_rejections`
- `dry_run_runs`
- `dry_run_errors`

## API

Read-only:

- `GET /api/dry-run/status`
- `GET /api/dry-run/eligibility`
- `GET /api/dry-run/intents`
- `GET /api/dry-run/intents/{dry_run_intent_id}`
- `GET /api/dry-run/orders`
- `GET /api/dry-run/orders/{dry_run_order_id}`
- `GET /api/dry-run/executions`
- `GET /api/dry-run/positions`
- `GET /api/dry-run/ledger`
- `GET /api/dry-run/errors`

Manual token-protected simulation:

- `POST /api/dry-run/evaluate`
- `POST /api/dry-run/intents/from-candidate/{candidate_instance_id}`
- `POST /api/dry-run/orders/from-intent/{dry_run_intent_id}`
- `POST /api/dry-run/orders/{dry_run_order_id}/simulate-fill`
- `POST /api/dry-run/positions/mark-to-market`

`POST /api/orders/enqueue`는 없다.

## CLI

```powershell
python tools/evaluate_dry_run_eligibility.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/create_dry_run_intent.py --candidate-instance-id CAND-2026-06-27-005930-1
python tools/create_dry_run_order.py --dry-run-intent-id dry_run_intent_x
python tools/simulate_dry_run_fill.py --dry-run-order-id dry_run_order_x
python tools/mark_to_market_dry_run_positions.py
```

CLI는 broker를 호출하지 않고 Gateway command row를 만들지 않는다.

## Dashboard Policy

Dashboard는 DRY_RUN status, counts, recent intents/orders, paper positions를 read-only로 표시한다. intent/order/fill/mark-to-market/buy/sell/cancel/modify 버튼이 없다.

## PR11 / PR12 Boundary

PR11 DRY_RUN Exit Engine은 simulated close accounting만 담당한다. PR12 LIVE_SIM은 별도의 simulation-account-only path다. DRY_RUN evidence가 LIVE_SIM prerequisite일 수는 있지만 DRY_RUN이 LIVE_SIM intent/order를 자동 생성하거나 승격하지 않는다.

## 운영자 체크포인트

- `MATCHED_OBSERVATION`과 `OBSERVE_PASS`가 있어도 DRY_RUN은 내부 모의 회계일 뿐이다.
- DRY_RUN은 broker 주문이 아니다.
- DRY_RUN은 `GatewayCommand`를 만들지 않는다.
- DRY_RUN 결과를 LIVE_SIM이나 LIVE_REAL로 자동 승격하지 않는다.
