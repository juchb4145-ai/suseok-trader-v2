# LIVE_SIM Enablement

## 요약

PR12 `LIVE_SIM`은 Kiwoom mock-trading acceptance test를 위한 simulation-account-only path다. 기본 disabled, manual only, safety-gated이며 DRY_RUN과 LIVE_REAL과 분리된다. LIVE_SIM은 모의투자 전용이며 실계좌 주문이 아니다. `LIVE_REAL`은 현재 구현되어 있지 않으며 별도 future safety project다.

## Boundary

- LIVE_SIM은 Gateway command queue를 통해서만 broker command를 enqueue할 수 있다.
- 허용 command는 safety gate를 통과한 `send_order`로 제한된다.
- `cancel_order`와 `modify_order`는 disabled다.
- DRY_RUN evidence는 prerequisite이 될 수 있지만 DRY_RUN이 LIVE_SIM intent를 자동 생성하지 않는다.
- AI Sidecar, RCA, Codex prompt draft, LIVE_SIM Review report는 review-only이며 order input이 아니다.
- Dashboard는 read-only이며 buy, sell, cancel, modify, reconcile, queue button이 없다.

## Safety Gate

`check_live_sim_safety_gate(connection, settings)`는 다음을 확인한다.

- `TRADING_MODE=LIVE_SIM` 또는 explicit LIVE_SIM enablement
- `TRADING_ALLOW_LIVE_SIM=true`
- `TRADING_ALLOW_LIVE_REAL=false`
- `LIVE_SIM_ENABLED=true`
- `LIVE_SIM_ORDER_ROUTING_ENABLED=true`
- `LIVE_SIM_GATEWAY_COMMAND_ENABLED=true`
- `LIVE_SIM_KILL_SWITCH=false`
- fresh Gateway heartbeat
- Gateway orderable status
- simulation-like configured/reported account/server/broker mode
- configured simulation account ID
- positive notional and daily limits
- daily order count remaining
- Gateway queue healthy
- AI tools and AI order tools disabled

실패하면 intent와 Gateway command가 생성되지 않는다. rejection은 `live_sim_rejections`에 기록된다.

## Eligibility

`evaluate_live_sim_eligibility`는 추가로 다음을 요구한다.

- Candidate가 `CONTEXT_READY`
- latest Strategy가 `MATCHED_OBSERVATION`
- latest Risk가 `OBSERVE_PASS`
- latest tick 존재 및 fresh
- `LIVE_SIM_REQUIRE_DRY_RUN_EVIDENCE=true`이면 DRY_RUN evidence 존재
- same code duplicate active LIVE_SIM intent/order 없음
- per-order notional, daily count, daily notional, active order, active position limit 통과
- buy allowed
- market order disabled by default
- sell/exit-sell disabled by default

`MATCHED_OBSERVATION`은 매수 신호가 아니고 `OBSERVE_PASS`는 주문 승인이 아니다. LIVE_SIM eligibility는 모의투자 전용 gate다.

## PR-4 OrderPlanDraft Pilot

PR-4는 `OrderPlanDraft(PLAN_READY)`를 기존 candidate 기반 manual LIVE_SIM 경로와 별도로 평가한다. `OrderPlanDraft`는 여전히 주문 의도가 아니며, LIVE_SIM OrderPlan eligibility가 계좌/heartbeat/kill switch/중복/한도/fresh tick/price drift/strategy/risk를 다시 확인한 뒤에만 `LiveSimIntent`가 된다.

추가 설정은 기본 disabled다.

- `TRADING_PROFILE=LIVE_SIM_PILOT`
- `LIVE_SIM_PILOT_PIPELINE_ENABLED=true`
- `LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=true`
- `LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=true`일 때만 run_once command queue 가능
- `LIVE_SIM_ORDER_PLAN_REQUIRE_DRY_RUN_EVIDENCE=false`가 기본이며, 운영자가 true로 바꾸면 dry-run evidence를 요구한다.

자세한 절차는 [LIVE_SIM Pilot Pipeline from OrderPlanDraft](live_sim_pilot_pipeline_ko.md)를 따른다.

## Required Environment for Local Acceptance

```powershell
$env:TRADING_PROFILE = "LIVE_SIM_PILOT"
$env:TRADING_MODE = "LIVE_SIM"
$env:TRADING_ALLOW_LIVE_SIM = "true"
$env:TRADING_ALLOW_LIVE_REAL = "false"
$env:LIVE_SIM_ENABLED = "true"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "true"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "true"
$env:LIVE_SIM_ACCOUNT_ID = "SIM-ACCOUNT-ID"
$env:LIVE_SIM_KILL_SWITCH = "false"
```

## Command Flow

1. API 또는 CLI로 eligibility 평가
2. `LiveSimIntent` 생성
3. intent에서 `LiveSimOrderRecord` queue
4. Gateway가 command polling
5. Mock Gateway가 `command_started`, `command_ack` 전송
6. optional mock execution event가 `live_sim_executions` row 기록
7. reconcile이 local-only `live_sim_reconcile_snapshots` row 저장

Queued command 조건:

- `command_type=send_order`
- `source=live_sim`
- `mode=LIVE_SIM`
- `live_mode=LIVE_SIM`
- command-level and payload `idempotency_key`
- payload `live_sim_only=true`
- payload `live_real_allowed=false`
- simulation-like `account_mode`, `broker_env`, `server_mode`
- `metadata.live_sim_only=true`
- `metadata.live_real_allowed=false`
- `metadata.live_sim_intent_id`
- PR12에서는 BUY side only

## DB Tables

- `live_sim_intents`
- `live_sim_orders`
- `live_sim_executions`
- `live_sim_rejections`
- `live_sim_runs`
- `live_sim_reconcile_snapshots`
- `live_sim_errors`

## API

Read-only:

- `GET /api/live-sim/status`
- `GET /api/live-sim/intents`
- `GET /api/live-sim/intents/{live_sim_intent_id}`
- `GET /api/live-sim/orders`
- `GET /api/live-sim/orders/{live_sim_order_id}`
- `GET /api/live-sim/executions`
- `GET /api/live-sim/rejections`
- `GET /api/live-sim/reconcile`
- `GET /api/live-sim/errors`

Manual local-token protected:

- `POST /api/live-sim/evaluate`
- `POST /api/live-sim/intents/from-candidate/{candidate_instance_id}`
- `POST /api/live-sim/orders/from-intent/{live_sim_intent_id}`
- `POST /api/live-sim/intents/from-order-plan/{order_plan_id}`
- `POST /api/live-sim/orders/from-order-plan/{order_plan_id}`
- `POST /api/live-sim/pilot/run-once?queue_commands=false`
- `POST /api/live-sim/reconcile`

POST response는 `live_sim_only=true`, `live_real_allowed=false`, `broker_order_path=LIVE_SIM_ONLY`, `real_order_allowed=false`를 포함한다.

## CLI

```powershell
python tools/evaluate_live_sim_eligibility.py --candidate-instance-id CANDIDATE_ID
python tools/create_live_sim_intent.py --candidate-instance-id CANDIDATE_ID
python tools/queue_live_sim_order.py --live-sim-intent-id live_sim_intent_x
python -m tools.evaluate_live_sim_order_plan --order-plan-id OPD-...
python -m tools.create_live_sim_intent_from_order_plan --order-plan-id OPD-...
python -m tools.run_live_sim_pilot_once
python -m tools.run_live_sim_pilot_once --queue-commands
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python tools/reconcile_live_sim.py
```

CLI는 safety gate를 우회하지 않고 LIVE_REAL order를 만들지 않는다.

## Mock Gateway Acceptance

Mock Gateway는 LIVE_SIM metadata가 있고 simulation-like인 `send_order`만 수락한다. `command_started`, `command_ack`를 mock broker order number와 함께 전송한다. Core는 PyQt5/QAxWidget을 import하지 않고 Kiwoom real order API를 호출하지 않는다.

## Reconcile Policy

PR12 reconcile은 local-only다. local open LIVE_SIM order와 Gateway command state를 비교해 `LOCAL_ONLY` 또는 `RECONCILE_MISMATCH`를 저장한다. broker account snapshot은 별도 simulation-account reconcile command review 전까지 unavailable로 표시한다.

## Rollback

```powershell
$env:LIVE_SIM_KILL_SWITCH = "true"
$env:LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND = "false"
$env:LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED = "false"
$env:LIVE_SIM_PILOT_PIPELINE_ENABLED = "false"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "false"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_ENABLED = "false"
$env:TRADING_ALLOW_LIVE_SIM = "false"
Invoke-RestMethod http://127.0.0.1:8000/api/live-sim/status
```

추가 확인:

- `live_sim_orders`
- `gateway_commands`
- `live_sim_reconcile_snapshots`
- `live_sim_errors`

## 운영자 체크포인트

- LIVE_SIM은 모의투자 전용이다.
- LIVE_REAL은 현재 구현되어 있지 않다.
- `send_order`는 LIVE_SIM service safety-gated path에서만 허용된다.
- `cancel_order`, `modify_order`는 disabled다.
- Dashboard에는 LIVE_SIM 실행 버튼이 없다.
