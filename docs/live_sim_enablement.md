# LIVE_SIM Enablement

## 요약

`LIVE_SIM`은 Kiwoom mock-trading acceptance test와 모의투자 파일럿을 위한 simulation-account-only path다. 기본 disabled, manual/run_once only, safety-gated이며 DRY_RUN과 LIVE_REAL과 분리된다. LIVE_SIM은 모의투자 전용이며 실계좌 주문이 아니다. `LIVE_REAL`은 현재 구현되어 있지 않으며 별도 future safety project다.

## Boundary

- LIVE_SIM은 Gateway command queue를 통해서만 broker command를 enqueue할 수 있다.
- BUY command는 PR-4 `send_order` path에서만 생성된다.
- `cancel_order`는 PR-5 미체결 BUY TTL 취소 전용이다.
- SELL `send_order`는 PR-5 open position close-only exit 전용이다.
- `modify_order`는 disabled다.
- DRY_RUN evidence는 prerequisite이 될 수 있지만 DRY_RUN이 LIVE_SIM intent를 자동 생성하지 않는다.
- `LIVE_SIM_PILOT`에서는 같은 profile 안에서 DRY_RUN shadow evidence와 LIVE_SIM intent를 순차 생성할 수 있다.
- AI Sidecar, RCA, Codex prompt draft, LIVE_SIM Review report는 review-only이며 order input이 아니다.
- AI Candidate Scorer는 advisory-only이며 score/confidence/risk_reward가 LIVE_SIM routing input이 아니다.
- Dashboard는 read-only이며 buy, sell, cancel, modify, reconcile, queue button이 없다.

## Safety Gate

`TRADING_PROFILE=LIVE_SIM_PILOT` is the profile-level capability selector for LIVE_SIM pilot
work. The older `TRADING_MODE` and `TRADING_ALLOW_LIVE_SIM` flags are still read as
compatibility/safety switches, but they are not the source of truth for capabilities. Keep
`TRADING_ALLOW_LIVE_REAL=false`; that flag is a safety lock, not a deprecated toggle.

`check_live_sim_safety_gate(connection, settings)`는 다음을 확인한다.

- `TRADING_PROFILE=LIVE_SIM_PILOT`
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

## PR-6 AI Candidate Scorer Advisory

AI Candidate Scorer는 LIVE_SIM safety gate 앞이나 뒤에서 주문 결정을 대체하지 않는다. 최신 advisory summary는 dashboard와 pilot run 결과에 표시될 수 있지만, `LiveSimIntent`, `LiveSimOrderRecord`, `GatewayCommand` 생성 조건에 포함되지 않는다.

AI failure는 pipeline failure가 아니다. invalid schema, timeout, provider failure는 `ai_advisory_errors`와 scoring run status로 남기고 기존 LIVE_SIM path는 계속 fail-open으로 동작한다.

Rollback:

```powershell
$env:AI_CANDIDATE_SCORER_ENABLED = "false"
$env:AI_CANDIDATE_SCORER_PROVIDER = "mock"
$env:AI_CANDIDATE_SCORER_ATTACH_TO_ORDER_PLAN = "false"
$env:AI_CANDIDATE_SCORER_ATTACH_TO_LIVE_SIM_RUN = "false"
```

## Required Environment for Local Acceptance

```powershell
$env:TRADING_PROFILE = "LIVE_SIM_PILOT"
# legacy compatibility/safety switches below; profile remains the capability source.
$env:TRADING_MODE = "LIVE_SIM"
$env:TRADING_ALLOW_LIVE_SIM = "true"
$env:TRADING_ALLOW_LIVE_REAL = "false"
$env:LIVE_SIM_ENABLED = "true"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "true"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "true"
$env:LIVE_SIM_ACCOUNT_ID = "SIM-ACCOUNT-ID"
$env:LIVE_SIM_KILL_SWITCH = "false"
```

## PR-5 Execution Lifecycle

PR-5는 매수 이후 운영을 위해 broker ack/reject/execution event, partial/full fill, local position, TTL cancel, stop-loss/take-profit/trailing/max-hold exit, reconcile block을 추가한다.

자세한 내용은 [LIVE_SIM Execution Lifecycle](live_sim_execution_lifecycle_ko.md)를 따른다.

중요 원칙:

- PR-5는 LIVE_SIM 모의투자 전용이다.
- LIVE_REAL은 구현하지 않는다.
- scheduler/daemon/while-loop는 구현하지 않는다.
- run_once API/CLI로만 cancel/exit/reconcile을 실행한다.
- 모든 주문성 command는 safety gate, simulation account/server, kill switch, heartbeat, duplicate, limit를 확인한다.
- 미체결 BUY 방치 금지와 손절 조건 우선 원칙을 따른다.

## Command Flow

1. API 또는 CLI로 eligibility 평가
2. `LiveSimIntent` 생성
3. intent에서 `LiveSimOrderRecord` queue
4. Gateway가 command polling
5. Mock Gateway가 `command_started`, `command_ack` 전송
6. broker ack/reject/execution event가 order lifecycle에 반영
7. BUY fill이면 local position 생성/갱신
8. TTL 초과 BUY 미체결이면 optional `cancel_order` command
9. exit signal이면 optional close-only SELL `send_order` command
10. reconcile이 `live_sim_reconcile_snapshots` row 저장

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
- BUY side는 PR-4 path에서만 허용
- SELL side는 `close_only=true`, `position_id`, `exit_intent_id`가 있는 PR-5 exit path에서만 허용
- cancel은 `command_type=cancel_order`, `side=BUY_CANCEL`, original broker order no가 있는 PR-5 cancel path에서만 허용

## DB Tables

- `live_sim_intents`
- `live_sim_orders`
- `live_sim_executions`
- `live_sim_positions`
- `live_sim_position_events`
- `live_sim_exit_signals`
- `live_sim_exit_intents`
- `live_sim_cancel_intents`
- `live_sim_lifecycle_events`
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
- `GET /api/live-sim/reconcile/latest`
- `GET /api/live-sim/positions`
- `GET /api/live-sim/positions/{position_id}`
- `GET /api/live-sim/exit-signals`
- `GET /api/live-sim/cancel-intents`
- `GET /api/live-sim/lifecycle-events`
- `GET /api/live-sim/errors`

Manual local-token protected:

- `POST /api/live-sim/evaluate`
- `POST /api/live-sim/intents/from-candidate/{candidate_instance_id}`
- `POST /api/live-sim/orders/from-intent/{live_sim_intent_id}`
- `POST /api/live-sim/intents/from-order-plan/{order_plan_id}`
- `POST /api/live-sim/orders/from-order-plan/{order_plan_id}`
- `POST /api/live-sim/pilot/run-once?queue_commands=false`
- `POST /api/live-sim/reconcile`
- `POST /api/live-sim/cancel/run-once`
- `POST /api/live-sim/exit/run-once`

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
python -m tools.run_live_sim_cancel_unfilled_once --dry-run
python -m tools.run_live_sim_cancel_unfilled_once --queue-commands
python -m tools.run_live_sim_exit_once --dry-run
python -m tools.run_live_sim_exit_once --queue-commands
python -m tools.inspect_live_sim_positions
python -m tools.inspect_live_sim_lifecycle
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python tools/reconcile_live_sim.py
```

CLI는 safety gate를 우회하지 않고 LIVE_REAL order를 만들지 않는다.

## Mock Gateway Acceptance

Mock Gateway는 LIVE_SIM metadata가 있고 simulation-like인 `send_order`만 수락한다. `command_started`, `command_ack`를 mock broker order number와 함께 전송한다. Core는 PyQt5/QAxWidget을 import하지 않고 Kiwoom real order API를 호출하지 않는다.

## Reconcile Policy

PR-5 reconcile은 local open LIVE_SIM order, Gateway command state, execution sum, local position state를 비교한다. broker account snapshot이 없으면 `LOCAL_ONLY_WITHOUT_BROKER_SNAPSHOT`로 저장한다. mismatch가 있으면 `RECONCILE_MISMATCH`, `blocking_new_buy=true`가 되며 신규 BUY eligibility를 차단한다.

## Rollback

```powershell
$env:LIVE_SIM_KILL_SWITCH = "true"
$env:LIVE_SIM_CANCEL_ENABLED = "false"
$env:LIVE_SIM_CANCEL_UNFILLED_ENABLED = "false"
$env:LIVE_SIM_EXIT_ENGINE_ENABLED = "false"
$env:LIVE_SIM_EXIT_ORDER_CREATION_ENABLED = "false"
$env:LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED = "false"
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
- `cancel_order`는 미체결 BUY TTL 취소 전용이다.
- SELL은 open position close-only exit 전용이다.
- `modify_order`는 disabled다.
- Dashboard에는 LIVE_SIM 실행 버튼이 없다.
