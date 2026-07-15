# LIVE_SIM Execution Lifecycle, Cancel, Exit, Reconcile

## 범위

PR-5는 Kiwoom OpenAPI+ 모의투자 `LIVE_SIM` 전용 execution lifecycle이다. `LIVE_REAL`은 구현하지 않는다. 실계좌, 실서버, REAL broker mode에서는 `send_order`, `cancel_order`, close-only SELL을 포함한 모든 주문성 command가 safety gate에서 차단되어야 한다.

이번 PR은 scheduler가 아니다. 자동 반복 loop, daemon, while-loop, 5~10초 주기 scheduler는 후속 PR 범위다. 운영은 run_once API/CLI로만 수행한다.

## 허용 command

- BUY: PR-4 OrderPlanDraft path에서만 생성된다.
- cancel_order: TTL이 지난 미체결 BUY 주문 취소 전용이다.
- SELL: open local position을 줄이거나 닫는 close-only exit 전용이다.
- modify_order: 구현하지 않는다.

모든 command payload와 metadata는 `live_sim_only=true`, `live_real_allowed=false`, `broker_order_path=LIVE_SIM_ONLY`, `real_order_allowed=false`를 포함한다.

## DB 상태

추가 테이블:

- `live_sim_positions`
- `live_sim_position_events`
- `live_sim_exit_signals`
- `live_sim_exit_intents`
- `live_sim_cancel_intents`
- `live_sim_lifecycle_events`

`live_sim_executions.execution_key`는 broker execution event 중복 반영을 막는다. 같은 broker execution이 다시 들어오면 수량과 PnL은 재반영하지 않고 lifecycle event로 남긴다.

## Fill Accounting

BUY fill:

- broker ack는 `live_sim_orders.broker_order_no/result/message`에 반영된다.
- partial fill은 `PARTIALLY_FILLED`, full fill은 `FILLED`가 된다.
- local position이 없으면 `OPEN` position을 만든다.
- position이 있으면 quantity와 `avg_entry_price`를 가중평균으로 갱신한다.
- `highest_price`, `lowest_price`, `last_price`, `unrealized_pnl`을 갱신한다.

SELL close fill:

- position 없는 SELL fill은 reconcile mismatch/error로 기록한다.
- sell quantity가 available quantity를 넘으면 position을 `RECONCILE_MISMATCH`로 둔다.
- realized PnL은 `(sell_price - avg_entry_price) * quantity - fee - tax`로 계산한다.
- 남은 quantity가 0이면 `CLOSED`, 남으면 `CLOSING`을 유지한다.

기본 수수료/세금은 `LIVE_SIM_FEE_RATE=0.0`, `LIVE_SIM_TAX_RATE=0.0`이다.

## Cancel 정책

대상:

- BUY order
- `COMMAND_QUEUED`, `COMMAND_DISPATCHED`, `BROKER_ACKED`, `PARTIALLY_FILLED` 등 open 상태
- `remaining_quantity > 0`
- TTL 초과
- broker order no 존재. 예외는 `LIVE_SIM_CANCEL_REQUIRE_BROKER_ORDER_NO=false`와 `LIVE_SIM_CANCEL_ALLOW_WITHOUT_BROKER_ORDER_NO=true`를 함께 명시한 경우만 가능하다.

기본 설정:

```env
LIVE_SIM_CANCEL_ENABLED=false
LIVE_SIM_CANCEL_UNFILLED_ENABLED=false
LIVE_SIM_CANCEL_ORDER_TTL_SEC=60
LIVE_SIM_CANCEL_MAX_COMMANDS_PER_RUN=3
LIVE_SIM_CANCEL_REQUIRE_BROKER_ORDER_NO=true
LIVE_SIM_CANCEL_ALLOW_WITHOUT_BROKER_ORDER_NO=false
LIVE_SIM_CANCEL_KILL_SWITCH=false
```

중복 cancel intent/command는 만들지 않는다.

## Exit Engine

대상은 `OPEN` local position이다. SELL은 신규 매도나 short가 아니라 close-only다.

조건:

- STOP_LOSS: `last_price <= avg_entry_price * (1 - pct/100)`
- TAKE_PROFIT: `last_price >= avg_entry_price * (1 + pct/100)`
- TRAILING_STOP: activation 이상 상승 후 high 대비 trailing pct 하락
- MAX_HOLD: `opened_at` 이후 max hold 초과
- EOD_FLATTEN: 별도 flag가 켜진 경우만

기본 설정:

```env
LIVE_SIM_EXIT_ENGINE_ENABLED=false
LIVE_SIM_EXIT_ORDER_CREATION_ENABLED=false
LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED=false
LIVE_SIM_EXIT_ALLOW_SELL_CLOSE_ONLY=true
LIVE_SIM_EXIT_ALLOW_SHORT=false
LIVE_SIM_EXIT_DEFAULT_ORDER_TYPE=LIMIT
LIVE_SIM_EXIT_ALLOW_MARKET_ORDER=false
LIVE_SIM_EXIT_STOP_LOSS_PCT=3.0
LIVE_SIM_EXIT_TAKE_PROFIT_PCT=5.0
LIVE_SIM_EXIT_TRAILING_STOP_PCT=2.5
LIVE_SIM_EXIT_TRAILING_ACTIVATION_PCT=2.0
LIVE_SIM_EXIT_MAX_HOLD_SEC=1800
LIVE_SIM_EXIT_MIN_HOLD_SEC=30
```

시장가 exit는 기본 금지다. stop 조건이어도 `LIVE_SIM_EXIT_USE_MARKET_FOR_STOP=false`가 기본이다.

## Reconcile

`reconcile_live_sim_orders_and_positions()`는 local open order, gateway command, execution sum, local position quantity, stale order를 비교한다. broker snapshot이 없으면 `LOCAL_ONLY_WITHOUT_BROKER_SNAPSHOT`로 남긴다. mismatch가 있으면 `RECONCILE_MISMATCH`와 `blocking_new_buy=true`를 기록한다.

기본 정책:

```env
LIVE_SIM_RECONCILE_ENABLED=true
LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED=false
LIVE_SIM_RECONCILE_BLOCK_NEW_BUY_ON_MISMATCH=true
LIVE_SIM_RECONCILE_ALLOW_EXIT_ON_MISMATCH=true
LIVE_SIM_RECONCILE_STALE_ORDER_SEC=300
```

reconcile mismatch는 신규 BUY를 차단한다. exit/cancel은 포지션 보호 목적이므로 기본적으로 허용 가능하다.

## API

Read-only:

- `GET /api/operator/live-sim/execution-lifecycle/status`
- `GET /api/live-sim/positions`
- `GET /api/live-sim/positions/{position_id}`
- `GET /api/live-sim/exit-signals`
- `GET /api/live-sim/cancel-intents`
- `GET /api/live-sim/lifecycle-events`
- `GET /api/live-sim/reconcile/latest`

Local-token protected:

- `POST /api/live-sim/cancel/run-once?dry_run=false&queue_commands=false`
- `POST /api/live-sim/exit/run-once?dry_run=false&queue_commands=false`
- `POST /api/live-sim/reconcile`
- `POST /api/live-sim/reconcile/request-broker-snapshot`은 기본 disabled다.

## CLI

```powershell
python -m tools.run_live_sim_cancel_unfilled_once --dry-run
python -m tools.run_live_sim_cancel_unfilled_once --queue-commands
python -m tools.run_live_sim_exit_once --dry-run
python -m tools.run_live_sim_exit_once --queue-commands
python -m tools.reconcile_live_sim
python -m tools.inspect_live_sim_positions --limit 50
python -m tools.inspect_live_sim_lifecycle --limit 50
```

CLI에서 `--queue-commands`를 명시하지 않으면 cancel/exit command는 queue하지 않는다. 그래도 설정 flag와 safety gate가 꺼져 있으면 reject된다.

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
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_ENABLED = "false"
```

확인:

- `GET /api/live-sim/status`
- `GET /api/live-sim/lifecycle-events`
- `GET /api/live-sim/reconcile/latest`
- `gateway_commands`에 신규 `send_order`/`cancel_order`가 없는지 확인

## 운영 원칙

- 미체결 BUY 방치는 금지한다.
- 손절 조건은 exit signal 우선 원칙으로 기록한다.
- Gateway는 payload 검증과 broker API 호출만 담당한다.
- Dashboard에는 buy/sell/cancel 버튼을 추가하지 않는다.
