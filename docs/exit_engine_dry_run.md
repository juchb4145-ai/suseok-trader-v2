# DRY_RUN Exit Engine

## 요약

PR11 DRY_RUN Exit Engine은 existing `dry_run_positions`에 대해 deterministic exit evaluation과 simulated close accounting을 수행한다. 실제 매도 주문이 아니다. stop-loss, take-profit, trailing stop, max-hold는 simulated exit signal일 뿐이며 broker sell approval이 아니다.

## DRY_RUN-only 원칙

- `DRY_RUN_EXIT_ENGINE_ENABLED=false`가 기본이다.
- `DRY_RUN_EXIT_INTENT_CREATION_ENABLED=false`가 기본이다.
- `DRY_RUN_EXIT_ORDER_CREATION_ENABLED=false`가 기본이다.
- `DRY_RUN_EXIT_SIMULATED_FILL_ENABLED=false`가 기본이다.
- `DRY_RUN_EXIT_ORDER_ROUTING_ENABLED=false`
- `DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED=false`
- `DRY_RUN_EXIT_ALLOW_SHORT=false`
- `DRY_RUN_EXIT_ALLOW_SELL_CLOSE_ONLY=true`

Exit record는 `dry_run_only=true`, `close_only=true`, `live_order_allowed=false`, `gateway_command_allowed=false`, `broker_order_sent=false`를 가진다.

## Exit Rules

| Signal | 의미 |
| --- | --- |
| `STOP_LOSS` | latest price <= average price * `(1 - stop_loss_pct)` |
| `TAKE_PROFIT` | latest price >= average price * `(1 + take_profit_pct)` |
| `TRAILING_STOP` | position high watermark 대비 drawdown이 threshold 이상 |
| `MAX_HOLD` | position age가 `max_hold_sec` 이상 |
| `DATA_STALE_EXIT_CAUTION` | latest tick age가 stale threshold 초과 |
| `THEME_WEAKENING` | related theme state/coverage/rising ratio 약화 |
| `RISK_DETERIORATION` | latest risk observation이 `OBSERVE_BLOCK` 또는 `OBSERVE_CAUTION` |
| `STRATEGY_INVALIDATED` | latest strategy observation이 `DATA_WAIT`, `NO_SETUP`, `STALE_CONTEXT`, `INVALID_CONTEXT` |
| `MANUAL_REVIEW` | 운영자 검토 placeholder |

모든 signal은 simulated exit signal이다. 실제 매도 주문이 아니다.

## Lifecycle

1. `evaluate_dry_run_exit_for_position`이 active DRY_RUN position, latest tick, optional metrics, theme/strategy/risk context를 읽고 evaluation/signal row 저장
2. `evaluate_all_dry_run_exits`가 active position batch를 평가하고 run/error row 저장
3. `create_dry_run_exit_intent`가 engine/intent flag, PR11 safety gate, closeable position, `EXIT_SIGNAL_OBSERVED` evaluation을 확인하고 내부 intent 생성
4. `convert_exit_intent_to_dry_run_order`가 내부 exit order 생성
5. `simulate_fill_dry_run_exit_order`가 latest tick을 fill price로 사용해 simulated execution 저장, paper position reduce/close

background automatic exit worker는 없다.

## Safety Gate

PR11 safety gate는 PR10 safety gate를 확장해 다음을 확인한다.

- `TRADING_MODE=OBSERVE`
- live flag disabled
- exit order routing disabled
- exit gateway command creation disabled
- sell close-only true
- short selling disabled
- `broker_order_sent=false`
- AI/RCA/Codex output을 automatic exit input으로 사용하지 않음

## DB Tables

- `dry_run_exit_evaluations`
- `dry_run_exit_signals`
- `dry_run_exit_intents`
- `dry_run_exit_orders`
- `dry_run_exit_executions`
- `dry_run_exit_runs`
- `dry_run_exit_errors`
- `dry_run_position_metrics`

기존 `dry_run_ledger`에는 `EXIT_EVALUATION`, `EXIT_INTENT_CREATED`, `EXIT_ORDER_CREATED`, `SIMULATED_EXIT_FILL`, `POSITION_CLOSED`, `POSITION_REDUCED` event가 저장된다.

## API

Read-only:

- `GET /api/dry-run/exits/status`
- `GET /api/dry-run/exits/evaluations`
- `GET /api/dry-run/exits/evaluations/{exit_evaluation_id}`
- `GET /api/dry-run/exits/signals`
- `GET /api/dry-run/exits/intents`
- `GET /api/dry-run/exits/intents/{dry_run_exit_intent_id}`
- `GET /api/dry-run/exits/orders`
- `GET /api/dry-run/exits/orders/{dry_run_exit_order_id}`
- `GET /api/dry-run/exits/executions`
- `GET /api/dry-run/exits/runs`
- `GET /api/dry-run/exits/errors`

Manual local-token protected simulation:

- `POST /api/dry-run/exits/evaluate`
- `POST /api/dry-run/exits/intents/from-position/{dry_run_position_id}`
- `POST /api/dry-run/exits/orders/from-intent/{dry_run_exit_intent_id}`
- `POST /api/dry-run/exits/orders/{dry_run_exit_order_id}/simulate-fill`

POST response도 simulation-only safe flag를 유지한다.

## CLI

```powershell
python tools/evaluate_dry_run_exits.py --dry-run-position-id dry_run_position_x
python tools/evaluate_dry_run_exits.py --trade-date 2026-06-27 --limit 50
python tools/create_dry_run_exit_intent.py --dry-run-position-id dry_run_position_x
python tools/create_dry_run_exit_order.py --dry-run-exit-intent-id dry_run_exit_intent_x
python tools/simulate_dry_run_exit_fill.py --dry-run-exit-order-id dry_run_exit_order_x
python tools/inspect_dry_run_exit.py --exit-evaluation-id dry_run_exit_eval_x
```

CLI는 broker order를 보내지 않고 Gateway command를 만들지 않는다.

## Dashboard Policy

Dashboard는 DRY_RUN section 아래 read-only DRY_RUN Exit panel을 보여준다. sell, close, fill, cancel, modify, exit execution 버튼이 없다.

## PR12 Boundary

PR12 LIVE_SIM은 별도의 simulation-account-only buy-side acceptance path다. PR12는 DRY_RUN exit을 broker exit으로 변환하지 않는다. LIVE_SIM sell과 exit-sell은 later safety review 없이는 disabled다. LIVE_REAL은 disabled다.

## 운영자 체크포인트

- exit signal은 실제 매도 주문이 아니다.
- simulated fill은 paper position 회계 처리다.
- `GatewayCommand`, `BrokerOrderRequest`가 생성되면 안 된다.
- AI/RCA/Codex output으로 exit/order intent를 자동 생성하지 않는다.
