# 주문 브로커 경계 Runbook

## 목적

`gateway_order_broker_boundaries`는 Core가 주문 명령을 claim한 시점부터 Gateway 시작, durable DB pre-ack, 브로커 접수, Chejan 확인까지를 명시적으로 기록한다. 이 기능은 LIVE_SIM/LIVE_REAL을 활성화하지 않으며 실제 주문 검증을 수행하지 않는다.

## 상태 계약

- `CLAIMED`: Core가 주문 명령을 Gateway에 전달 대상으로 claim했다.
- `GATEWAY_STARTED`: Gateway handler가 명령 처리를 시작했다.
- `PRE_ACK_RECORDED`: 주문 호출 직전 `order_pre_ack`가 Core DB에 commit됐다.
- `BROKER_ACCEPTED`: Kiwoom `SendOrder` 반환이 접수 성공을 나타냈다.
- `CHEJAN_CONFIRMED`: 주문 Chejan 또는 execution event가 확인됐다.
- `UNCONFIRMED`: claim/pre-ack 이후 제한 시간 안에 브로커 확인이 없어 reconcile이 필요하다.

상태 이벤트는 늦거나 순서가 바뀌어 도착해도 하위 상태로 되돌아가지 않는다. `UNCONFIRMED`는 늦은 broker ack/Chejan으로만 회복한다.

## Durable pre-ack

Gateway는 `send_order`와 `cancel_order` 모두 다음 순서를 지킨다.

1. `command_started`를 생성한다.
2. 선택적으로 local JSONL pre-ack journal을 append한다.
3. Core `/api/gateway/events`에 `order_pre_ack`를 동기 POST한다.
4. 응답의 `accepted=true`와 `durable_pre_ack_recorded=true`를 모두 확인한다.
5. 확인된 경우에만 Kiwoom client의 주문 메서드를 호출한다.

Core timeout, HTTP 오류, malformed 응답, DB commit 미확인 시 `DURABLE_DB_PRE_ACK_FAILED`로 fail closed하며 broker 메서드는 호출하지 않는다. 비동기 Core worker queue는 이 경계에 사용하지 않는다.

durable pre-ack 확인 뒤 Kiwoom broker method 자체가 예외를 던지면 접수 여부를 단정할 수 없다. 이 경우 `order_broker_unconfirmed`를 기록해 command/boundary를 즉시 `UNCONFIRMED`로 전환하고 broker snapshot/Chejan reconcile 전까지 신규 BUY routing을 차단한다.

## Startup migration

Schema migration은 savepoint 안에서 table/index를 생성하고 기존 `gateway_commands`, `gateway_events`, `gateway_command_events`를 결정적 순서로 backfill한다. 과거 `ACKED`만 있고 `order_pre_ack` evidence가 없으면 `BROKER_ACCEPTED` 상태는 보존하지만 durable pre-ack 시각을 만들지 않는다. 이 경우 status는 `DURABLE_PRE_ACK_GAP`으로 FAIL이다.

기존 `UNCONFIRMED`는 임의로 해소하지 않는다. 운영자 reconcile이 필요한 WARN이며 신규 주문 routing 차단 상태를 유지한다.

## API 확인

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/gateway/order-broker-boundaries/status" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}

Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/gateway/order-broker-boundaries?limit=100" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

정상 기준:

- `status=PASS`, 또는 기존 미확정 건만 존재하는 `WARN`
- `table_exists=true`
- `required_indexes_present=true`
- `missing_boundary_count=0`
- `durable_pre_ack_gap_count=0`
- `duplicate_idempotency_count=0`
- `command_state_mismatch_count=0`
- `UNCONFIRMED>0`이면 `block_new_order_routing=true`

Dashboard fast path:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=order_broker_boundaries,pipeline_summary" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

## OBSERVE 점검

Core만 OBSERVE-safe 환경으로 실행한다. Gateway를 시작하거나 `/api/gateway/commands`를 polling하지 않는다.

```powershell
$env:TRADING_MODE="OBSERVE"
$env:TRADING_ALLOW_LIVE_SIM="false"
$env:TRADING_ALLOW_LIVE_REAL="false"

python -m tools.ops_order_broker_boundary_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --out-dir reports/order_broker_boundary
```

점검 도구는 read-only API만 호출하며 command를 enqueue/poll하지 않는다. 시작 전후 전체 command 수와 order command 수가 모두 동일해야 한다.

## 장애 처리

- `DURABLE_PRE_ACK_GAP`: 신규 주문 routing을 중지하고 원본 event/journal을 대조한다. 누락 evidence를 추정 생성하지 않는다.
- `UNCONFIRMED_ORDER_BOUNDARY_REQUIRES_RECONCILE`: broker snapshot/Chejan과 수동 reconcile evidence를 확보할 때까지 WARN과 routing block을 유지한다.
- `ORDER_BOUNDARY_IDEMPOTENCY_DUPLICATE`: 운영 DB row를 삭제하지 말고 복제 DB에서 migration을 재현한다.
- `ORDER_COMMAND_BOUNDARY_STATE_MISMATCH`: command event 순서와 terminal 상태를 확인하고 Core를 OBSERVE로 유지한다.

운영 DB 삭제, destructive migration, 실제 주문/취소 호출은 금지한다. 재현과 replay는 주문 side effect를 차단한 복제 DB에서만 수행한다.
