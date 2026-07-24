# Gateway Transport Surface

## 요약

Gateway Transport는 future Gateway process와 Core 사이의 HTTP/SQLite 경계다. Gateway는 broker event를 Core로 보내고, Core가 queue한 command를 polling한다. 이 surface 자체는 전략, 리스크, OMS, AI, 주문 실행을 하지 않는다.

## Boundary

| 방향 | Endpoint | 의미 |
| --- | --- | --- |
| Gateway -> Core | `POST /api/gateway/events` | broker-neutral event ingest |
| Gateway -> Core | `POST /api/gateway/events/batch` | 최대 200건의 at-least-once event ingest |
| Core -> Gateway | `GET /api/gateway/commands` | queued command long-poll |
| Operator/Test | `GET /api/gateway/status` | transport 상태 조회 |
| Operator/Test | `GET /api/gateway/events/recent` | 최근 event 조회 |
| Operator/Test | `GET /api/gateway/commands/status` | command 상태 count 조회 |

Core는 `domain/broker`의 broker-neutral envelope만 받는다. Future Kiwoom/PyQt code는 Core process 밖에 있어야 한다.

## Mock Gateway Flow

`python -m apps.mock_gateway --once`는 다음 순서로 한 번 실행된다.

1. `heartbeat` 전송
2. `price_tick` 전송
3. `condition_event` 전송
4. `GET /api/gateway/commands` polling
5. 허용된 command를 mock handler로 처리
6. `command_started`, command별 mock event, `command_ack` 또는 `command_failed` 전송

Loop mode는 heartbeat와 deterministic price tick을 주기적으로 보내고 command polling을 계속한다. 기본 loop는 `execution_event`를 만들지 않는다.

## Event Ingest

`POST /api/gateway/events`는 `GatewayEvent` envelope을 받는다.

저장 위치:

- `raw_events`: 원본 ingest table, `payload_hash`, `duplicate_count` 포함
- `gateway_events`: transport-facing event table, status와 error text 포함

Known payload event type은 broker-neutral model로 검증된다.

- `price_tick`
- `condition_event`
- `tr_response`
- `execution_event`

Unknown event type은 버리지 않고 `UNKNOWN_EVENT_TYPE`으로 저장한다.

### Backlog와 SQLite lock

Gateway ingest 연결은 일반 Core 연결의 15초 대기 대신 250ms busy timeout과
50/100/200ms의 제한된 lock 재시도를 사용한다. SQLite writer 또는 Core 내부
event write lock을 제한 시간 안에 얻지 못하면 HTTP `503`과
`status=LOCKED_RETRYABLE`, `reason_codes=["SQLITE_DATABASE_LOCKED"]`를 반환한다.
DB 경로와 원본 SQLite 오류 문자열은 응답에 노출하지 않는다.
단일 요청의 모든 open/lock/retry는 공통 5초 write budget을 사용한다.

- `price_tick`/`condition_event` fast batch는 한 transaction으로 처리한다. lock
  충돌 시 전체 transaction을 rollback한 뒤 같은 batch를 재시도한다.
- 단건 및 non-fast batch는 commit 결과가 불명확할 수 있으므로
  `commit_outcome=UNKNOWN_RETRY_SAME_EVENT_ID`를 반환한다. Gateway는 event를
  버리지 않고 동일 `event_id`로 재전송하며 Core의 idempotency가 중복 side
  effect를 막는다. raw event 뒤 outbox 생성이 실패한 경우에도 duplicate
  재전송이 idempotent outbox enqueue를 다시 수행한다.
- mixed batch가 일부를 이미 반영했다면 503 detail의
  `batch_commit_state=PARTIAL_PREFIX_COMMITTED`, `committed_event_ids`,
  `retry_event_id`로 재전송 범위를 오해하지 않게 한다. Gateway는 여전히
  at-least-once 계약에 따라 원래 batch를 그대로 보존한다.
- Gateway worker의 연속 POST 실패 재시도는 기본 0.2초에서 시작해 2배씩
  늘어나며 최대 5초다. 성공하면 즉시 초기화된다. backoff 중에도 command
  polling deadline은 유지하고 CPU busy-spin을 하지 않는다.
  `/api/gateway/status`의 `core_io_worker_consecutive_post_error_count`로 현재
  연속 실패 횟수를 확인할 수 있다.
- Candidate condition/theme source는 event 내부 SAVEPOINT 원자성을 유지한 채
  10건마다 commit한다. 각 commit 직전 runtime execution fence를 같은
  transaction에서 검증하므로 stale owner의 chunk를 rollback한다.
- Theme leadership candidate source는 매 rebuild마다 source ID가 달라질 수
  있으므로 부분 chunk commit을 사용하지 않고 전체 event 묶음을 한
  transaction으로 유지한다.

## Market Projection

accepted non-duplicate Gateway event는 PR4 Market Data Service projection으로 이어진다.

- `price_tick`: latest tick, tick sample, minute bars, VWAP, freshness input
- `condition_event`: append-only signal과 latest condition state
- `tr_response`: market observation snapshot

projection 실패는 `market_projection_errors`에 남긴다. 실패가 주문, 전략, 리스크, OMS 동작을 만들지 않는다.

Rebuild:

```powershell
python -m tools.rebuild_market_data_projection --clear-projection
```

## Command Polling

`GET /api/gateway/commands` parameter:

- `limit`: default `20`, maximum `100`
- `wait_sec`: default `0`, maximum `5`

polling은 `QUEUED` command를 선택하고 `DISPATCHED`로 바꾸며 `attempts`와 `dispatched_at`을 기록한다.

public enqueue endpoint는 없다. 내부 service와 tests만 `storage.gateway_command_store.enqueue_command()`로 허용 command를 넣는다.

## Command Lifecycle

| State | 운영자 해석 |
| --- | --- |
| `QUEUED` | Gateway가 아직 가져가지 않음 |
| `DISPATCHED` | Gateway로 전달됨 |
| `UNCONFIRMED` | 주문 command가 Gateway로 전달됐으나 ack/failure/execution 이벤트가 시간 내 돌아오지 않아 broker TR/저널 대사가 필요함 |
| `ACKED` | Gateway가 정상 처리 ack |
| `REJECTED` | safety/policy에 의해 거부 |
| `FAILED` | Gateway 처리 실패 |
| `EXPIRED` | Gateway가 가져가기 전에 TTL 만료 |
| `CANCELLED` | future cancellation 예약 |

`command_started`, `command_ack`, `command_failed` event가 `command_id`를 포함하면 `gateway_commands`와 `gateway_command_events`가 업데이트된다.

### TTL / Expired / Unstarted

| 조건 | 전이 | 의미 |
| --- | --- | --- |
| `QUEUED` + `expires_at <= now` + `dispatched_at IS NULL` | `EXPIRED` | Gateway가 command를 보지 못한 상태로 만료. broker 도달 없음. |
| `DISPATCHED` + non-order command timeout | `FAILED` | transport 처리 결과가 사라진 것으로 간주. 재시도는 새 command로만 한다. |
| `DISPATCHED` + `send_order`/`cancel_order` timeout | `UNCONFIRMED` | broker 도달 여부를 알 수 없음. pre-ack journal, broker snapshot, chejan/TR 대사로 해소해야 한다. |
| Gateway가 command를 claim했지만 `command_started`도 못 보냄 | `command_failed` event 우선 전송 | Gateway runtime이 미시작 in-flight command를 실패 이벤트로 보상한다. |

Polling은 만료된 `QUEUED` command를 먼저 `EXPIRED`로 정리한 뒤 ready command를 `DISPATCHED`로 claim한다. ready command 정렬은 order-critical command를 최우선으로 둔다.

우선순위:

1. `send_order`, `cancel_order`
2. `heartbeat_request`, `broker_snapshot_request`, `load_conditions`, `send_condition`, `stop_condition`, 기타 control command
3. `register_realtime`, `request_tr`

### Delivery Semantics

Event ingest는 at-least-once다. 같은 `event_id`는 중복 count로 보존하고, command-critical event(`command_started`, `command_ack`, `command_failed`, `rate_limited`, `execution_event`, chejan 계열)는 Gateway buffer에서 drop-protected다.

Command dispatch는 at-most-once를 목표로 한다. `poll_commands()`가 command를 반환하는 순간 `DISPATCHED`와 `attempts += 1`이 기록된다. order command는 timeout 시 `FAILED`가 아니라 `UNCONFIRMED`가 되며, 보상 경로는 다음 순서다.

- pre-ack journal recovery event 재전송
- broker snapshot reconcile
- operator drill 또는 `tools/resolve_live_sim_order.py` 수동 해소
- Core polling stale 시 Gateway dead-man cancel은 미체결 BUY의 cancel-only만 허용

## Token Auth

`TRADING_CORE_TOKEN`이 비어 있으면 local development에서 Gateway write/poll endpoint를 token 없이 호출할 수 있다.

설정되어 있으면 다음 header 중 하나가 필요하다.

- `X-Local-Token: <token>`
- `X-Core-Token: <token>`

`GET /health`, `GET /api/status`, read-only Gateway status/list endpoint는 token-free다.

## Order Command Policy

Order command는 기본적으로 거부된다. LIVE_SIM safety gate의 좁은 예외만 있다.

LIVE_SIM에서 허용되는 조건:

- `command_type=send_order` 또는 PR-5 미체결 BUY TTL 전용 `cancel_order`
- `source=live_sim`
- command와 payload 모두 `idempotency_key` 보유
- `mode=LIVE_SIM`
- `live_mode=LIVE_SIM`
- simulation-like `account_mode`, `broker_env`, `server_mode`
- `metadata.live_sim_only=true`
- `metadata.live_real_allowed=false`
- BUY `send_order`는 `metadata.live_sim_intent_id`와 `side=BUY`
- SELL `send_order`는 open position close-only exit metadata(`position_id`, `exit_intent_id`, short 금지)
- `cancel_order`는 미체결 BUY 원주문 metadata(`cancel_intent_id`, `original_live_sim_order_id`, `original_order_no`)

계속 거부되는 command:

- `submit_order`
- `modify_order`
- `enqueue_order`
- `order_intent`
- `gateway_order`
- `live_order`

`LIVE_SIM`은 모의투자 전용이며 실계좌 주문이 아니다. `LIVE_REAL`은 현재 구현되어 있지 않다.

## 운영자 체크포인트

- Gateway event가 들어오는지 먼저 `/api/gateway/status`에서 heartbeat를 본다.
- command가 `DISPATCHED` 후 `ACKED`로 가는지 확인한다.
- `REJECTED` command는 safety/policy 이유를 먼저 확인한다.
- `send_order`와 `cancel_order`는 LIVE_SIM service safety-gated path 외 생성되면 안 된다.
