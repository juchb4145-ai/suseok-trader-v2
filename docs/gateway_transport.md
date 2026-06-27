# Gateway Transport Surface

## 요약

Gateway Transport는 future Gateway process와 Core 사이의 HTTP/SQLite 경계다. Gateway는 broker event를 Core로 보내고, Core가 queue한 command를 polling한다. 이 surface 자체는 전략, 리스크, OMS, AI, 주문 실행을 하지 않는다.

## Boundary

| 방향 | Endpoint | 의미 |
| --- | --- | --- |
| Gateway -> Core | `POST /api/gateway/events` | broker-neutral event ingest |
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
| `ACKED` | Gateway가 정상 처리 ack |
| `REJECTED` | safety/policy에 의해 거부 |
| `FAILED` | Gateway 처리 실패 |
| `EXPIRED` | 만료 |
| `CANCELLED` | future cancellation 예약 |

`command_started`, `command_ack`, `command_failed` event가 `command_id`를 포함하면 `gateway_commands`와 `gateway_command_events`가 업데이트된다.

## Token Auth

`TRADING_CORE_TOKEN`이 비어 있으면 local development에서 Gateway write/poll endpoint를 token 없이 호출할 수 있다.

설정되어 있으면 다음 header 중 하나가 필요하다.

- `X-Local-Token: <token>`
- `X-Core-Token: <token>`

`GET /health`, `GET /api/status`, read-only Gateway status/list endpoint는 token-free다.

## Order Command Policy

Order command는 기본적으로 거부된다. PR12의 좁은 예외만 있다.

LIVE_SIM에서 허용되는 조건:

- `command_type=send_order`
- `source=live_sim`
- command와 payload 모두 `idempotency_key` 보유
- `mode=LIVE_SIM`
- `live_mode=LIVE_SIM`
- simulation-like `account_mode`, `broker_env`, `server_mode`
- `metadata.live_sim_only=true`
- `metadata.live_real_allowed=false`
- `metadata.live_sim_intent_id`
- PR12에서는 BUY side만

계속 거부되는 command:

- `submit_order`
- `cancel_order`
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
- `send_order`는 LIVE_SIM service safety-gated path 외 생성되면 안 된다.
