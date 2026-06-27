# Broker Gateway/Core Event Contract

## 요약

이 문서는 Gateway와 Core가 주고받는 broker-neutral 데이터 모양을 설명한다. 계약 모델은 **실행이 아니라 데이터 모양**이다. `BrokerOrderRequest`라는 이름이 있어도 이것만으로 주문이 실행되지 않는다.

Core는 Kiwoom, PyQt, QAxWidget concrete runtime을 import하지 않는다. Gateway event와 command는 JSON-compatible dictionary로 직렬화된다.

## 설계 규칙

- 계약은 `to_dict()`와 `from_dict(data)`로 JSON-compatible dictionary를 다룬다.
- timestamp는 내부에서 UTC-aware `datetime`, wire에서는 ISO-8601 string을 쓴다.
- message ID가 없으면 model layer가 생성한다.
- `idempotency_key`는 envelope과 order/result contract에서 보존된다.
- `"71,000"`, `"+1.25%"` 같은 broker-style 숫자 문자열은 domain boundary에서 정규화한다.
- 국내 stock `code`는 `A005930`을 `005930`으로 정규화하고 6자리 형식이 아니면 거부한다.
- 어떤 model도 PyQt, QAxWidget, Kiwoom OpenAPI+ concrete implementation을 import하지 않는다.

## Envelope

### `GatewayEvent` / `EventEnvelope`

Gateway에서 Core로 들어오는 관찰 event다.

| Field | Required | 의미 |
| --- | --- | --- |
| `event_id` | omit 가능 | 없으면 `evt_` prefix ID 생성 |
| `event_type` | Yes | `heartbeat`, `price_tick`, `condition_event`, `tr_response`, `execution_event`, `command_ack`, `gateway_error` 등 |
| `source` | Yes | Gateway 또는 subsystem 식별자 |
| `ts` | omit 가능 | Gateway event timestamp |
| `payload` | Yes | broker-neutral payload dictionary |
| `command_id` | No | command 응답 correlation |
| `idempotency_key` | No | duplicate 방지와 correlation |

### `GatewayCommand` / `CommandEnvelope`

Core에서 Gateway로 전달할 command envelope이다.

| Field | Required | 의미 |
| --- | --- | --- |
| `command_id` | omit 가능 | 없으면 `cmd_` prefix ID 생성 |
| `command_type` | Yes | `request_tr`, `register_realtime`, `load_conditions`, `send_condition` 등 |
| `source` | Yes | 보통 `core`, LIVE_SIM 예외에서는 `live_sim` |
| `ts` | omit 가능 | command creation timestamp |
| `payload` | Yes | broker-neutral command payload |
| `idempotency_key` | No | duplicate 방지 key |

`GatewayCommand`는 통제된 safety gate 없이는 생성 금지다. PR12에서는 LIVE_SIM service가 검증한 `send_order`만 예외적으로 queue할 수 있다.

## Payload Model

| Model | 의미 | 주의 |
| --- | --- | --- |
| `BrokerPriceTick` | 실시간 tick payload | price, volume, spread, day range 검증 |
| `BrokerConditionEvent` | condition search ENTER/EXIT 관찰 | 조건식 관찰이지 주문 판단 아님 |
| `BrokerTrRequest` | broker-neutral TR request | request data shape |
| `BrokerTrResponse` | broker-neutral TR response | market observation snapshot |
| `BrokerOrderRequest` | 주문 의도 payload 모양 | 실행 API가 아니다. 브로커 주문이 아니다. |
| `BrokerOrderResult` | broker-neutral acceptance/rejection 결과 모양 | 실제 주문 path와 연결하려면 별도 safety project 필요 |
| `BrokerExecutionEvent` | broker-neutral 체결 event 모양 | PR12 LIVE_SIM에서는 mock/simulation evidence로만 사용 |

## HTTP Surface

### `POST /api/gateway/events`

Gateway가 `GatewayEvent` envelope을 Core에 전달한다. Core는 canonical JSON payload를 `raw_events`와 `gateway_events`에 저장한다.

```json
{
  "event_id": "evt_heartbeat_1",
  "event_type": "heartbeat",
  "source": "local-gateway",
  "ts": "2026-06-26T09:01:02Z",
  "payload": {"status": "ok"}
}
```

지원 event type:

- `heartbeat`
- `price_tick`
- `condition_event`
- `tr_response`
- `execution_event`
- `command_started`
- `command_ack`
- `command_failed`
- `gateway_error`

알 수 없는 event type은 drop하지 않고 `UNKNOWN_EVENT_TYPE`으로 저장해 운영자가 확인할 수 있게 한다.

### `GET /api/gateway/commands`

Gateway가 Core command queue를 long-poll한다. polling 시 선택된 command는 `QUEUED`에서 `DISPATCHED`로 바뀌고 `attempts`, `dispatched_at`이 기록된다.

```json
{
  "commands": [
    {
      "command_id": "cmd_request_tr_1",
      "command_type": "request_tr",
      "source": "core",
      "ts": "2026-06-26T09:01:02Z",
      "payload": {"tr_code": "OPT10001", "params": {"code": "005930"}},
      "idempotency_key": "tr-005930-once"
    }
  ]
}
```

public command enqueue endpoint는 없다.

## Command 상태

| State | 의미 |
| --- | --- |
| `QUEUED` | Gateway polling 대기 |
| `DISPATCHED` | Gateway에 전달됨 |
| `ACKED` | Gateway가 `command_ack` 전송 |
| `FAILED` | Gateway 실패 event 또는 future handler 실패 |
| `EXPIRED` | dispatch 전 만료 |
| `REJECTED` | policy에 의해 enqueue 거부 |
| `CANCELLED` | future cancellation 예약 상태 |

## Duplicate / Idempotency

- 같은 `event_id`와 같은 `payload_hash`: duplicate로 수락하고 `duplicate_count` 증가
- 같은 `event_id`와 다른 `payload_hash`: `CONFLICT`
- command `idempotency_key`가 있으면 `gateway_command_dedupe_keys`가 중복 생성을 막는다.

## 주문 관련 주의

Order-like command type은 기본적으로 차단된다.

- `submit_order`
- `cancel_order`
- `modify_order`
- `enqueue_order`
- `order_intent`
- `gateway_order`
- `live_order`

PR12에서만 LIVE_SIM service의 safety-gated path가 `send_order`를 제한적으로 queue할 수 있다. 이 경로도 모의투자 전용이며 실계좌 주문이 아니다.

## 운영자 체크포인트

- 계약 이름에 `Order`가 들어가도 실행을 뜻하지 않는다.
- `GatewayCommand`는 public API로 생성하지 않는다.
- duplicate와 idempotency 기록을 확인해 재전송인지 충돌인지 구분한다.
- `LIVE_REAL`은 현재 구현되어 있지 않으며 별도 future safety project다.
