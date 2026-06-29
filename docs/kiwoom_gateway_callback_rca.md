# Kiwoom Gateway Callback RCA

## 요약

`SetRealReg`의 `result_code=0`은 등록 요청 성공이지 `OnReceiveRealData` 수신 성공이 아니다. 이번 장애 후보의 1순위는 FID/SetRealReg 규격 문제가 아니라, Qt main event loop가 Core HTTP long-polling 또는 event POST 동기 호출에 의해 반복적으로 block되어 Kiwoom `QAxWidget`/ActiveX callback을 처리하지 못하는 경로다.

현재 분리 기준은 다음과 같다.

- isolation mode에서 callback이 오고 normal mode에서 안 오면 `CORE_IO_BLOCKING_SUSPECTED`
- isolation mode에서도 `OnReceiveConditionVer`/`OnReceiveRealData`가 안 오면 `ACTIVE_X_CALLBACK_SUSPECTED`
- raw callback은 오지만 price tick이 없으면 parser/FID 계층을 본다

## Blocking 경로 확인

기존 경로:

1. `apps/kiwoom_gateway.py`의 `QTimer`가 Qt main thread에서 `runtime.poll_and_handle_commands` 호출
2. `gateway/kiwoom_runtime.py`가 `CoreClient.poll_commands`를 동기 호출
3. `gateway/core_client.py`가 `GET /api/gateway/commands?wait_sec=<poll_wait_sec>`를 동기 HTTP로 호출
4. `api/routes/gateway.py`가 `storage/gateway_command_store.py::poll_commands` 실행
5. `poll_commands`는 `wait_sec` 동안 sleep loop를 돌 수 있음
6. 같은 main thread의 `flush_events`가 event마다 `CoreClient.post_event`를 동기 호출

따라서 `poll_wait_sec=1`이어도 main event loop가 짧게 계속 점유되고, ActiveX event sink가 callback을 처리할 기회를 잃을 수 있다.

## 작업 분리

| 영역 | 실행 thread | 함수/호출 | blocking 가능성 | 원칙 |
| --- | --- | --- | --- | --- |
| ActiveX main thread work | Qt main thread | `QAxWidget` 생성, signal connect | 낮음 | main thread 고정 |
| ActiveX main thread work | Qt main thread | `CommConnect`, `GetConditionLoad`, `SendCondition`, `SetRealReg` | Kiwoom 내부 대기 가능 | worker에서 직접 호출 금지 |
| ActiveX main thread work | Qt main thread | `OnEventConnect`, `OnReceiveConditionVer`, `OnReceiveRealData`, `OnReceiveTrData`, `OnReceiveChejanData`, `OnReceiveMsg` | event loop가 막히면 미수신 | raw counter를 callback 첫 진입부에서 증가 |
| Core network IO work | Core IO worker thread | `CoreClient.poll_commands` | long-poll `wait_sec` 동안 block 가능 | Qt main thread에서 호출 금지 |
| Core network IO work | Core IO worker thread | `CoreClient.post_event` | HTTP timeout까지 block 가능 | worker event queue로 전달 |
| Command execution | Qt main thread | worker가 받은 `GatewayCommand` drain 후 `KiwoomGatewayCommandHandler.handle` | ActiveX 호출 포함 | main thread `QTimer`가 queue drain |

## Timer/timeout

| 항목 | 값 |
| --- | --- |
| heartbeat timer | `--heartbeat-interval-sec`, 기본 2초 |
| command drain timer | 100ms, worker command queue만 drain |
| event flush timer | 200ms, event를 worker queue 또는 local isolation log로 전달 |
| condition watchdog timer | 1000ms |
| `poll_wait_sec` 기본값 | 1초, worker thread에서만 사용 |
| Core command long-poll server limit | `/api/gateway/commands` `wait_sec` 최대 5초 |
| condition callback timeout | 기본 10초 |
| realtime callback timeout | 기본 15초 |

## 변경된 구조

`gateway/core_io_worker.py`가 Core HTTP IO를 전담한다.

- event posting: runtime event queue -> worker event queue -> `CoreClient.post_event`
- command polling: worker thread -> `CoreClient.poll_commands`
- command execution: worker command queue -> Qt main thread `runtime.drain_core_io_worker`
- queue가 밀리면 `core_io_worker_event_queue_size`, `core_io_worker_command_queue_size`, `core_io_worker_last_error`로 노출

`apps/kiwoom_gateway.py`의 command timer는 더 이상 `runtime.poll_and_handle_commands`를 호출하지 않는다. main thread는 worker command queue를 drain하고, ActiveX `dynamicCall`은 main thread에서만 수행한다.

## Isolation mode

Core 영향과 ActiveX 환경 문제를 분리하려면 Core를 켜지 않고 다음처럼 실행한다.

```powershell
.\venv_32\Scripts\python.exe -m apps.kiwoom_gateway `
  --disable-core-io `
  --observe-only `
  --auto-login `
  --no-threaded-login `
  --realtime-codes 005930,000660
```

이 모드에서는 `CoreClient.post_event`와 `CoreClient.poll_commands`를 호출하지 않는다. heartbeat/event는 Core로 보내지 않고 local drain count로만 남기며, 60초 동안 주기적으로 콘솔에 다음 값을 출력한다.

- `qt_main_thread_id`
- `active_x_thread_id`
- `login_requested_at`
- `on_event_connect_count`
- `condition_load_requested_at`
- `condition_ver_callback_count`
- `realtime_registration_success_count`
- `raw_realtime_callback_count`
- `parsed_price_tick_count`
- `realtime_parse_error_count`
- `latest_condition_ver_callback_at`
- `latest_realtime_callback_at`
- `latest_price_tick_at`

## 확인 순서

1. isolation mode 실행
2. `on_event_connect_count` 확인
3. `condition_ver_callback_count` 확인
4. `realtime_registration_success_count` 확인
5. `raw_realtime_callback_count` 확인
6. normal mode와 isolation mode 차이 비교

판정:

- `realtime_registration_success_count > 0`, `raw_realtime_callback_count = 0`, timeout 전: `REGISTERED_WAITING_CALLBACK`
- 등록 후 timeout: `CALLBACK_TIMEOUT`
- raw callback 수신 중: `CALLBACK_ACTIVE`
- raw callback은 있으나 parser 실패: `PARSE_ERROR`
- main-thread Core IO 관측 뒤 timeout: `CORE_IO_BLOCKING_SUSPECTED`
- isolation mode에서도 callback 없음: `ACTIVE_X_CALLBACK_SUSPECTED`

## 안전 범위

이번 RCA는 callback 수신성과 thread 분리만 다룬다. `LIVE_REAL` 활성화, 주문 flag 활성화, `queue_commands` 기본값 변경, 전략 threshold 조정, FID 감 변경은 하지 않는다.
