# SQLite Lock Contention Runbook

목적: 장중 Gateway ingest가 많은 상태에서 operator run-once가 SQLite write lock과 경합할 때, 이를 Core 장애로 오인하지 않고 재시도 가능한 운영 상태로 분류한다.

## 안전 원칙

- `LIVE_REAL`은 활성화하지 않는다.
- 주문 정책, 매수 gate, safety gate는 완화하지 않는다.
- PR-10.6은 operator 안정화이며 append-only cutover나 주문 동작 변경이 아니다.

## 관측 신호

- `/api/operator/projection-outbox/run-once`
  - 정상: `status=COMPLETED`, `PARTIAL_MAX_WALL_MS`, `NOOP` 등
  - 재시도 가능 락: HTTP `409` 또는 body `status=LOCKED_RETRYABLE`
- `/api/operator/market-data-projection-reconcile/run-once`
  - live-safe 기본: `persist=false`
  - persist 요청 중 lock 발생 후 read-only fallback 성공: `status=WARN_READ_ONLY_RESULT`, `locked_fallback_used=true`
  - fallback도 lock: `status=LOCKED_RETRYABLE`
- `/api/operator/projection-outbox/status`
  - `operator_lock_health`
  - `projection_outbox_processing_stale_count`
  - `recommended_run_once_limit`

## 운영 절차

1. 장중 run-once는 live-safe 기본값을 유지한다.

```powershell
python -m tools.ops_market_data_condition_event_side_effect_check --run-once --limit 100
```

2. 결과가 `LOCKED_RETRYABLE`이면 Core FAIL로 보지 않는다.

- ops verdict가 `WARN`이고 `block_next_pr=true`이면 ingest 압력이 낮아진 뒤 같은 명령을 재실행한다.
- `projection_outbox` pending이 많으면 limit을 낮춰 재시도한다.
- 동일 락이 반복되면 `projection_outbox_processing_stale_count`와 Core error log를 확인한다.

3. reconcile은 장중에 persist를 강제하지 않는다.

- 기본 run-once는 `live_safe=true`에서 read-only로 동작한다.
- persist가 필요한 경우에도 lock이 나면 read-only fallback 결과를 먼저 확인한다.
- `WARN_READ_ONLY_RESULT`는 DB snapshot 저장 실패가 아니라 장중 lock 회피 결과다.

## 조정 가능한 설정

- `OPERATOR_SQLITE_LOCK_RETRY_ATTEMPTS=3`
- `OPERATOR_SQLITE_LOCK_RETRY_BASE_SLEEP_SEC=0.05`
- `OPERATOR_SQLITE_LOCK_RETRY_MAX_SLEEP_SEC=0.5`
- `OPERATOR_SQLITE_BUSY_TIMEOUT_MS=500`
- `OPERATOR_RUN_ONCE_LOCKED_HTTP_STATUS=409`
- `PROJECTION_OUTBOX_LIVE_RUN_ONCE_BATCH_SIZE=50`
- `PROJECTION_OUTBOX_RUN_ONCE_MAX_WALL_MS=5000`
- `MARKET_DATA_RECONCILE_LIVE_DEFAULT_PERSIST=false`
- `MARKET_DATA_RECONCILE_LOCKED_FALLBACK_TO_READ_ONLY=true`
- `OPS_SCRIPT_LOCKED_RETRY_ATTEMPTS=3`
- `OPS_SCRIPT_LOCKED_RETRY_SLEEP_SEC=1.0`

## 다음 조치 기준

- `LOCKED_RETRYABLE`이 1-2회만 발생하고 이후 PASS/WARN이면 다음 PR 검증을 계속할 수 있다.
- 반복적으로 `LOCKED_RETRYABLE`이 소진되면 다음 PR은 보류하고 batch size, max wall time, ingest peak 시간대를 조정한다.
- `PROJECTION_OUTBOX_ERROR_OR_DEAD_LETTER`, `LATEST_RECONCILE_FAIL`, effective skip forbidden 계열은 lock warning이 아니라 실제 검증 실패로 본다.
