# Runtime Execution Lock Runbook

## 목적

`runtime_execution_locks`는 Strategy, Risk, EntryTiming, observe cycle, incremental evaluation, theme refresh의 중복 실행을 막는다. lock은 주문 허가나 trading mode 변경 기능이 아니다.

## Lease 계약

- lock owner는 `owner_id`, `process_id`, `thread_id`로 식별한다.
- `heartbeat_at`과 `expires_at`은 별도 daemon heartbeat가 갱신한다.
- acquisition마다 `runtime_execution_lock_fences.last_fencing_token`이 증가한다.
- transaction write 진입 시 owner와 `fencing_token`이 현재 row와 일치해야 한다.
- TTL이 지났더라도 owner process/thread가 살아 있으면 takeover를 거부한다.
- release나 startup cleanup이 lock row를 삭제해도 fence sequence는 삭제하지 않는다.

## Startup Cleanup

Core startup은 다음 row만 삭제한다.

- 현재 Core process가 소유한 residual row
- TTL이 만료됐고 owner process가 살아 있지 않은 row

다른 process의 active row나 TTL이 지났어도 owner가 살아 있는 row는 삭제하지 않는다. 운영 중 `DELETE FROM runtime_execution_locks` 전체 삭제를 실행하지 않는다.

## API 확인

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/runtime-execution-locks/status" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

정상 기준:

- idle: `status=PASS`, `lock_count=0`
- running: `status=PASS`, `active_count>0`
- active row의 `process_id/thread_id/fencing_token`이 모두 양수
- `owner_alive=true`, `heartbeat_at` 존재
- `stale_expired_count=0`
- `expired_owner_alive_count=0`

Dashboard fast path:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=runtime_execution_locks,pipeline_summary" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

## Ops Report

```powershell
python -m tools.ops_runtime_execution_lock_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --out-dir reports/runtime_execution_lock
```

## 장애 판정

- `STALE_EXPIRED`: owner가 죽고 lease도 만료됨. 다음 Core startup cleanup 대상이다.
- `EXPIRED_OWNER_ALIVE`: heartbeat 지연이지만 owner가 살아 있어 takeover가 차단된 상태다. 신규 run을 시작하지 말고 DB lock/CPU stall을 조사한다.
- `EVALUATION_RUN_FENCE_LOST`: 현재 owner/token이 바뀌어 stale writer가 transaction 전에 차단됐다.
- 반복되는 heartbeat lock contention은 장기 write transaction과 `busy_timeout`을 확인한다.

## Rollback

이 PR은 새 execution mode를 열지 않는다. 이상 시 Core/Gateway를 OBSERVE-safe로 중지하고 active owner PID를 확인한다. 살아 있는 owner를 강제 삭제하지 말고, process 종료와 lease 만료를 확인한 뒤 Core startup cleanup으로 정리한다.
