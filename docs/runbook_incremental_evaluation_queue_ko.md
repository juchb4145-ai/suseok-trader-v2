# Incremental Evaluation Queue Operations Runbook

## 범위

schema 58은 incremental evaluation retry limit에 도달한 candidate를 active queue에서
`incremental_evaluation_dead_letters`로 원자 이동한다. active queue에는 worker가 다시
처리할 수 있는 row만 남고, dead-letter는 operator가 원인을 확인해 명시적으로 reset할
때까지 보존된다.

이 경로는 Candidate -> Strategy -> Risk 관측 갱신만 수행한다.

- order intent 또는 broker command를 만들지 않는다.
- LIVE_SIM/LIVE_REAL을 허용하지 않는다.
- 기존 evaluation runtime lock과 fencing token을 사용한다.
- dead-letter나 retry-exhausted row를 자동 삭제하지 않는다.

## 상태 기준

`GET /api/operator/incremental-evaluation/status`는 다음을 반환한다.

- `queued_count`, `oldest_enqueued_at`, `oldest_age_sec`
- `oldest_updated_at`, `oldest_updated_age_sec`
- `stale_queue_count`, `stale_fail_count`
- `retry_exhausted_count`
- `dead_letter_count`
- `status`, `backlog_status`, `reason_codes`

기본 threshold:

```text
INCREMENTAL_EVALUATION_BACKLOG_WARN_COUNT=100
INCREMENTAL_EVALUATION_BACKLOG_FAIL_COUNT=1000
INCREMENTAL_EVALUATION_STALE_WARN_SEC=30
INCREMENTAL_EVALUATION_STALE_FAIL_SEC=300
```

active retry-exhausted 또는 unresolved dead-letter가 하나라도 있으면 `FAIL`이다. backlog와
stale age는 warn/fail threshold를 각각 적용한다.

## Dead-letter 이동

정상 worker failure는 attempt를 증가시킨다. 다음 attempt가 retry limit에 도달하면 같은
transaction에서 dead-letter insert와 active queue delete를 수행한다.

schema 58 이전의 retry-exhausted active row는 다음 경로로 이동한다.

- incremental worker batch 시작 시 guarded sweep
- operator sweep endpoint
- 같은 candidate의 새 event enqueue 직전

새 event는 이전 failure evidence를 지우거나 reset을 우회하지 않는다. exhausted row를
dead-letter로 보존한 뒤 `BLOCKED_DEAD_LETTER`를 반환하며, operator reset만 attempts 0의
fresh queue row를 복원한다.

## API

- `GET /api/operator/incremental-evaluation/status`
- `GET /api/operator/incremental-evaluation/dead-letters?limit=100`
- `POST /api/operator/incremental-evaluation/dead-letters/sweep?limit=100`
- `POST /api/operator/incremental-evaluation/dead-letters/reset?dead_letter_id=...`
- Dashboard fast section: `incremental_evaluation`

sweep는 evaluation runtime lock과 fencing token을 획득한다. lock이 이미 사용 중이면 HTTP
409로 fail-closed한다.

## OBSERVE-safe 점검

```powershell
python -m tools.ops_incremental_evaluation_queue_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN
```

기본 실행은 read-only다. 명시적 이동이 필요할 때만 다음 옵션을 사용한다.

```powershell
python -m tools.ops_incremental_evaluation_queue_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --sweep-retry-exhausted

python -m tools.ops_incremental_evaluation_queue_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --reset-dead-letter-id incremental_dead_letter_...
```

ops report는 Core OBSERVE, LIVE_SIM/LIVE_REAL false, queue/Dashboard 일치와 command 및
order-command delta 0을 함께 확인한다.

## SQL 점검

```sql
SELECT COUNT(*) AS queued_count,
       MIN(enqueued_at) AS oldest_enqueued_at,
       MIN(updated_at) AS oldest_updated_at,
       MAX(attempts) AS max_attempts
FROM incremental_evaluation_queue;

SELECT candidate_instance_id, code, attempts, last_error, updated_at
FROM incremental_evaluation_queue
WHERE attempts >= 3
ORDER BY updated_at;

SELECT dead_letter_id,
       candidate_instance_id,
       code,
       attempts,
       last_error,
       status,
       dead_lettered_at,
       reset_at
FROM incremental_evaluation_dead_letters
ORDER BY dead_lettered_at DESC;
```

retry limit은 운영 설정과 맞춰 SQL 조건을 조정한다.

## Reset 절차

1. candidate가 active인지 확인한다.
2. `last_error`, source event, candidate/tick/theme freshness를 확인한다.
3. 동일 candidate의 active queue row가 없는지 확인한다.
4. 단일 `dead_letter_id`를 reset한다.
5. 작은 run-once batch로 처리하고 Strategy/Risk 최신 row를 확인한다.
6. queue와 dead-letter status, command delta를 다시 확인한다.

candidate가 closed이거나 같은 candidate의 newer active queue가 있으면 reset은 차단된다.
기존 active row를 덮어쓰지 않는다.

## Rollback

worker 이상 시 `INCREMENTAL_EVALUATION_WORKER_ENABLED=false`로 재기동하고 read-only status와
dead-letter evidence를 보존한다. queue/dead-letter row를 직접 삭제하거나 attempts를 수동
감소시키지 않는다. 원인 수정 후 단일 reset과 작은 run-once로 복구한다.
