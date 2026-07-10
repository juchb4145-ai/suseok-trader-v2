# LIVE_SIM Lifecycle Durable Consumer Runbook

## 범위

schema 56은 Gateway에서 수신한 LIVE_SIM lifecycle event를
`live_sim_lifecycle_inbox`에 event rowid 순서로 보존한다. 기존 lifecycle 상태 전이는
`gateway_inline_compatibility` 경로에서 계속 실행되지만, handler write와 inbox 종결,
`live_sim_lifecycle` success watermark는 하나의 SQLite transaction으로 커밋된다.

이 단계는 준비 PR이다.

- `LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED=false`
- `LIVE_SIM_LIFECYCLE_WORKER_ENABLED=false`
- Gateway inline compatibility 유지
- LIVE_REAL 허용 없음
- broker 주문, 정정, 취소 command 생성 없음
- request path 제거와 비동기 cutover는 후속 guarded PR 범위

## Event 계약

다음 accepted Gateway event만 inbox 대상이다.

- `command_started`, `command_ack`, `command_failed`
- `execution_event`
- `order_rejected`, `cancel_ack`, `cancel_rejected`
- `balance_snapshot`, `account_snapshot`
- `kiwoom_balance_chejan`, `kiwoom_order_chejan`

`order_pre_ack`과 `order_broker_unconfirmed`는 P0-4 broker boundary 소유이므로 lifecycle
consumer에서 처리하지 않는다. market data와 heartbeat도 대상이 아니다.

## 원자성과 순서

- `event_id`는 inbox primary key다.
- `event_rowid`는 UNIQUE이며 worker 처리 순서다.
- handler 내부 `commit()`은 consumer transaction 안에서 지연된다.
- handler 결과, lifecycle table write, inbox `APPLIED`, projection event result와 success
  watermark가 같은 commit에 포함된다.
- crash 또는 예외가 발생하면 handler write 전체를 rollback하고 inbox attempt만 별도
  transaction으로 기록한다.
- 가장 오래된 unresolved event가 `PROCESSING` 또는 `DEAD_LETTER`이면 뒤 event를
  처리하지 않는다.

## 상태와 재시도

`PENDING -> PROCESSING -> APPLIED`가 정상 경로다. 실패하면 attempt를 증가시키고
`PENDING`으로 되돌린다. retry limit에 도달하면 `DEAD_LETTER`가 되며 이후 sequence를
fail-closed로 차단한다.

stale `PROCESSING`은 `LIVE_SIM_LIFECYCLE_PROCESSING_TTL_SEC` 뒤 reclaim한다. reclaim도
attempt로 계산하며 retry limit을 넘으면 dead-letter 처리한다.

## OBSERVE-safe 확인

Core는 별도 safe env에서 다음 조건으로 실행한다.

```text
TRADING_MODE=OBSERVE
TRADING_ALLOW_LIVE_SIM=false
TRADING_ALLOW_LIVE_REAL=false
LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED=false
LIVE_SIM_LIFECYCLE_WORKER_ENABLED=false
```

상태 확인:

```powershell
python -m tools.ops_live_sim_lifecycle_consumer_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN
```

준비 단계에서는 consumer/worker disabled warning이 예상된다. 다음 항목은 모두 0이어야
한다.

- `dead_letter_count`
- `stale_processing_count`
- `missing_inbox_count`
- `applied_without_result_count`
- command count delta
- order command count delta

## API

- `GET /api/operator/live-sim/lifecycle-consumer/status`
- `GET /api/operator/live-sim/lifecycle-consumer/inbox?limit=100`
- `POST /api/operator/live-sim/lifecycle-consumer/run-once?limit=20`
- `POST /api/operator/live-sim/lifecycle-consumer/reset-dead-letter?event_id=...`
- Dashboard fast section: `live_sim_lifecycle_consumer`

`run-once`는 consumer와 worker flag가 모두 true일 때만 처리한다. LIVE_SIM/LIVE_REAL을
활성화하지 않으며, inbox에 이미 수신된 broker observation만 반영한다.

## SQL 점검

```sql
SELECT status, COUNT(*)
FROM live_sim_lifecycle_inbox
GROUP BY status;

SELECT event_id, event_rowid, event_type, status, attempts, last_error
FROM live_sim_lifecycle_inbox
WHERE status != 'APPLIED'
ORDER BY event_rowid;

SELECT projection_name,
       last_success_event_rowid,
       last_success_event_id,
       last_error_event_rowid,
       last_error_event_id,
       last_error_message
FROM projection_watermarks
WHERE projection_name = 'live_sim_lifecycle';

SELECT inbox.event_id
FROM live_sim_lifecycle_inbox AS inbox
LEFT JOIN projection_event_results AS result
  ON result.projection_name = 'live_sim_lifecycle'
 AND result.event_id = inbox.event_id
WHERE inbox.status = 'APPLIED'
  AND result.event_id IS NULL;
```

## Dead-letter 복구

1. inbox row의 source event, command id, broker boundary와 lifecycle state를 대조한다.
2. 원인을 수정하고 LIVE_SIM reconcile이 신규 BUY를 안전하게 차단하는지 확인한다.
3. 단일 event만 reset endpoint로 `PENDING` 전환한다.
4. 작은 `limit=1` run-once로 재처리한다.
5. watermark와 뒤 event 순서가 유지되는지 확인한다.

근거 없이 row를 `APPLIED`로 직접 수정하거나 dead-letter를 일괄 삭제하지 않는다.

## Rollback

준비 단계 rollback은 consumer/worker flag를 false로 유지하는 것이다. Gateway inline
compatibility는 계속 lifecycle을 적용한다. inbox와 watermark는 감사 evidence이므로
삭제하지 않는다. 후속 cutover에서 worker 오류, dead-letter, stale processing 또는 inbox
gap이 발생하면 즉시 inline compatibility로 복귀해야 한다.
