# Projection watermark / retention RCA runbook

대상: projection별 success/error watermark, event별 retention eligibility, replay 보존
상태를 확인하는 운영 절차.

## 안전 원칙

- 기본값 `EVENT_STORE_RETENTION_ENABLED=false`를 유지한다.
- 기본값 `PROJECTION_EVENT_RESULT_BACKFILL_ENABLED=false`를 유지한다.
- status, RCA, backfill dry-run은 event 또는 주문을 삭제/생성하지 않는다.
- 실제 retention apply는 retention enable과 projection gate가 모두 PASS일 때만 가능하다.
- 오래된 projection 대상 event가 하나라도 blocked면 전체 apply를 거부한다.
- 운영 검증에서는 `dry_run=false` prune과 backfill apply를 실행하지 않는다.
- 주문/정정/취소/SOR, LIVE_SIM/LIVE_REAL 설정은 변경하지 않는다.

## Schema 48 계약

`projection_watermarks`는 기존 `last_event_*` 호환 컬럼과 별도로 다음 위치를 저장한다.

```text
last_success_event_rowid / id / received_at / processed_at
last_error_event_rowid / id / received_at / processed_at / error_message
```

`projection_event_results`는 `(projection_name,event_id)`별 최신 `SUCCESS|ERROR`, outcome,
attempt count를 저장한다. `projection_retention_event_rca` SQL view는 Gateway event,
outbox, result를 같은 row에서 조회한다.

legacy schema 47 migration은 기존 `last_event_rowid`를 success로 추정하지 않는다. 새
success/error 컬럼과 result ledger는 0/빈 상태에서 시작하므로 retention은 evidence 없는
과거 event를 자동 차단한다.

## Watermark 확인

```powershell
Invoke-RestMethod `
  http://127.0.0.1:8000/api/operator/projection-watermarks/status

Invoke-RestMethod `
  "http://127.0.0.1:8000/api/operator/projection-watermarks/results?status=ERROR&limit=20"
```

확인값:

```text
watermarks[].last_success_event_rowid
watermarks[].last_error_event_rowid
by_projection.*.success_count/error_count
unresolved_error_count
```

market-data inline 오류는 error watermark와 result `ERROR`를 기록하고 success/legacy
watermark를 전진시키지 않는다. outbox `APPLIED`와 result `SUCCESS`, outbox
`ERROR|DEAD_LETTER`와 result `ERROR`는 같은 commit에 기록된다.

## Event별 RCA

최근 old event blocker:

```powershell
Invoke-RestMethod `
  "http://127.0.0.1:8000/api/operator/projection-retention/rca?blocked_only=true&limit=50"
```

특정 event:

```powershell
Invoke-RestMethod `
  "http://127.0.0.1:8000/api/operator/projection-retention/rca?event_id=<EVENT_ID>"
```

projection 대상 accepted event는 아래가 모두 참이어야 eligible이다.

```text
raw event 존재
command_id 없음
projection_outbox job 1개 이상
모든 required job status=APPLIED
모든 projection_event_results status=SUCCESS
각 projection last_success_event_rowid >= event rowid
```

주요 blocker reason:

```text
PROJECTION_OUTBOX_MISSING
PROJECTION_OUTBOX_NOT_APPLIED
PROJECTION_RESULT_MISSING
PROJECTION_RESULT_ERROR
PROJECTION_SUCCESS_WATERMARK_BEHIND
RAW_EVENT_MISSING
COMMAND_LINKED_EVENT_PROTECTED
```

## Legacy result backfill

dry-run만 먼저 실행한다.

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/projection-watermarks/backfill?dry_run=true&limit=100" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

backfill은 기존 `projection_outbox.status=APPLIED`만 success 근거로 인정한다. `SKIPPED`,
`ERROR`, `DEAD_LETTER`, artifact만 존재하는 row는 자동 승격하지 않는다. apply는 RCA와
reconcile을 확인한 뒤 `PROJECTION_EVENT_RESULT_BACKFILL_ENABLED=true`인 별도 프로세스에서
짧은 batch로 별도 승인해 실행한다.

## Retention dry-run

```powershell
Invoke-RestMethod `
  http://127.0.0.1:8000/api/operator/event-retention/status

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/event-retention/prune?dry_run=true&limit=100" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

보존식:

```text
age_eligible_event_count
  = candidate_event_count + projection_blocked_event_count
```

`apply_ready=true`는 retention enabled와 blocked count 0을 모두 뜻한다. 기본 설정에서는
`EVENT_RETENTION_DISABLED` 때문에 `apply_ready=false`가 정상이다.

실제 apply 요청은 다음 중 하나라도 해당하면 HTTP 409다.

```text
EVENT_RETENTION_DISABLED
PROJECTION_RETENTION_GATE_BLOCKED
```

## KRX/NXT 확인

- RCA `venue_counts`와 event `venue`를 KRX/NXT/UNKNOWN/MIXED로 분리한다.
- payload/metadata/row에 `exchange=NXT`가 명시된 event만 NXT로 인정한다.
- condition event는 KRX evidence이며 NXT condition evidence로 승격하지 않는다.
- venue가 달라도 global retention gate와 event age는 초기화되지 않는다.

## 통합 ops report

OBSERVE-safe Core에서 실행한다.

```powershell
python -m tools.ops_projection_retention_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN
```

도구는 GET과 backfill dry-run만 수행한다. 전후 command/order-command delta가 0인지,
Dashboard fast section과 API count가 일치하는지 확인하고
`reports/projection_retention/<timestamp>/raw.json`, `summary.md`를 남긴다.

## Rollback

- retention 즉시 정지: `EVENT_STORE_RETENTION_ENABLED=false` 프로세스로 재시작한다.
- result/backfill 이상: backfill apply를 중지하고 RCA/reconcile로 원인을 확인한다.
- error result를 SQL로 임의 SUCCESS 변경하거나 watermark를 수동 전진시키지 않는다.
- 이미 삭제된 raw event는 projection DB만으로 복구할 수 없다. apply 전 replay export와
  hash 검증을 완료한다.

이 PR은 retention apply 또는 append-only cutover를 활성화하지 않는다.
