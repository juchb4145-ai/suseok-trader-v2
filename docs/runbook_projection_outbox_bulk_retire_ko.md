# Projection Outbox Bulk Retire Runbook

## 목적

PR-10.8은 `condition_event` cutover 전 누적된 old shadow backlog를 빠르게 정리하기 위한 운영 절차다. Gateway inline projection으로 이미 처리된 근거가 있는 `projection_outbox` job만 `APPLIED` 또는 `SKIPPED`로 terminal 처리한다.

이 절차는 append-only 전환이 아니며, projection table과 주문/LIVE_SIM/LIVE_REAL 동작을 변경하지 않는다.

## 핵심 개념

- `total_pending_count`: 전체 PENDING outbox job 수
- `bulk_retire_eligible_count`: inline artifact/error/skip-safe evidence로 bulk terminal 처리 가능한 old shadow job 수
- `non_blocking_shadow_pending_count`: PR-11 cutover를 직접 막지 않는 old shadow backlog
- `blocking_pending_count`: bulk retire로 안전하게 정리할 수 없는 PENDING job 수
- `condition_event_blocking_pending_count`: PR-11 `condition_event` cutover 전 반드시 낮춰야 하는 blocking backlog
- `effective_skip_pending_count`: `effective_skip_inline=True`로 worker apply가 책임져야 하며 bulk retire 대상에서 제외되는 pending 수

raw pending이 커도 `blocking_pending_count`가 낮고 latest reconcile이 PASS면 PR-11 readiness는 WARN 또는 PASS가 될 수 있다. 반대로 error/dead_letter, effective skip pending, recent pending, condition_event blocking pending은 계속 차단 조건이다.

## Dry-run

장중에는 먼저 dry-run으로 eligible 규모를 확인한다.

```powershell
python -m tools.ops_projection_outbox_bulk_retire `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --dry-run `
  --limit 5000 `
  --older-than-sec 60 `
  --out-dir reports/projection_outbox_bulk_retire
```

확인 항목:

- `retired_count > 0`
- `error_count == 0`
- `dead_letter_count == 0`
- `blocking_pending_count`가 fail threshold 미만
- `effective_skip_pending_count == 0`
- `condition_event_blocking_pending_count`가 PR-11 기준 이하

## Apply

장중 apply는 DB write lock을 피하기 위해 작은 batch로 시작한다.

```powershell
python -m tools.ops_projection_outbox_bulk_retire `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --apply `
  --limit 1000 `
  --older-than-sec 60 `
  --out-dir reports/projection_outbox_bulk_retire
```

장후 또는 ingest 압박이 낮을 때는 `--limit 5000`으로 반복 실행할 수 있다.

API를 직접 호출할 때:

```powershell
Invoke-RestMethod `
  -Method POST `
  -Uri "http://127.0.0.1:8000/api/operator/projection-outbox/bulk-retire?limit=5000&dry_run=false&older_than_sec=60&live_safe=true" `
  -Headers @{ "X-Local-Token" = $env:TRADING_CORE_TOKEN }
```

## 판정

PASS:

- dry-run에서 eligible job이 확인됨
- apply 후 pending이 감소함
- `error_count == 0`, `dead_letter_count == 0`
- `blocking_pending_count`와 `condition_event_blocking_pending_count`가 기준 이하
- `pr11_condition_event_cutover_ready == true`

WARN:

- total pending은 크지만 non-blocking shadow backlog가 대부분임
- live ingest 중 `LOCKED_RETRYABLE`이 발생함
- bulk retire eligible이 남아 추가 반복 실행이 필요함

FAIL:

- error/dead_letter가 존재함
- blocking pending이 fail threshold 이상임
- condition_event blocking pending이 기준 초과임
- apply 후 pending이 줄지 않음
- effective skip pending이 stuck 상태임

## Safety

bulk retire는 다음만 갱신한다.

- `projection_outbox.status`
- `projection_outbox.updated_at`
- `projection_outbox.processed_at`
- `projection_outbox.metadata_json`
- `projection_outbox.locked_by`, `locked_at`, `last_error` 정리

bulk retire는 다음을 절대 하지 않는다.

- projection table insert/update/delete
- Gateway ingest path 변경
- `condition_event` inline skip/cutover 활성화
- price_tick/tr_response cutover 조건 완화
- worker apply 조건 완화
- LIVE_REAL 활성화
- 주문 정책, safety gate, buy gate 변경

## Rollback

projection table을 건드리지 않으므로 기능 rollback은 코드 revert로 충분하다. 이미 `APPLIED`/`SKIPPED` 처리한 outbox row를 운영상 되돌려야 한다면, 같은 `bulk_retire_run_id` metadata를 기준으로 별도 SQL 감사 후 수동 복구한다. 단, error/dead_letter를 숨긴 것이 아니어야 하며, 복구 전 raw `gateway_events`와 inline artifact가 보존되어 있는지 확인한다.

## PR-11 진입 조건

- latest market_data projection reconcile PASS
- `error_count == 0`
- `dead_letter_count == 0`
- `stale_processing_count == 0`
- `effective_skip_pending_count == 0`
- `condition_event_blocking_pending_count <= PROJECTION_OUTBOX_BACKLOG_CONDITION_EVENT_READY_MAX_PENDING`
- `condition_event_recent_pending_count <= PROJECTION_OUTBOX_BACKLOG_CONDITION_EVENT_READY_RECENT_MAX_PENDING`
- `pr11_condition_event_cutover_ready == true`
