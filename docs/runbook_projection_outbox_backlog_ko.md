# Projection Outbox Backlog Runbook

## 목적

PR-10.7은 `condition_event` cutover 전 `projection_outbox` backlog를 별도 readiness gate로 본다. 이번 단계는 운영 관측과 drain tooling이며, 주문/LIVE_REAL/safety gate/매수 조건/worker apply 정책을 변경하지 않는다.

## 상태 해석

- `PASS`: error/dead_letter/stale PROCESSING이 없고, recent pending과 condition_event pending이 기준 이하이며 latest reconcile이 PASS다.
- `WARN`: 오래된 pending backlog가 크지만 최근 이벤트 압박은 낮다. drain을 권장하지만 core 검증 실패와 구분한다.
- `FAIL`: error/dead_letter, stale PROCESSING, recent pending 폭증, condition_event pending threshold 초과, latest reconcile 미통과, condition_event effective skip 발생 중 하나다.

핵심 필드:

- `pending_count`: 전체 PENDING backlog
- `recent_pending_count`: 최근 `PROJECTION_OUTBOX_BACKLOG_RECENT_WINDOW_SEC` 안에 생긴 PENDING
- `condition_event_pending_count`: PR-11 진입 전 반드시 낮춰야 하는 condition_event PENDING
- `stale_processing_count`: worker가 잡고 오래 끝내지 못한 PROCESSING
- `pr11_condition_event_cutover_ready`: PR-11 검토 가능 여부
- `operator_actions`: 권장 운영 조치

## 조회

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/projection-outbox/backlog" `
  -Headers @{ "X-Local-Token" = $env:TRADING_CORE_TOKEN }
```

Dashboard fast snapshot에서도 확인할 수 있다.

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/dashboard/snapshot?sections=projection_outbox,projection_outbox_backlog,pipeline_summary&fast=true" `
  -Headers @{ "X-Local-Token" = $env:TRADING_CORE_TOKEN }
```

## Drain Once

장중에는 작은 live_safe batch로 실행한다.

```powershell
Invoke-RestMethod `
  -Method POST `
  -Uri "http://127.0.0.1:8000/api/operator/projection-outbox/drain-once?limit=100&apply_projection=true&live_safe=true&max_batches=1&stop_on_locked=true" `
  -Headers @{ "X-Local-Token" = $env:TRADING_CORE_TOKEN }
```

주의:

- `apply_projection=true`를 요청해도 기존 설정 `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED`와 `PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED`가 꺼져 있으면 shadow verify만 수행한다.
- `LOCKED_RETRYABLE`은 HTTP 500이 아니며, ingest 압박이 낮아진 뒤 작은 batch로 재시도한다.
- 이 API는 주문 관련 side effect를 만들지 않는다.

## Ops Script

상태만 확인:

```powershell
python -m tools.ops_projection_outbox_backlog_drain `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --out-dir reports/projection_outbox_backlog
```

drain 포함:

```powershell
python -m tools.ops_projection_outbox_backlog_drain `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --drain `
  --limit 100 `
  --max-batches 20 `
  --sleep-sec 0.5 `
  --stop-on-locked `
  --out-dir reports/projection_outbox_backlog
```

보고서는 `reports/projection_outbox_backlog/<timestamp>/raw.json`와 `summary.md`에 저장된다.

## PR-11 진입 조건

- `readiness_status == PASS`
- `pr11_condition_event_cutover_ready == true`
- `condition_event_pending_count <= PROJECTION_OUTBOX_BACKLOG_CONDITION_EVENT_READY_MAX_PENDING`
- `condition_event_recent_pending_count <= PROJECTION_OUTBOX_BACKLOG_CONDITION_EVENT_READY_RECENT_MAX_PENDING`
- latest market_data projection reconcile PASS
- `condition_event_effective_skip_count == 0`
- projection_outbox error/dead_letter 0

## 금지 사항

- condition_event inline skip/cutover 활성화 금지
- price_tick/tr_response cutover 조건 완화 금지
- projection worker apply 조건 완화 금지
- LIVE_REAL 활성화 금지
- 주문 정책, safety gate, 매수 조건 변경 금지
