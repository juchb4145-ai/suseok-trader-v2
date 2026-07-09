# Market Reference Projection Runbook

## 목적

PR-13은 `market_reference` cutover가 아니다. `market_symbols` inline projection은 Gateway request path에서 계속 실행되며, `market_reference effective_skip_inline`은 항상 `False`다.

이번 단계의 목적은 `market_symbols` event를 `projection_outbox` worker가 안전하게 처리할 준비가 되었는지 확인하고, inline artifact와 worker/reconcile evidence를 비교하는 것이다.

## 역할

- 입력 event: `gateway_events.event_type='market_symbols'`
- projection artifact: `market_symbol_memberships`
- outbox job: `projection_outbox.projection_name='market_reference'`
- reconcile table:
  - `market_reference_projection_reconcile_runs`
  - `market_reference_projection_reconcile_issues`
- routing table:
  - `market_reference_projection_routing_decisions`

## Payload 구조

지원되는 대표 형태:

```json
{"markets": {"KOSPI": [{"code": "005930", "name": "삼성전자"}]}}
```

```json
{"markets": [{"market": "KOSDAQ", "symbols": [{"code": "035420", "name": "NAVER"}]}]}
```

`markets`가 dict/list가 아닌 경우 reconcile은 `payload_shape_counts.other`로 집계한다. symbols가 비어 있으면 projection worker는 `SKIPPED/MARKET_REFERENCE_NO_SYMBOLS`로 처리한다.

## Safe 기본값

- `PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED=false`
- `GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED=false`
- `GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED=false`
- `GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13=true`
- `PROJECTION_OUTBOX_WORKER_ENABLED=false`
- `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false`

이 기본값에서는 production behavior가 바뀌지 않는다.

## Worker Apply 조건

worker가 `process_market_symbols_event()`를 호출하는 조건:

- `apply_projection=true` run-once 호출
- `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=true`
- `PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED=true`
- source `gateway_events.status='ACCEPTED'`
- `projection_name='market_reference'`
- `event_type='market_symbols'`
- inline artifact가 아직 없고 payload에 symbols가 있음

이미 `market_symbol_memberships.event_id` artifact가 있으면 worker는 재호출하지 않고 `APPLIED/INLINE_ARTIFACT_OBSERVED`로 종료한다.

## Reconcile PASS 기준

PASS 조건:

- accepted `market_symbols` event가 존재
- `market_symbol_memberships` count가 configured minimum 이상
- payload symbol이 membership에 누락 없이 저장됨
- latest `market_symbols` event가 membership event_id로 반영됨
- `projection_outbox` market_reference job이 존재
- outbox `ERROR/DEAD_LETTER=0`
- terminal outbox 이후 artifact missing 없음
- `append_only_ready=true`

WARN 예:

- symbols empty
- outbox PENDING/PROCESSING within SLA
- membership count가 minimum보다 낮지만 0은 아님
- 오래된 event membership이 newer event로 superseded됨

FAIL 예:

- membership count 0
- payload parse error
- membership missing/mismatch
- latest event membership event_id mismatch
- outbox job missing
- outbox ERROR/DEAD_LETTER
- terminal outbox 이후 artifact missing

## 장중 확인 절차

1. OBSERVE-safe 환경 확인:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

2. reconcile run-once:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/market-reference-projection-reconcile/run-once?limit=100&persist=true&live_safe=true" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

3. latest reconcile 확인:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/market-reference-projection-reconcile/latest" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

4. routing status 확인:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/market-reference-append-only-routing/status" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

5. dashboard fast path 확인:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=gateway,market_data,market_reference,projection_outbox,pipeline_summary,errors" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

6. ops script 실행:

```powershell
python -m tools.ops_market_reference_projection_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --out-dir reports/market_reference_projection
```

## 확인할 숫자

- `latest_run.status = PASS`
- `latest_run.append_only_ready = true`
- `stored_membership_count >= GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT`
- `missing_membership_count = 0`
- `outbox_error_count = 0`
- `outbox_dead_letter_count = 0`
- `market_reference.append_only_routing.effective_skip_inline_count = 0`
- Gateway response `projection_statuses.market_reference_effective_skip_inline = "FALSE"`

## PR-14 진입 조건

- 장중 `market_reference` reconcile PASS가 안정적으로 유지됨
- dashboard fast path `market_reference.append_only_ready=true`
- outbox ERROR/DEAD_LETTER 0
- worker apply run-once evidence 정상
- effective skip count 0
- market_data append-only controller와 기존 price_tick/tr_response/condition_event 동작에 회귀 없음

PR-14에서만 작은 budget과 rollback 절차 아래 `market_reference` limited cutover를 검토한다.

## Rollback

PR-13에서는 inline skip이 없으므로 rollback은 flag를 safe default로 되돌리는 것이다.

```powershell
GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED=false
GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED=false
PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED=false
```

이 변경은 주문, LIVE_SIM, LIVE_REAL, safety gate, buy gate를 건드리지 않는다.
