# Market Reference Projection Runbook

## 목적

PR-14는 `market_reference`에 한해 `market_symbols` inline projection을 분당 최대 1건만 worker로 넘기는 limited cutover다. 모든 gate가 충족되지 않으면 같은 Gateway 요청에서 즉시 `process_market_symbols_event()` inline 경로로 되돌아간다.

주문, LIVE_SIM, LIVE_REAL, market_data controller와 market_index/regime/scan 동작은 변경하지 않는다.

## 역할

- 입력 event: `gateway_events.event_type='market_symbols'`
- projection artifact: `market_symbol_memberships`
- outbox job: `projection_outbox.projection_name='market_reference'`
- reconcile table:
  - `market_reference_projection_reconcile_runs`
  - `market_reference_projection_reconcile_issues`
- routing table:
  - `market_reference_projection_routing_decisions`
- global budget state:
  - `market_reference_append_only_budget_state`

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
- `GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH=true`
- `GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE=0`
- `GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA=1`
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

## Limited Cutover Gate

effective skip은 다음 조건을 모두 만족할 때만 허용된다.

- dry-run flag와 cutover flag ON
- global kill switch OFF
- legacy PR-13 emergency guard OFF
- global/reference worker apply ON
- latest reconcile `PASS`, `append_only_ready=true`, age 300초 이내
- 기존 membership count가 configured minimum 이상
- 현재 event가 accepted `market_symbols`이고 symbols payload가 비어 있지 않음
- 현재 event의 market_reference outbox job이 ready 상태
- market_reference outbox `ERROR/DEAD_LETTER=0`
- processing job 0, pending job은 현재 event를 포함해 configured SLA 이하
- 이전 effective skip의 outbox가 `APPLIED`이고 event별 membership artifact가 존재
- 원자적 global budget reservation 성공

budget state는 venue key를 사용하지 않는다. KRX/NXT 전환이나 Gateway 재시작으로 같은 분의 budget이 초기화되지 않는다.

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
./tools/start_market_open_observe.ps1 `
  -RunCore `
  -RunGateway `
  -RealtimeExchange nxt `
  -MarketReferenceProjectionValidation

Invoke-RestMethod http://127.0.0.1:8000/health
```

`-MarketReferenceProjectionValidation`은 background worker와 market_data apply를
끄고, market_reference 수동 run-once apply와 dry-run routing만 활성화한다.
operator batch와 SQLite lock을 경쟁하지 않도록 이 검증 모드에서만 periodic
condition-fusion sweep, incremental worker, retention worker도 중지한다.
KRX 정규장에서는 `-RealtimeExchange krx`, NXT 세션에서는 `nxt`를 사용한다.

2. reconcile run-once:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/market-reference-projection-reconcile/run-once?limit=100&persist=true&live_safe=true" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

3. market_reference 전용 worker run-once:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/projection-outbox/run-once?projection_name=market_reference&limit=1&apply_projection=true&live_safe=true" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

`projection_name=market_reference`는 오래된 market_data backlog를 claim하거나
terminal 처리하지 않고 reference job만 선택한다.

4. reconcile을 다시 실행한 뒤 latest reconcile 확인:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/market-reference-projection-reconcile/latest" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

5. routing status 확인:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/market-reference-append-only-routing/status" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

6. dashboard fast path 확인:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=gateway,market_data,market_reference,projection_outbox,pipeline_summary,errors" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

7. ops script 실행:

```powershell
python -m tools.ops_market_reference_projection_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --out-dir reports/market_reference_projection
```

## PR-14 Limited Cutover 확인

PR-13 preflight가 PASS인 운영 DB에서 Core/Gateway를 OBSERVE-safe로 다시 띄운다.

```powershell
./tools/start_market_open_observe.ps1 `
  -RunCore `
  -RunGateway `
  -RealtimeExchange nxt `
  -MarketReferenceLimitedCutover
```

`-MarketReferenceLimitedCutover`만 process-local로 cutover ON, kill switch OFF, budget `1/min`, worker apply ON을 설정한다. 실제 `market_symbols` event가 없으면 NXT tick만으로 reference PASS를 추정하지 않는다.

새 `market_symbols` routing decision에서 `effective_skip_inline=true`를 확인한 직후 reference-only worker와 reconcile을 실행한다.

```powershell
python -m tools.ops_market_reference_projection_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-worker `
  --expect-effective-skip `
  --out-dir reports/market_reference_projection
```

## 확인할 숫자

- `latest_run.status = PASS`
- `latest_run.append_only_ready = true`
- `stored_membership_count >= GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MIN_MEMBERSHIP_COUNT`
- `missing_membership_count = 0`
- `outbox_error_count = 0`
- `outbox_dead_letter_count = 0`
- preflight: `effective_skip_inline_count = 0`
- limited cutover: `status = PASS`
- limited cutover: `effective_skip_inline_count > 0`
- limited cutover: `skip_budget_limit = 1`
- limited cutover: `skip_budget_used_current_minute = 1`
- limited cutover 후 worker: `effective_skip_health.pending_worker_count = 0`
- limited cutover 후 worker: `effective_skip_health.artifact_missing_count = 0`
- Gateway response: `projection_statuses.market_reference_effective_skip_inline = "TRUE"`
- worker evidence: `apply_mode=MARKET_REFERENCE_APPLY`, `apply_result=APPLIED_BY_WORKER`

## 다음 PR 진입 조건

- 장중 `market_reference` reconcile PASS가 안정적으로 유지됨
- dashboard fast path `market_reference.append_only_ready=true`
- outbox ERROR/DEAD_LETTER 0
- worker apply run-once evidence 정상
- PR-14 controller PASS
- effective skip count > 0와 budget `1/min` evidence
- effective-skip event가 worker `APPLIED_BY_WORKER` 후 membership artifact를 가짐
- rollback required false
- market_data append-only controller와 기존 price_tick/tr_response/condition_event 동작에 회귀 없음

## Rollback

어느 gate든 실패하면 요청 단위 inline fallback이 자동 적용된다. 즉시 전체 cutover를 닫으려면 Core를 아래 safe default로 재시작한다.

```powershell
GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED=false
GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED=false
GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH=true
GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE=0
GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13=true
PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED=false
```

이 변경은 주문, LIVE_SIM, LIVE_REAL, safety gate, buy gate를 건드리지 않는다.
