# Market Regime Projection PR-18 / PR-19 Runbook

## 목적과 범위

PR-18은 공통 `market_context`를 생성하는 `market_regime` projection outbox worker,
reconcile, Gateway dry-run routing을 준비한다. 이 단계는 append-only cutover가 아니다.
Gateway inline market-regime/context builder는 그대로 실행하고
`effective_skip_inline`은 코드와 schema evidence에서 항상 false다.

PR-19는 이 준비 계약 위에 KRX 정규장 `market_index_tick` 소량 event만 worker로 넘기는
limited cutover를 추가한다. 기본값에서는 계속 inline이며, 활성화된 경우에도
`market_index` projection은 먼저 완료되고 market-regime/context builder만 worker로 이관된다.

실제 주문, 정정, 취소, SOR, LIVE_SIM/LIVE_REAL 동작은 변경하지 않는다. 운영 `.env`와
운영 DB는 수정하지 않고 별도 OBSERVE-safe process 환경과 격리 DB에서 먼저 검증한다.

## 안전 기본값

```text
TRADING_PROFILE=OBSERVE
TRADING_MODE=OBSERVE
TRADING_ALLOW_LIVE_SIM=false
TRADING_ALLOW_LIVE_REAL=false

GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED=false
GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED=false
GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH=true
GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE=0
GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_PENDING_WITHIN_SLA=1
GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_RECONCILE_PASS=true
GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_PRIOR_EVENT_RECONCILE=true
GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_INDEX_ROUTING_GUARD=true
GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_WORKER_CONTEXT_REFRESH=true
GATEWAY_MARKET_REGIME_APPEND_ONLY_FAIL_CLOSED_ON_CONTEXT_REFRESH_ERROR=true
GATEWAY_MARKET_REGIME_APPEND_ONLY_RECONCILE_MAX_AGE_SEC=300
GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18=true

PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false
PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED=false
PROJECTION_OUTBOX_MARKET_REGIME_APPLY_BATCH_SIZE=20
PROJECTION_OUTBOX_MARKET_REGIME_APPLY_MIN_AGE_SEC=1.0
```

worker apply, dry-run, cutover는 기본 disabled이고 global kill switch와 PR-18 legacy guard는
enabled다. PR-19 limited cutover에는 dry-run/cutover true, kill switch/legacy guard false,
budget `1/min`이 동시에 필요하다.

## 데이터 계약

- schema 52는 `market_regime_projection_reconcile_runs/issues`와
  `market_regime_projection_routing_decisions`를 additive로 추가한다.
- schema 53은 regime routing cutover/controller evidence와 원자적
  `market_regime_append_only_budget_state`를 additive로 추가한다.
- worker는 accepted `market_index_tick`의 index sample이 먼저 존재할 때만 regime job을
  처리한다. dependency 누락은 retryable error이며 retry limit 도달 시 `DEAD_LETTER`다.
- standalone regime worker는 PR-17 공통 builder만 사용한다. 새 index watermark에서 global
  regime 1개와 KOSPI/KOSDAQ context pair를 만들고, 같은 watermark 재시도는 verify-only다.
- 이미 더 최신인 동일 index event가 있으면 오래된 source event는 새 snapshot을 만들지 않고
  latest common context를 검증한다.
- reconcile은 accepted event, index sample, regime outbox, latest KOSPI/KOSDAQ watermark,
  source-regime reference, parser/data readiness를 대조한다.
- dry-run routing은 fresh reconcile PASS, append-only readiness, accepted source, index artifact,
  regime outbox, worker apply, OBSERVE-safe runtime을 모두 요구한다.
- `would_skip_inline=true`여도 `effective_skip_inline=false`와
  `EFFECTIVE_SKIP_DISABLED_IN_PR18` evidence를 반드시 남긴다.
- PR-19 effective skip은 current index routing evidence가 parser VERIFIED, data usable,
  explicit REALTIME, KRX 평일 REGULAR, fresh Gateway heartbeat/parsed tick인 경우만 허용한다.
- latest reconcile은 current event 직전 accepted index event까지 덮어야 한다. 이전 effective
  skip의 regime outbox, worker apply evidence, event-linked global regime, KOSPI/KOSDAQ context
  pair가 닫히지 않으면 다음 event부터 inline fallback한다.
- effective-skip worker는 5초 cadence를 무시하고 current event-linked context pair를 강제로
  생성한다. worker 전에 동일 index의 더 최신 event가 들어오면 retry/dead-letter와 rollback으로
  fail-closed한다.

## API 점검 순서

1. Core status에서 OBSERVE와 LIVE_SIM/LIVE_REAL false를 확인한다.
2. `market_index` projection이 먼저 적용됐는지 확인한다.
3. 작은 `market_regime` worker batch를 실행한다.

```powershell
$headers = @{ "X-Local-Token" = $env:TRADING_CORE_TOKEN }
Invoke-RestMethod -Method Post -Headers $headers `
  "http://127.0.0.1:8000/api/operator/projection-outbox/run-once?projection_name=market_regime&limit=2&apply_projection=true&live_safe=true"
```

4. reconcile을 실행하고 latest 결과를 확인한다.

```powershell
Invoke-RestMethod -Method Post -Headers $headers `
  "http://127.0.0.1:8000/api/operator/market-regime-projection-reconcile/run-once?limit=100&persist=true&live_safe=true"
Invoke-RestMethod `
  "http://127.0.0.1:8000/api/operator/market-regime-projection-reconcile/latest"
```

5. routing과 Dashboard에서 effective skip 0을 확인한다.

```powershell
Invoke-RestMethod `
  "http://127.0.0.1:8000/api/operator/market-regime-append-only-routing/status"
Invoke-RestMethod `
  "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=market_context,market_regime_projection_reconcile,market_regime_append_only_routing,projection_outbox"
```

종합 evidence:

```powershell
.\venv_64\Scripts\python.exe tools\ops_market_regime_projection_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-worker `
  --worker-limit 2
```

PR-19 effective skip 1건과 worker closure를 확인할 때만 다음 옵션을 추가한다.

```powershell
.\venv_64\Scripts\python.exe tools\ops_market_regime_projection_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-worker `
  --worker-limit 1 `
  --expect-effective-skip
```

결과는 `reports/market_regime_projection/<UTC timestamp>/raw.json`과 `summary.md`에
기록된다.

## PASS/WARN/FAIL

PASS:

- Core profile/mode OBSERVE, LIVE_SIM/LIVE_REAL 허용 false
- KOSPI/KOSDAQ accepted index event와 sample coverage 존재
- common context pair의 watermark와 source regime reference가 coherent
- context parser VERIFIED, data usable, fresh
- reconcile PASS와 `append_only_ready=true`
- regime outbox `ERROR/DEAD_LETTER=0/0`
- routing `effective_skip_inline_count=0`
- 점검 전후 Gateway command/order-command delta `0/0`

PR-19 `--expect-effective-skip` PASS:

- cutover true, global kill switch false, PR-18 legacy guard false, budget `1/min`
- current event index routing의 KRX session/parser/data/source/Gateway health PASS
- effective skip 1건 이상, budget 범위 이내, controller PASS, rollback false
- worker `APPLIED_BY_WORKER`와 mutation `market_regime,market_context`
- event-linked global regime과 context pair, regime outbox APPLIED
- effective-skip pending/error/evidence/snapshot/context 누락 0
- reconcile PASS, outbox ERROR/DEAD_LETTER `0/0`, command delta `0/0`

WARN:

- event pair 누락, stale, parser unverified, data unusable
- reconcile WARN 또는 common context WARN
- 이 상태는 PR-19 cutover 진입 evidence로 사용할 수 없다.

FAIL:

- index dependency, outbox, context watermark 또는 source-regime lineage 누락
- regime outbox ERROR/DEAD_LETTER
- reconcile FAIL
- PR-18 준비 점검에서 effective skip 1건 이상 또는 legacy guard 비활성화
- PR-19 기대 모드에서 skip, budget, index-routing health, worker closure 중 하나라도 누락
- OBSERVE-safe 조건 위반, command/order-command 증가

## KRX/NXT 경계

NXT price tick과 NXT-only 시간은 venue-neutral market-data worker, replay, Dashboard 검증에
활용할 수 있다. 그러나 KOSPI/KOSDAQ market-regime의 source watermark는 KRX index event여야
한다. NXT event, synthetic index, 전일 KRX snapshot은 현재 장중 KRX regime PASS를 대체하지
않는다. 정규장 밖에는 `PENDING_KRX_SESSION`을 유지하고 synthetic fixture PASS를 실제
cutover 승인으로 사용하지 않는다.

## Rollback

다음 값을 끄면 worker apply와 dry-run evidence 생성을 즉시 중단하고 기존 inline 경로만
남는다.

```text
GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED=false
GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED=false
GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH=true
GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE=0
PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED=false
PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false
GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18=true
```

reconcile/routing/budget evidence table과 ERROR/DEAD_LETTER row는 원인 확인 전에 삭제하지
않는다. kill switch를 켜거나 cutover를 끄면 현재 요청부터 inline builder로 돌아가며 이미
accepted된 effective-skip event는 worker가 closure를 완료해야 한다.
