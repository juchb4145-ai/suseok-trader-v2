# Market Regime Projection PR-18 Runbook

## 목적과 범위

PR-18은 공통 `market_context`를 생성하는 `market_regime` projection outbox worker,
reconcile, Gateway dry-run routing을 준비한다. 이 단계는 append-only cutover가 아니다.
Gateway inline market-regime/context builder는 그대로 실행하고
`effective_skip_inline`은 코드와 schema evidence에서 항상 false다.

실제 주문, 정정, 취소, SOR, LIVE_SIM/LIVE_REAL 동작은 변경하지 않는다. 운영 `.env`와
운영 DB는 수정하지 않고 별도 OBSERVE-safe process 환경과 격리 DB에서 먼저 검증한다.

## 안전 기본값

```text
TRADING_PROFILE=OBSERVE
TRADING_MODE=OBSERVE
TRADING_ALLOW_LIVE_SIM=false
TRADING_ALLOW_LIVE_REAL=false

GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED=false
GATEWAY_MARKET_REGIME_APPEND_ONLY_REQUIRE_RECONCILE_PASS=true
GATEWAY_MARKET_REGIME_APPEND_ONLY_RECONCILE_MAX_AGE_SEC=300
GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18=true

PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false
PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED=false
PROJECTION_OUTBOX_MARKET_REGIME_APPLY_BATCH_SIZE=20
PROJECTION_OUTBOX_MARKET_REGIME_APPLY_MIN_AGE_SEC=1.0
```

worker apply와 dry-run은 기본 disabled다. PR-18에서 effective skip guard를 끄는 것은
활성화 절차가 아니며 routing readiness를 fail-closed하는 설정 오류로 판정한다.

## 데이터 계약

- schema 52는 `market_regime_projection_reconcile_runs/issues`와
  `market_regime_projection_routing_decisions`를 additive로 추가한다.
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

WARN:

- event pair 누락, stale, parser unverified, data unusable
- reconcile WARN 또는 common context WARN
- 이 상태는 PR-19 cutover 진입 evidence로 사용할 수 없다.

FAIL:

- index dependency, outbox, context watermark 또는 source-regime lineage 누락
- regime outbox ERROR/DEAD_LETTER
- reconcile FAIL
- effective skip 1건 이상 또는 PR-18 guard 비활성화
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
PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED=false
PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false
GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18=true
```

reconcile/routing evidence table과 ERROR/DEAD_LETTER row는 원인 확인 전에 삭제하지 않는다.
PR-19 limited cutover는 별도 kill switch, 원자적 budget, fresh health/session guard와 장중 KRX
evidence를 갖춘 독립 PR로 진행한다.
