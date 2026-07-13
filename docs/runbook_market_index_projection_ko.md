# Market Index Projection PR-15 / PR-16 Runbook

## 목적

`market_index_tick` projection을 Gateway inline 경로에서 worker로 옮기기 전에 다음 계약을 검증한다.

- `market_index` outbox worker apply
- accepted event, sample, projection error, outbox terminal 상태 reconcile
- Gateway dry-run routing evidence
- parser confidence와 data usability의 독립 판정
- realtime과 TR bootstrap source의 명시적 구분

PR-15에서는 실제 inline skip을 허용하지 않는다. PR-16은 별도 kill switch, 원자적 budget,
fresh reconcile, worker-side regime 연속성을 모두 통과한 소량 event만 skip한다.

## 절대 안전 조건

```text
TRADING_MODE=OBSERVE
TRADING_ALLOW_LIVE_SIM=false
TRADING_ALLOW_LIVE_REAL=false
GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED=false
GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE=0
GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15=true
```

- 실제 주문, 정정, 취소, SOR 주문을 실행하지 않는다.
- NXT tick은 KRX 지수 evidence로 인정하지 않는다.
- 운영 DB에서 retention apply, destructive migration, 임의 error 삭제를 하지 않는다.
- `.env`를 수정하지 않고 별도 safe env 또는 process-local 환경변수를 사용한다.
- PR-15 준비 점검은 `projection_name=market_index`, 작은 batch로 실행한다. PR-16
  effective-skip 검증은 dependency ordering이 적용된 index/regime sibling 2건만 실행한다.

## 기본 설정

```text
GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED=false
GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED=false
GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE=0
GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_PENDING_WITHIN_SLA=1
GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_RECONCILE_PASS=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_DATA_USABLE=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_PARSER_VERIFIED=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_WORKER_REGIME_REFRESH=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_FAIL_CLOSED_ON_REGIME_REFRESH_ERROR=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_RECONCILE_MAX_AGE_SEC=300
GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_EVENT_AGE_SEC=30
GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_FUTURE_SKEW_SEC=5
GATEWAY_MARKET_INDEX_APPEND_ONLY_REQUIRE_FRESH_GATEWAY_HEALTH=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_GATEWAY_HEALTH_MAX_AGE_SEC=30
GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15=true

PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false
PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED=false
PROJECTION_OUTBOX_MARKET_INDEX_APPLY_BATCH_SIZE=20
PROJECTION_OUTBOX_MARKET_INDEX_APPLY_MIN_AGE_SEC=1.0
```

모든 apply와 dry-run 기본값은 disabled다. worker apply를 점검할 때만 global apply와 market-index apply를 함께 켠다.

PR-16 effective skip은 위 기본값에서 발생하지 않는다. 활성화에는 cutover true, kill switch
false, legacy PR-15 guard false, 양수 budget이 모두 필요하다.

## 품질 계약

### Data usability

`data_usable`은 다음을 의미한다.

- payload가 `BrokerMarketIndexTick`으로 파싱된다.
- price/change-rate sanity guard를 통과한다.
- projection sample이 event id 기준으로 존재한다.
- Dashboard readiness에서는 latest tick이 FRESH/DEGRADED이고 설정된 bar interval이 모두 존재한다.

### Parser confidence

`parser_verified`는 payload metadata의 `parser_status=VERIFIED`인 경우에만 true다.

- metadata가 없으면 `UNKNOWN`이다.
- `PILOT_UNVERIFIED_FID_MAP`은 usable data와 별개로 unverified다.
- unverified parser는 reconcile projection 정합성 자체를 FAIL로 만들지 않지만, 기본 cutover readiness를 false로 만든다.

### Realtime FID probe 계약

Kiwoom 개발가이드의 업종 실시간 타입을 기준으로 `업종지수`와 `업종등락`은 FID
`20/10/11/12`를 각각 체결시간/현재가/전일대비/등락률로 읽는다. `예상업종지수`는 같은
tick 계약으로 파싱하지 않고 관측 후 무시한다.

- 네 FID 중 하나라도 비어 있거나 숫자 파싱에 실패하면 event를 만들지 않는다.
- FID 20은 정확한 6자리 `HHMMSS`만 허용하며 현재 시각 fallback을 사용하지 않는다.
- 성공 event metadata는 `raw_fids`에 네 원문 값을 그대로 보존한다.
- 실패 evidence는 `INDEX_PRICE_*`, `INDEX_CHANGE_VALUE_*`, `INDEX_CHANGE_RATE_*`,
  `INDEX_TRADE_TIME_*`, `INDEX_REAL_TYPE_UNSUPPORTED` reason을 구분한다.
- 정적 문서 mapping 확인만으로 `PILOT_UNVERIFIED_FID_MAP`을 승격하지 않는다. 실제 KRX
  장중 callback에서 raw FID, parsed value, timestamp와 freshness를 대조한 뒤 별도 승인한다.

운영 DB 대신 명시적 격리 DB로 FID callback만 확인한다.

```powershell
$probeDb = Join-Path $env:TEMP 'market-index-realtime-fid.sqlite3'
.\tools\start_market_open_observe.ps1 `
  -RealtimeFidValidation `
  -DbPath $probeDb `
  -RunCore `
  -RunGateway
```

이 모드는 condition profile/stock realtime/theme refresh와 TR bootstrap을 끄고 market-index
realtime만 남긴다. 명시적 `-AllowOperatingDatabase` 없이는 운영 DB 경로를 거부한다.

### Source

- `REALTIME`: metadata source가 `KIWOOM_REALTIME_MARKET_INDEX` 등 명시적 realtime source다.
- `TR_BOOTSTRAP`: metadata source가 명시적 TR bootstrap source다.
- `UNKNOWN`: source를 판정할 수 없다.

TR bootstrap adapter는 generic `request_tr`와 v1 source lineage contract로 구현되어 있다.
유효한 parent `tr_response`는 `TR_BOOTSTRAP` source로 집계하며 source/contract/index/TR
lineage가 없거나 다르면 FAIL한다. `OPT20001` parser는 2026-07-10 KOA Studio와 실제
비장중 callback으로 `VERIFIED`됐지만 `TR_BOOTSTRAP` source는 realtime cutover 및 연속
10거래일 KRX anchor에서 계속 제외한다. 상세 절차는
`docs/runbook_market_index_tr_bootstrap_ko.md`를 따른다.

## API

```text
GET  /api/operator/market-index-projection-reconcile/latest
POST /api/operator/market-index-projection-reconcile/run-once
GET  /api/operator/market-index-append-only-routing/status
GET  /api/operator/market-index-append-only-routing/decisions
GET  /api/operator/market-index/tr-bootstrap/status
POST /api/operator/market-index/tr-bootstrap/run-once
POST /api/operator/projection-outbox/run-once
GET  /api/operator/projection-outbox/status
```

POST endpoint는 local token을 요구한다.

## 권장 점검 순서

1. Core가 OBSERVE이고 LIVE_SIM/LIVE_REAL이 false인지 확인한다.
2. Gateway heartbeat에서 `market_index_realtime_enabled`, registered codes, callback/parse-error count를 확인한다.
3. 최근 accepted `market_index_tick`의 metadata source와 parser status를 확인한다.
4. 작은 worker batch를 실행한다.

```powershell
Invoke-RestMethod `
  -Method Post `
  -Headers @{ 'X-Local-Token' = $env:TRADING_CORE_TOKEN } `
  -Uri 'http://127.0.0.1:8000/api/operator/projection-outbox/run-once?projection_name=market_index&limit=1&apply_projection=true&live_safe=true'
```

5. reconcile을 실행한다.

```powershell
Invoke-RestMethod `
  -Method Post `
  -Headers @{ 'X-Local-Token' = $env:TRADING_CORE_TOKEN } `
  -Uri 'http://127.0.0.1:8000/api/operator/market-index-projection-reconcile/run-once?limit=100&persist=true&live_safe=true'
```

6. routing status에서 `effective_skip_inline_count=0`을 확인한다.
7. outbox `ERROR/DEAD_LETTER=0`, command/order-command delta `0/0`을 확인한다.
8. Dashboard fast section의 index/reconcile/routing 값이 API와 일치하는지 확인한다.

## Ops script

조회와 reconcile만 실행:

```powershell
python tools\ops_market_index_projection_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN
```

`market_index` worker 1건을 먼저 실행:

```powershell
python tools\ops_market_index_projection_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-worker
```

PR-16 effective skip 1건과 worker-side index/regime closure를 확인:

```powershell
python tools\ops_market_index_projection_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-worker `
  --expect-effective-skip
```

이 모드는 worker batch 2건을 사용해 sibling `market_index`와 `market_regime` outbox를
함께 닫는다. 다른 projection backlog가 섞인 운영 환경에서는 먼저 outbox ordering을 확인한다.

report는 `reports/market_index_projection/<UTC stamp>/raw.json`과 `summary.md`에 기록된다.

## PASS/WARN/FAIL

PR-15 준비 PASS:

- Core OBSERVE, LIVE_SIM/LIVE_REAL false
- KOSPI/KOSDAQ accepted event coverage
- sample missing/projection error 0
- index outbox ERROR/DEAD_LETTER 0
- data unusable 0
- explicit REALTIME source
- parser verified
- effective skip 0
- command/order-command delta 0/0

PR-16 `--expect-effective-skip` PASS:

- effective skip 1건 이상과 budget 범위 준수
- 해당 event의 index worker `APPLIED_BY_WORKER`
- event-linked regime snapshot과 index/regime outbox `APPLIED`
- effective-skip pending/error/evidence/artifact/regime missing 0
- controller PASS, rollback false, command/order-command delta 0/0

WARN:

- event 없음 또는 outbox PENDING이 SLA 이내
- parser unverified/unknown이지만 data usable
- source UNKNOWN

FAIL:

- sample missing, projection error, stale outbox
- outbox ERROR/DEAD_LETTER/SKIPPED
- data unusable
- TR bootstrap v1 source/contract/index/parent lineage 위반 또는 row parse 실패
- PR-15 준비 점검에서 effective skip 1건 이상
- PR-16 기대 모드에서 effective skip 또는 linked index/regime closure 누락
- 주문 command 증가 또는 OBSERVE 안전 조건 위반

## PR-16 limited cutover gate

effective skip은 다음 조건을 모두 만족할 때만 1건 예약된다.

- dry-run과 cutover true
- trading profile/mode OBSERVE, LIVE_SIM/LIVE_REAL 허용 false
- global kill switch false
- `GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15=false`
- budget 1/min 이상이며 현재 minute 잔여량 존재
- latest reconcile PASS, append-only ready, freshness SLA 이내
- current payload data usable, parser verified, source REALTIME
- event age 30초 이내, future skew 5초 이내, KRX 평일 REGULAR session
- Gateway heartbeat/latest index tick 30초 이내, realtime enabled, adapter `CALLBACK_ACTIVE`, parsed tick 존재
- market-regime enabled와 worker regime refresh guard true
- reconcile/data-usability/parser guard와 regime refresh fail-closed 설정 모두 true
- index/regime outbox ERROR/DEAD_LETTER 0, 각각 pending SLA 이내
- 이전 effective skip의 index sample, index worker evidence, event-linked regime snapshot,
  index/regime outbox APPLIED가 모두 존재

worker는 effective-skip event의 index projection 후 regime snapshot evidence에
`source_event_id`, `source_projection=market_index`, `generated_by`를 기록한다. regime refresh가
실패하면 index outbox를 retryable PENDING/DEAD_LETTER로 전환하고 다음 skip을 막는다. 같은 lineage는
schema 50 nullable column에도 기록되며 source-event partial index로 routing 조회를 제한한다.

regime refresh 오류는 index artifact를 지우지 않고 해당 index outbox를 retryable `PENDING`으로
되돌린다. 다음 worker run은 effective-skip lineage를 확인해 orphaned artifact를
`MARKET_INDEX_WORKER_ARTIFACT_RECOVERED`/`APPLIED_BY_WORKER`로 닫는다. 반복 실패가
`PROJECTION_OUTBOX_RETRY_LIMIT`에 도달하면 `DEAD_LETTER`가 되며 자동 cutover는 계속 차단된다.

claim ordering은 같은 event의 `market_index` job을 sibling `market_regime`보다 먼저 처리하되
다른 event의 전역 priority를 바꾸지 않는다. enqueue commit 직후 worker가 routing decision보다
먼저 실행되어도 cutover가 armed 상태이면 linked regime snapshot을 먼저 닫는다. 이미 APPLIED인
index job이 linked snapshot, regime APPLIED,
`APPLIED_BY_WORKER` evidence를 갖추지 못한 경우 routing은 effective skip 대신 inline fallback한다.

같은 Kiwoom callback 묶음에서 budget 1/min의 첫 event만 effective skip되고 후속 event가 inline으로
먼저 최신 snapshot을 갱신할 수 있다. 이때 worker가 처리하는 앞선 event는
`market_index_tick_samples`에 sample-only로 적재한다. `market_index_ticks_latest`와 bar는 되감지
않지만 event-linked regime/context는 해당 sample을 기준으로 완성한다. sample 없이
`MARKET_INDEX_OLDER_THAN_LATEST`로 종결하면 reconcile FAIL이며 cutover를 계속 닫는다.

## Rollback

PR-15는 inline 경로를 제거하지 않으므로 다음 값을 끄면 즉시 기존 동작만 남는다.

```text
GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED=false
GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH=true
GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED=false
PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED=false
PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false
```

reconcile/routing table은 evidence이므로 삭제하지 않는다. ERROR/DEAD_LETTER row도 원인 확인 전 임의 정리하지 않는다.

## PR-16 활성화 조건

- reconcile PASS와 `append_only_ready=true`
- parser verified와 data usable을 각각 확인
- explicit REALTIME source, KOSPI/KOSDAQ coverage
- worker apply evidence 존재
- index outbox ERROR/DEAD_LETTER 0
- PR-15 effective skip 0
- command/order-command delta 0/0

2026-07-13 실제 KRX session에서 KOSPI/KOSDAQ FID `10/11/12/20`, parser `VERIFIED`,
freshness/context/regime continuity와 1/min limited skip을 확인했다. 이 단일 거래일 evidence는
연속 10거래일 gate를 대체하지 않는다. realtime path는 계속 1/min 한도와 OBSERVE-safe DB에서만
검증하며 qualified 10일과 운영 승인 전에는 기본값 활성화 또는 inline fallback 제거를 금지한다.
