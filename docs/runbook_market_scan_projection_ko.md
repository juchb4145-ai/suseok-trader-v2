# Market Scan Projection 운영 Runbook

## 목적

PR-20은 `market_scan` worker apply, reconcile, append-only dry-run evidence를 준비한다.
PR-21은 같은 계약 위에 global kill switch, 원자적 budget, worker closure와 즉시 rollback을
추가해 제한적으로 Gateway inline scan projection을 건너뛸 수 있게 한다.

## 안전 기본값

- `TRADING_PROFILE=OBSERVE`
- `TRADING_MODE=OBSERVE`
- `TRADING_ALLOW_LIVE_SIM=false`
- `TRADING_ALLOW_LIVE_REAL=false`
- `MARKET_SCAN_ENABLED=false`
- `PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED=false`
- `GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED=false`
- `GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED=false`
- `GATEWAY_MARKET_SCAN_APPEND_ONLY_GLOBAL_KILL_SWITCH=true`
- `GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE=0`
- `GATEWAY_MARKET_SCAN_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR20=true`

실제 주문, 정정, 취소, SOR 주문은 이 경로에서 생성하지 않는다. scan worker가 변경할 수
있는 projection은 `market_scan`뿐이며 candidate ingest도 실행하지 않는다.

## 구조 계약

schema 54는 source lineage와 reconcile/routing 준비 table을 additive하게 추가한다.

- `market_scan_snapshots`와 `market_scan_latest`의 `source_event_id`, `request_id`,
  `parser_status`, `generated_by`
- `market_scan_projection_reconcile_runs/issues`
- `market_scan_projection_routing_decisions`
- `source_event_id`와 `request_id` 조회 index

schema 55는 routing table의 cutover/kill-switch/event-freshness/budget/rollback/controller
evidence, effective-skip index와 `market_scan_append_only_budget_state`를 additive하게 추가한다.

scan id는 Python process hash 대신 SHA-256 기반 결정적 id를 사용한다. 늦게 처리된 과거
이벤트는 snapshot에는 보존되지만 `market_scan_latest.scanned_at`을 과거로 되돌리지 않는다.

## 실제 Kiwoom TR 계약

2026-07-11 로컬 OpenAPI contract와 모의투자 callback을 대조한 계약은 다음과 같다.

| scan | TR | input | output | first-page fields |
|---|---|---|---|---|
| TRADE_VALUE | `OPT10032` | `시장구분`, `관리종목포함=0`, `거래소구분=1` | `거래대금상위` multi | `종목코드`, `종목명`, `현재순위`, `현재가`, `등락률`, `거래대금`, `현재거래량` |
| CHANGE_RATE | `OPT10027` | `시장구분`, `정렬구분=1`, `거래량조건=0`, `종목조건=1`, `신용조건=0`, `상하한포함=0`, `가격조건=0`, `거래대금조건=0`, `거래소구분=1` | `전일대비등락률상위` multi | `종목코드`, `종목명`, `현재가`, `등락률`, `현재거래량` |

KOSPI/KOSDAQ 모두 `OPT10032=100행`, `OPT10027=200행`, success true였고 네 command는
ACKED, failed 0이었다. 첫 페이지의 `continuation_key=2`는 다음 페이지 존재를 뜻하지만 현재
market-scan은 자동 pagination을 수행하지 않는다. 이 실증은 첫 페이지 parser/projection
계약이며 전체 연속조회 완료 근거가 아니다.

저장소 기본 parser status는 안전하게 `PILOT_UNVERIFIED`를 유지한다. 이 설치본처럼 KOA
contract와 실제 callback을 확인한 격리 runtime에서만 Core/Gateway 시작 전에 다음 값을
process env로 명시한다. 운영 `.env`를 자동 변경하지 않는다.

```powershell
$env:MARKET_SCAN_PARSER_STATUS = 'KOA_STUDIO_VERIFIED'
```

## Worker 선행 조건

scan 관련 `tr_response`에는 `market_data`와 `market_scan` outbox job이 함께 생성된다.
worker는 같은 이벤트의 `market_data` artifact 또는 APPLIED outbox를 먼저 확인한다.
선행 projection이 없으면 scan job은 retryable error로 PENDING에 복귀하고 retry limit 이후에만
DEAD_LETTER가 된다.

worker apply를 격리 환경에서만 준비할 때 사용하는 설정은 다음과 같다.

```text
PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=true
PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=true
PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED=true
PROJECTION_OUTBOX_MARKET_SCAN_APPLY_BATCH_SIZE=1
PROJECTION_OUTBOX_MARKET_SCAN_APPLY_MIN_AGE_SEC=0
```

Core 기동 전 별도 safe env의 DB path와 OBSERVE-safe 상태를 확인한다. 저장소 `.env`를 직접
수정하지 않는다.

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/status
Invoke-RestMethod -Method Post `
  'http://127.0.0.1:8000/api/operator/projection-outbox/run-once?projection_name=market_scan&limit=1&apply_projection=true&live_safe=true' `
  -Headers @{'X-Local-Token'=$env:TRADING_CORE_TOKEN}
```

worker evidence는 다음을 만족해야 한다.

- `market_scan_apply_enabled=true`
- `applied_by_worker_count` 또는 `applied_by_verify_count`가 기대 건수와 일치
- worker mutation이 발생했다면 `mutated_projection_names=['market_scan']`
- `candidate_ingest_executed=false`
- command/order-command delta `0/0`
- outbox `ERROR=0`, `DEAD_LETTER=0`

## Reconcile

```powershell
Invoke-RestMethod -Method Post `
  'http://127.0.0.1:8000/api/operator/market-scan-projection-reconcile/run-once?limit=100&persist=true&live_safe=true' `
  -Headers @{'X-Local-Token'=$env:TRADING_CORE_TOKEN}
```

PASS 조건은 최신 scan stream별로 다음을 모두 충족하는 것이다.

- accepted scan TR source row가 snapshot 또는 명시적 row error로 terminal 처리됨
- row projection error가 없음
- parser status가 `VERIFIED`, `KOA_STUDIO_VERIFIED`, `PRODUCTION_VERIFIED` 중 하나임
- 성공 응답에 usable row가 있음
- 같은 이벤트의 `market_data` dependency가 완료됨
- scan outbox가 APPLIED이고 ERROR/DEAD_LETTER가 없음

기본 `MARKET_SCAN_PARSER_STATUS=PILOT_UNVERIFIED`에서는 reconcile이 WARN이며 cutover 준비
PASS로 해석하지 않는다.

## Dry-run Routing

prior event reconcile PASS 뒤 아래 flag로 새 scan 이벤트의 `would_skip_inline`만 관측한다.

```text
GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_REQUIRE_RECONCILE_PASS=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_REQUIRE_PARSER_VERIFIED=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_REQUIRE_MARKET_DATA_DEPENDENCY=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_PENDING_WITHIN_SLA=4
GATEWAY_MARKET_SCAN_APPEND_ONLY_RECONCILE_MAX_AGE_SEC=300
GATEWAY_MARKET_SCAN_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR20=true
```

PR-20 준비 검증의 기대 결과는 `would_skip_inline>0`, `effective_skip_inline=0`, inline
`market_scan` APPLIED다.

## PR-21 제한적 Cutover

다음 조건을 모두 만족할 때만 현재 event의 budget을 원자적으로 예약하고 inline scan을
건너뛴다.

- OBSERVE profile/mode, LIVE_SIM/LIVE_REAL false
- dry-run/cutover ON, global kill switch OFF, PR-20 guard OFF
- `MARKET_SCAN_ENABLED=true`, global/scan worker apply ON
- 직전 accepted scan event를 덮는 fresh reconcile PASS와 append-only-ready
- parser VERIFIED, usable rows, current event age/future skew 허용 범위
- 같은 event의 market_data artifact 또는 ordered worker dependency
- scan outbox PENDING/PROCESSING, backlog SLA, ERROR/DEAD_LETTER 0
- 이전 effective skip의 outbox APPLIED, `APPLIED_BY_WORKER`, event-linked snapshot과
  `projection_outbox_worker:*` lineage 완료
- global budget 잔여량 1건 이상

최소 검증 설정은 다음과 같다.

```text
GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_GLOBAL_KILL_SWITCH=false
GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE=1
GATEWAY_MARKET_SCAN_APPEND_ONLY_REQUIRE_PRIOR_EVENT_RECONCILE=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_REQUIRE_WORKER_CLOSURE=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_FAIL_CLOSED_ON_WORKER_ERROR=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_EVENT_AGE_SEC=120
GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_FUTURE_SKEW_SEC=5
GATEWAY_MARKET_SCAN_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR20=false
```

같은 event를 재평가해도 budget을 다시 소비하지 않는다. budget은 venue별로 초기화하지 않는
`market_scan_global` 단일 bucket이다. worker closure 전 새 event, worker ERROR/DEAD_LETTER,
artifact/lineage 누락 또는 backlog 초과가 발생하면 다음 event부터 inline으로 즉시 복귀한다.

## 통합 점검

```powershell
python tools/ops_market_scan_projection_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-worker `
  --expect-dry-run-ready
```

PR-21 effective skip 소량 검증은 worker가 market_data sibling을 먼저 닫을 수 있도록
unfiltered batch를 사용한다. 실제 4-command scan cycle에는 `worker-limit 20`을 사용했다.

```powershell
python tools/ops_market_scan_projection_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-worker `
  --worker-limit 20 `
  --expect-dry-run-ready `
  --expect-effective-skip
```

결과는 `reports/market_scan_projection/<UTC>/raw.json`과 `summary.md`에 저장된다.

2026-07-11 실제 off-hours evidence는 controller/reconcile `PASS`, checked event `9`,
source/projected row `1300/1300`, effective skip `2`, worker closure gap `0`, outbox
ERROR/DEAD_LETTER `0/0`, command/order-command delta `0/0`, candidate ingest `0`이었다.
Report: `reports/market_scan_projection/20260710T231634Z/summary.md`.

## NXT 사용 원칙

NXT는 event의 `exchange` 또는 `venue`가 실제 `NXT`로 식별되고 scan 입력 계약이
venue-neutral임이 확인된 경우에만 worker ordering, outbox, lock, replay 검증에 사용할 수 있다.
기본 `OPT10032`/`OPT10027` KOSPI/KOSDAQ scan은 KRX 기준이므로 NXT tick만으로 parser 검증,
market coverage 또는 KRX 장중 PASS를 만들지 않는다. venue를 식별할 수 없는 SOR/통합시세는
`UNKNOWN/SOR`로 분리하고 NXT evidence로 인정하지 않는다.

## 즉시 복귀

오류가 관측되면 다음 flag를 false로 되돌리고 inline 경로를 유지한다.

```text
GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED=false
GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED=false
GATEWAY_MARKET_SCAN_APPEND_ONLY_GLOBAL_KILL_SWITCH=true
GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE=0
PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED=false
```

DB row를 삭제하거나 retention apply를 실행하지 않는다. 원인은 reconcile issue와 outbox
worker evidence로 보존한다.
