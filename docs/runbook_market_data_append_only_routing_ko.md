# Market Data Append-Only Routing Dry-Run Runbook

## 목적

PR-6는 Gateway market_data inline projection을 끄는 PR이 아니다. Gateway POST path의 `process_gateway_event()`는 계속 실행된다. 이 단계는 실제로 inline projection을 생략한다면 어떤 `price_tick`/`condition_event`/`tr_response`가 outbox-only 처리 가능해 보이는지 dry-run decision과 evidence로 기록하는 절차다.

## 기본 안전 조건

- `GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED=false`가 기본값이다.
- `GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=false`가 기본값이다.
- PR-6에서는 cutover flag가 true여도 `effective_skip_inline`은 항상 false여야 한다.
- LIVE_REAL, 주문 정책, safety gate, buy gate는 변경하지 않는다.
- projection outbox worker/apply worker 기본값은 disabled다.

## 설정

장중 dry-run 관측을 하려면 Core 환경에 아래만 제한적으로 켠다.

```powershell
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED = "true"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_REQUIRE_RECONCILE_PASS = "true"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_RECONCILE_MAX_AGE_SEC = "300"
```

`GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=true`는 PR-6에서 실제 skip을 만들지 않는다. 해당 flag가 켜져 있으면 routing evidence에 `EFFECTIVE_SKIP_DISABLED_IN_PR6`가 남아야 정상이다.

## 장중 검증 절차

1. PR-5 reconcile을 최신 실제 장 데이터 범위로 `PASS`가 되게 만든다.
2. Core/Gateway를 OBSERVE 또는 기존 안전 프로필로 실행한다.
3. dry-run flag를 켠 상태에서 실제 `price_tick`/`condition_event`/`tr_response`가 들어오게 둔다.
4. operator status를 확인한다.

```powershell
python -m tools.ops_market_data_append_only_routing_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --out-dir reports/market_data_append_only_routing
```

## 판정

PASS:

- `effective_skip_inline_count=0`
- 최신 reconcile `status=PASS`
- `append_only_ready=true`
- dry-run enabled 상태에서 일부 `would_skip_inline=True`
- Gateway inline projection status가 계속 정상 기록
- 주문/거래 side effect 없음

WARN:

- dry-run disabled
- 최신 reconcile이 없거나 stale
- outbox pending/error가 남아 있음
- checked event가 있는데 `would_skip_inline_count=0`

FAIL:

- `effective_skip_inline_count > 0`
- `append_only_ready=false`인데 `would_skip_inline=True`
- 최신 reconcile `FAIL`
- routing decision 저장 실패
- dashboard/operator status 불일치

## Operator/API 확인

```text
GET /api/operator/market-data-append-only-routing/status
GET /api/operator/market-data-append-only-routing/decisions?limit=100
GET /api/dashboard/snapshot
```

Dashboard snapshot에는 `market_data_append_only_routing`과 `pipeline_summary.market_data_append_only_routing`이 포함된다.

## PR-7 진입 조건

- 실제 장 데이터에서 PR-5 reconcile `PASS`가 반복 확인된다.
- PR-6 dry-run에서 `effective_skip_inline_count=0`이 유지된다.
- `would_skip_inline=True` 이벤트의 inline projection 결과가 계속 정상이다.
- projection_outbox backlog/error/dead_letter가 운영 기준 이하로 유지된다.
- rollback 기준과 관측 대시보드가 준비되어 있다.

## Rollback

PR-6 rollback은 dry-run flag를 끄는 것으로 충분하다.

```powershell
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED = "false"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED = "false"
```

DB decision row는 관측 evidence이며 주문/거래 상태에 영향을 주지 않는다.
