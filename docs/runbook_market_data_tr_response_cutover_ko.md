# Market Data TR Response Cutover Runbook

## 목적

PR-9는 MarketData append-only 전환의 두 번째 limited cutover 단계다. 범위는 `market_data.tr_response` 중 candidate quote refresh 응답에 한정한다.

기본값에서는 production behavior가 변하지 않는다. 실제 skip은 feature flag, reconcile PASS, worker apply enabled, synthetic child guard, per-minute budget이 모두 맞을 때만 발생한다.

`LIVE_REAL`, 주문 정책, safety gate, 매수 gate는 변경하지 않는다.

## 왜 condition_event는 제외하는가

`condition_event`는 market_data projection 뒤에 condition_fusion refresh가 이어진다. PR-9는 candidate quote refresh `tr_response` side-effect만 worker로 이전한 단계이므로, `condition_event`는 inline projection과 Gateway path side-effect를 유지한다.

`condition_event` effective skip이 발견되면 operator status, reconcile, cutover check에서 FAIL로 본다.

## 필요한 Flags

필수 조건:

- `GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED=true`
- `GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=true`
- `GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED=true`
- `GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE > 0`
- `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=true`
- `PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=true`
- 최신 `market_data_projection_reconcile.status=PASS`
- 최신 `market_data_projection_reconcile.append_only_ready=true`

기본값:

- `GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED=false`
- `GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE=0`

`MAX_SKIP_PER_MINUTE=0`은 무제한이 아니라 skip disabled다.

권장 시작값:

```powershell
GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE=1
```

## 장중 검증 절차

1. OBSERVE profile인지 확인한다.
2. LIVE_REAL/LIVE_SIM/order routing flags가 꺼져 있는지 확인한다.
3. projection_outbox pending/error/dead-letter backlog를 정리한다.
4. market_data projection reconcile을 run-once로 실행해 PASS를 확인한다.
5. tr_response cutover flag와 budget 1을 설정한다.
6. Gateway/Core를 OBSERVE로 기동한다.
7. candidate quote refresh TR을 소량 발생시킨다.
8. `/api/operator/market-data-append-only-routing/status`에서 `tr_response_effective_skip_count > 0`, `condition_event_effective_skip_count=0`, `invalid_effective_skip_count=0`을 확인한다.
9. projection_outbox worker run-once apply를 실행한다.
10. reconcile run-once를 실행해 PASS를 확인한다.
11. cutover check script를 실행한다.

## Worker Run-Once

```powershell
python -m tools.ops_market_data_tr_response_cutover_check `
  --core-url http://127.0.0.1:8000 `
  --limit 100 `
  --timeout-sec 90 `
  --run-once
```

이 명령은 projection_outbox worker apply와 market_data reconcile run-once를 호출한 뒤 report를 생성한다.

## PASS/WARN/FAIL

PASS:

- `tr_response_effective_skip_count > 0`
- `condition_event_effective_skip_count=0`
- `invalid_effective_skip_count=0`
- worker apply 후 `market_tr_snapshots` 생성
- worker apply 후 deferred candidate quote refresh evidence 정상
- projection_outbox ERROR/DEAD_LETTER 0
- latest reconcile PASS
- trading/order side-effect 없음

WARN:

- tr_response cutover flag는 켜졌지만 budget이 0 또는 소진
- tr_response event 또는 effective skip 미관측
- worker pending within SLA 존재
- reconcile stale 또는 missing
- candidate quote refresh code_count 0 다수

FAIL:

- condition_event effective skip 발생
- invalid effective skip 발생
- tr_response effective skip 발생 후 worker apply disabled
- deferred quote refresh error 발생
- effective skipped tr_response outbox ERROR/DEAD_LETTER
- terminal worker state인데 `market_tr_snapshots` 없음
- latest reconcile FAIL
- append_only_ready=false인데 effective tr_response skip 발생

## Rollback

즉시 rollback:

```powershell
GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED=false
```

보수적 rollback:

```powershell
GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=false
GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE=0
```

rollback 후 `tr_response`는 다시 Gateway request path에서 inline market_data projection과 candidate quote refresh enqueue를 수행한다.

## PR-10 진입 조건

- 장중 소량 tr_response cutover 반복 PASS
- worker apply 후 `market_tr_snapshots`와 synthetic child tick 누락 없음
- deferred candidate quote refresh evidence 누락 없음
- reconcile PASS 유지
- condition_event effective skip 0 유지
- outbox ERROR/DEAD_LETTER 0 유지

PR-10은 condition_event side-effect migration 준비 단계로 진행한다.
