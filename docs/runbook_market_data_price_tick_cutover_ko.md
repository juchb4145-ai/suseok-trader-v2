# Market Data Price Tick Cutover Runbook

## 목적

PR-7은 Gateway ingestion append-only 전환의 첫 실제 cutover 단계다. 범위는 `market_data.price_tick` 하나뿐이다.

이번 단계에서도 전체 append-only 전환은 아니다. `condition_event`, `tr_response`, `market_reference`, `market_index`, `market_regime`, `market_scan`, `condition_fusion`, LIVE_SIM gateway event handling은 inline projection을 유지한다.

`LIVE_REAL`, 주문 정책, safety gate, 매수 gate는 변경하지 않는다.

## 왜 price_tick만 대상인가

`price_tick`은 Gateway request path에서 market_data projection과 incremental evaluation enqueue가 이어지는 단순한 흐름이다. worker가 market_data apply 성공 후 deferred incremental enqueue를 재현할 수 있다.

`tr_response`는 candidate quote refresh enqueue와 synthetic child event 처리가 있고, `condition_event`는 condition_fusion refresh side effect가 있다. 이 둘은 PR-7 대상이 아니며 PR-8에서 별도 migration을 검토한다.

## 필요한 Flags

기본값에서는 cutover가 비활성이다.

필수 조건:

- `GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED=true`
- `GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=true`
- `GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED=true`
- `GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_EVENT_TYPES=price_tick`
- `GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE > 0`
- `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=true`
- `PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=true`

`GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE=0`은 무제한이 아니라 skip disabled다.

권장 시작값:

```powershell
GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE=1
```

## 장중 검증 절차

1. OBSERVE profile과 live flags를 확인한다.
2. `projection_outbox`에 `PENDING`, `ERROR`, `DEAD_LETTER` backlog가 없는지 확인한다.
3. 최신 `market_data_projection_reconcile`이 `PASS`이고 `append_only_ready=true`인지 확인한다.
4. 위 flags를 켠다. skip budget은 1부터 시작한다.
5. Gateway/Core를 OBSERVE로 기동한다.
6. `price_tick` effective skip이 소량 발생하는지 확인한다.
7. projection outbox worker를 run-once apply로 실행한다.
8. `market_tick_samples`, `market_ticks_latest`, `incremental_evaluation_queue`가 생성됐는지 확인한다.
9. reconcile을 다시 실행한다.
10. operator check를 실행한다.

## Worker Run-Once

```powershell
python -m tools.ops_market_data_price_tick_cutover_check `
  --core-url http://127.0.0.1:8000 `
  --limit 100 `
  --timeout-sec 90 `
  --run-once
```

이 명령은 `/api/operator/projection-outbox/run-once?apply_projection=true`와 `/api/operator/market-data-projection-reconcile/run-once`를 호출한 뒤 summary report를 생성한다.

## PASS/WARN/FAIL 해석

PASS:

- `effective_price_tick_skip_count > 0`
- `condition_event_effective_skip_count=0`
- `tr_response_effective_skip_count=0`
- `invalid_effective_skip_count=0`
- worker apply 후 deferred incremental enqueue evidence가 기록됨
- reconcile PASS 유지
- order/LIVE_REAL side effect 없음

WARN:

- skip budget이 0 또는 소진됨
- reconcile stale
- outbox pending backlog가 남아 worker run-once가 필요함
- effective skip은 발생했지만 worker가 아직 deferred enqueue를 처리하지 않음

FAIL:

- `condition_event` 또는 `tr_response` effective skip 발생
- `invalid_effective_skip_count > 0`
- worker apply disabled 상태에서 effective skip 발생
- effective skip 이후 outbox `ERROR`/`DEAD_LETTER`
- deferred incremental enqueue 누락
- latest reconcile FAIL
- `append_only_ready=false`인데 effective skip 발생

## Rollback

즉시 rollback:

```powershell
GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED=false
```

보수적 rollback:

```powershell
GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=false
GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE=0
```

rollback 후 Gateway는 다시 모든 market_data event를 inline projection으로 처리한다.

## PR-8 진입 조건

- 장중 소량 `price_tick` cutover가 반복 PASS
- worker apply 후 `market_tick_samples`와 latest tick 누락 없음
- deferred incremental enqueue evidence 누락 없음
- reconcile PASS 유지
- `condition_event`/`tr_response` effective skip 0 유지
- outbox `ERROR`/`DEAD_LETTER` 0 유지

PR-8은 `tr_response` candidate quote refresh enqueue와 `condition_event` condition_fusion refresh를 worker side effect로 이전할 수 있는지 검토하는 단계다.
