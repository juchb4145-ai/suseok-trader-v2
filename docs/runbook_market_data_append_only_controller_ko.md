# MarketData Append-Only Controller Runbook

## 목적

PR-12 controller는 `price_tick`, `tr_response`, `condition_event` limited cutover를 하나의 운영 모드로 묶어 관리한다. 기본값은 fail-closed이며, 주문/LIVE_SIM/LIVE_REAL/safety/buy gate는 변경하지 않는다.

## 운영 모드

- `OFF`: 모든 effective skip 금지. 기본값.
- `DRY_RUN`: would-skip evidence만 기록하고 effective skip 금지.
- `PRICE_TICK_ONLY`: `price_tick`만 controller gate 통과 가능.
- `TR_RESPONSE_ONLY`: `tr_response`만 controller gate 통과 가능.
- `CONDITION_EVENT_ONLY`: `condition_event`만 controller gate 통과 가능.
- `MARKET_DATA_LIMITED`: 세 event type을 per-type budget과 health gate 아래에서 허용.
- `MARKET_DATA_FULL_GUARDED`: `MARKET_DATA_LIMITED`와 같지만 backlog PASS 등 더 엄격한 health를 요구.

## Global Kill Switch

`GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH=true`이면 모든 market_data effective skip이 금지된다. 장중 이상 징후가 있으면 이 값을 true로 되돌리고 core를 재시작한다.

## Budget

- Global: `GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE`
- Price tick: `GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE`
- TR response: `GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE`
- Condition event: `GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE`

장중 첫 검증은 global/per-type 모두 `1~3/min` 수준으로 시작한다. budget이 0이면 해당 gate는 fail-closed다.

## Auto Rollback 조건

다음 중 하나라도 발생하면 controller는 `auto_rollback_required=true`로 보고하고 routing은 inline fallback을 강제한다. 설정 파일은 자동 수정하지 않고 `market_data_append_only_auto_rollback_events`에 evidence를 저장한다.

- projection outbox `ERROR` 또는 `DEAD_LETTER` threshold 초과
- invalid effective skip count > 0
- condition_event worker에서 `candidate_ingest_executed_count > 0`
- latest market_data reconcile `FAIL`
- effective skip 이후 `append_only_ready=false`
- backlog readiness `FAIL`
- worker apply disabled while cutover flag/mode enabled
- stale `PROCESSING` outbox 존재
- deferred side-effect error count > 0

## 장중 condition_event 소량 활성화 절차

1. `TRADING_MODE=OBSERVE`, `TRADING_ALLOW_LIVE_SIM=false`, `TRADING_ALLOW_LIVE_REAL=false` 확인.
2. `GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE=CONDITION_EVENT_ONLY` 설정.
3. `GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH=false` 설정.
4. `GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED=true`, `GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=true` 확인.
5. `GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED=true` 설정.
6. global budget과 condition_event budget을 `1~3/min`으로 설정.
7. `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=true`, `PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=true` 확인.
8. 짧은 batch로 worker run-once 실행:

```powershell
Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/projection-outbox/run-once?limit=5&apply_projection=true&live_safe=true" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

9. controller/ops check 실행:

```powershell
python -m tools.ops_market_data_append_only_controller_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --out-dir reports/market_data_append_only_controller
```

10. 다음을 확인한다.

- `condition_event_effective_skip_count > 0`
- `market_condition_signals`가 worker apply 이후 생성됨
- worker metadata의 `condition_fusion_refresh_status=APPLIED`
- `candidate_ingest_executed_count = 0`
- projection outbox `ERROR/DEAD_LETTER = 0`
- latest reconcile `PASS`, `append_only_ready=true`
- dashboard fast path의 `market_data_append_only_controller.status = PASS`

## Rollback 절차

1. `GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH=true` 설정.
2. 필요하면 `GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE=OFF`로 설정.
3. global/per-type budget을 0으로 되돌림.
4. core 재시작 후 controller status에서 `effective_cutover_enabled=false` 확인.
5. outbox backlog/reconcile을 다시 실행해 inline fallback 상태에서 `PASS` 또는 허용 가능한 `WARN`인지 확인.

## PR-13 진입 조건

- 장중 controller verdict `PASS`
- condition_event, price_tick, tr_response 각각 invalid effective skip 0
- `candidate_ingest_executed_count = 0`
- outbox `ERROR/DEAD_LETTER = 0`
- reconcile `PASS` 유지
- dashboard/controller/ops evidence가 모두 주문/LIVE_SIM/LIVE_REAL behavior unchanged를 표시

이 조건이 충족된 뒤 PR-13에서 market_reference/index/regime 등 non-market_data projection 분리를 검토한다.
