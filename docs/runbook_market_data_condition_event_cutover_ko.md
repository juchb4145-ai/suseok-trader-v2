# Market Data condition_event Cutover Runbook

## 목적

PR-11은 `condition_event` 전용 limited cutover다. 기본값에서는 Gateway POST path가 기존처럼
`process_gateway_event()`와 Gateway-side `condition_fusion` refresh를 수행한다.

소량 운영 검증 시에만 strict flags, fresh reconcile PASS, backlog readiness, worker apply enabled,
condition_fusion enabled, per-minute skip budget을 모두 만족하는 `condition_event`에 대해
Gateway inline market_data projection을 건너뛴다.

## 필요한 Flags

기본값은 모두 rollback-friendly 하다.

```powershell
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED="true"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED="true"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED="true"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE="1"
$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED="true"
$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED="true"
$env:CONDITION_FUSION_EVENT_INCREMENTAL_ENABLED="true"
```

`GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE=0`이면 skip disabled로 해석한다.
장중 첫 검증은 1 또는 매우 작은 값으로 시작한다.

## Cutover Gate

effective skip은 다음 조건을 모두 만족할 때만 발생한다.

- global dry-run enabled
- global market_data append-only cutover enabled
- condition_event cutover enabled
- condition_event skip budget remaining > 0
- projection_outbox apply projection enabled
- projection_outbox market_data apply enabled
- latest market_data reconcile status PASS
- latest reconcile append_only_ready true
- latest reconcile age within max age
- projection_outbox backlog `pr11_condition_event_cutover_ready=true`
- source gateway_event status ACCEPTED
- payload validates as `BrokerConditionEvent`
- condition_fusion incremental enabled
- worker side-effect ready
- candidate ingest in worker not allowed

## Gateway 동작

effective skip이면 Gateway response는 다음 상태를 포함한다.

```json
{
  "market_data": "SKIPPED_INLINE_APPEND_ONLY_CONDITION_EVENT",
  "market_data_effective_skip_inline": "TRUE",
  "condition_fusion": "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_CONDITION_EVENT"
}
```

이 경우 Gateway path는 `process_gateway_event()`와 `condition_fusion` refresh를 실행하지 않는다.
fallback이면 기존처럼 inline market_data projection과 Gateway-side `condition_fusion` refresh를 수행한다.

## Worker 동작

projection_outbox worker가 skipped `condition_event`의 market_data job을 apply한다.

- market_data apply 성공 후 deferred `condition_fusion` refresh 실행
- candidate ingest 실행 금지
- metadata에 `candidate_ingest_executed=false` 기록
- 주문, LIVE_SIM, LIVE_REAL side effect 없음

확인할 metadata 경로:

```text
projection_outbox.metadata_json.last_worker_evidence.post_apply_side_effects
projection_outbox.metadata_json.last_worker_evidence.append_only_cutover
```

## Candidate Ingest와 condition_fusion 차이

`condition_fusion` refresh는 조건식 hit를 code/profile/role별 row로 합성한다.
candidate ingest는 합성된 `candidate_condition_fusion` row를 후보 source로 만드는 별도 단계다.

PR-11 worker는 condition_fusion refresh까지만 수행한다.
`candidate_service.ingest_condition_sources()`는 worker에서 호출하면 안 된다.

## 장중 검증 절차

1. latest reconcile이 PASS이고 `append_only_ready=true`인지 확인한다.
2. `/api/operator/projection-outbox/backlog`에서 `pr11_condition_event_cutover_ready=true`인지 확인한다.
3. skip budget을 1로 설정한다.
4. condition_event 유입 후 `/api/operator/market-data-append-only-routing/status`를 확인한다.
5. worker run-once를 live-safe로 실행한다.
6. reconcile run-once를 실행한다.
7. dashboard snapshot에서 PR-11 condition_event cutover status를 확인한다.

```powershell
python tools/ops_market_data_condition_event_cutover_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --limit 100 `
  --run-once
```

보고서:

```text
reports/market_data_condition_event_cutover/<timestamp>/summary.md
reports/market_data_condition_event_cutover/<timestamp>/raw.json
```

## PASS/WARN/FAIL

PASS:

- condition_event effective skip > 0
- worker apply 후 `market_condition_signals` 생성
- deferred condition_fusion refresh evidence 정상
- candidate_ingest_executed_count = 0
- projection_outbox ERROR/DEAD_LETTER = 0
- reconcile PASS
- no trading/order side effects

WARN:

- cutover flag enabled but skip budget exhausted
- condition_event events 없음
- worker pending within SLA
- reconcile stale/missing
- condition_fusion fused_count=0이지만 error는 아님
- backlog readiness WARN

FAIL:

- condition_event effective skip 발생 후 worker apply disabled
- outbox ERROR/DEAD_LETTER
- artifact missing after worker terminal state
- deferred fusion refresh error
- candidate ingest in worker evidence
- latest reconcile FAIL
- append_only_ready=false인데 condition_event effective skip 발생
- backlog readiness FAIL

## Rollback

즉시 rollback은 둘 중 하나면 충분하다.

```powershell
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED="false"
```

또는:

```powershell
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE="0"
```

문제가 worker apply 쪽이면 다음도 끈다.

```powershell
$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED="false"
$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED="false"
```

주문/LIVE_SIM/LIVE_REAL, safety gate, buy gate 설정은 변경하지 않는다.

## PR-12 진입 조건

- 장중 condition_event cutover PASS
- effective skipped event의 worker applied count 증가 확인
- deferred condition_fusion refresh error 0
- candidate ingest in worker 0
- artifact missing after worker 0
- projection_outbox ERROR/DEAD_LETTER 0
- latest reconcile PASS 유지
- price_tick PR-7, tr_response PR-9 회귀 테스트 통과

PR-12는 market_reference/index/regime 등 비-market_data projection 분리 검토 단계다.
