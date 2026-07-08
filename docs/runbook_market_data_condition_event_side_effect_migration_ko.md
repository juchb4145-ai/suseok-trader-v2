# Market Data condition_event Side-Effect Migration Runbook

## 목적

PR-10은 `condition_event` cutover가 아니다. Gateway POST path의 `condition_event`
inline `process_gateway_event()`와 `condition_fusion` refresh는 계속 실행된다.

이번 단계의 목적은 projection_outbox worker가 미래의 append-only 전환에서
`condition_event` market_data projection을 직접 적용했을 때, 후속
`condition_fusion` refresh를 같은 worker-side deferred side-effect로 안전하게 수행할
수 있는지 검증하는 것이다.

## 왜 아직 condition_event skip을 하지 않는가

`condition_event`는 단순 market_data artifact가 아니라 후보 생성 흐름의 센서 입력이다.
`condition_fusion`은 조건식 hit를 profile/role별로 합성하고, candidate ingest는 그
결과를 후보 source로 변환한다. PR-10 worker는 `condition_fusion` refresh까지만
준비하며 `candidate_service.ingest_condition_sources()`를 호출하지 않는다.

따라서 PR-10 운영 기준은 다음과 같다.

- `condition_event effective_skip_inline = 0`
- Gateway inline projection 유지
- Gateway inline `condition_fusion` refresh 유지
- worker가 직접 apply한 `condition_event`에서만 deferred `condition_fusion` refresh
- inline already-applied `condition_event`에서는 worker side-effect 중복 금지
- worker evidence의 `candidate_ingest_executed = false`
- 주문, LIVE_SIM, LIVE_REAL behavior 변경 없음

## 주요 지표

Operator status:

```powershell
Invoke-RestMethod `
  -Headers @{ "X-Local-Token" = $env:TRADING_CORE_TOKEN } `
  http://127.0.0.1:8000/api/operator/market-data-append-only-routing/status
```

확인할 필드:

- `condition_event_effective_skip_count = 0`
- `condition_event_worker_side_effect_ready = true`
- `condition_event_deferred_fusion_refresh_error_count = 0`
- `condition_event_candidate_ingest_executed_count = 0`
- `condition_event_side_effect_duplicate_count = 0`
- `invalid_effective_skip_count = 0`

Dashboard snapshot:

```powershell
Invoke-RestMethod `
  -Headers @{ "X-Local-Token" = $env:TRADING_CORE_TOKEN } `
  "http://127.0.0.1:8000/api/dashboard/snapshot?sections=market_data,projection_outbox,pipeline_summary,gateway,errors&detail=summary&limit=1"
```

`pipeline_summary.market_data_append_only_routing`에서 다음 상태를 확인한다.

- `condition_event_side_effect_migration_status = PREP_ONLY_INLINE_REQUIRED`
- `condition_event_candidate_ingest_status = NOT_IN_WORKER`
- PR-10 warning 포함

## Run-Once 검증

장중 또는 mock/live gateway 수신 후 worker와 reconcile을 수동으로 한 번 실행한다.

```powershell
python tools/ops_market_data_condition_event_side_effect_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --limit 100 `
  --run-once
```

보고서는 기본적으로 다음 경로에 생성된다.

```text
reports/market_data_condition_event_side_effect/<timestamp>/summary.md
reports/market_data_condition_event_side_effect/<timestamp>/raw.json
```

## PASS 기준

- `condition_event effective skip = 0`
- worker-applied `condition_event`의 outbox metadata에
  `post_apply_side_effects.condition_fusion_refresh_status` 기록
- `condition_fusion_error_count = 0`
- `candidate_ingest_executed = false`
- inline already-applied event에서 duplicate side-effect 없음
- projection_outbox `ERROR`/`DEAD_LETTER = 0`
- latest reconcile이 `PASS` 또는 운영상 허용 가능한 `WARN`

## WARN 기준

- 장중 `condition_event`가 없어 관측 불충분
- `condition_event_would_skip_inline_count = 0`
- `condition_event_deferred_fusion_refresh_count = 0`
- `condition_event_worker_side_effect_ready = false`
- `condition_fusion_event_incremental_enabled = false`
- projection_outbox pending backlog 존재
- latest reconcile missing/stale

## FAIL 기준

- `condition_event_effective_skip_count > 0`
- `condition_event_candidate_ingest_executed_count > 0`
- `condition_event_deferred_fusion_refresh_error_count > 0`
- inline 처리 event에 worker side-effect evidence 중복
- projection_outbox `ERROR` 또는 `DEAD_LETTER`
- market_data reconcile `FAIL`
- `invalid_effective_skip_count > 0`

## 중복 Side-Effect 확인 SQL

```sql
SELECT outbox_id, event_id, metadata_json
FROM projection_outbox
WHERE projection_name = 'market_data'
  AND event_type = 'condition_event'
  AND json_extract(metadata_json, '$.last_worker_evidence.apply_result') = 'APPLIED_BY_VERIFY'
  AND json_extract(
    metadata_json,
    '$.last_worker_evidence.post_apply_side_effects.condition_fusion_refresh_status'
  ) IS NOT NULL;
```

결과는 0건이어야 한다.

## Candidate Ingest 금지 확인 SQL

```sql
SELECT outbox_id, event_id, metadata_json
FROM projection_outbox
WHERE projection_name = 'market_data'
  AND event_type = 'condition_event'
  AND json_extract(
    metadata_json,
    '$.last_worker_evidence.post_apply_side_effects.candidate_ingest_executed'
  ) = 1;
```

결과는 0건이어야 한다.

## Rollback

PR-10은 기본값에서 production behavior를 바꾸지 않는다. 문제가 있으면 다음을 확인한다.

```powershell
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_DRY_RUN_ENABLED="false"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED="false"
$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED="false"
$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED="false"
```

Gateway inline projection은 유지되므로 condition_event 수집 자체를 중단할 필요는 없다.
주문/LIVE_SIM/LIVE_REAL 설정은 변경하지 않는다.

## PR-11 진입 조건

- 실제 장중 PR-10 ops check가 PASS 또는 원인 설명 가능한 WARN
- `condition_event_effective_skip_count = 0`
- worker-applied condition_event에서 deferred `condition_fusion` refresh evidence 확인
- duplicate side-effect 0
- worker candidate ingest 0
- projection_outbox ERROR/DEAD_LETTER 0
- latest market_data reconcile PASS
- price_tick PR-7, tr_response PR-9 회귀 테스트 통과
