# Market Data TR Response Side-Effect Migration Runbook

목적: PR-8은 `tr_response` append-only cutover가 아니라, candidate quote refresh incremental enqueue를 worker side-effect로 옮길 수 있는지 검증하는 준비 단계다.

## 안전 원칙

- `tr_response` inline `process_gateway_event()`는 계속 실행한다.
- `condition_event` inline projection과 condition_fusion refresh는 계속 실행한다.
- `tr_response effective_skip_inline`은 PR-8에서 반드시 `0`이어야 한다.
- `condition_event effective_skip_inline`도 반드시 `0`이어야 한다.
- `LIVE_REAL`, 주문 정책, 매수 gate, safety gate는 변경하지 않는다.
- worker/apply 관련 flag 기본값은 disabled다.

## 권장 설정

실제 장 검증은 Core/Gateway 안전 env에서만 수행한다.

```powershell
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_DRY_RUN_ENABLED="true"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED="false"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_REQUIRE_WORKER_SIDE_EFFECTS="true"
$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED="true"
$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED="true"
```

주의: 위 설정은 worker run-once 검증 준비용이다. Gateway request path의 `tr_response` inline projection은 꺼지지 않는다.

## 검증 명령

```powershell
python -m tools.ops_market_data_tr_response_side_effect_check --core-url http://127.0.0.1:8000 --limit 100 --timeout-sec 90 --run-once
```

PASS 조건:

- `tr_response_effective_skip_count = 0`
- `condition_event_effective_skip_count = 0`
- `invalid_effective_skip_count = 0`
- `tr_response_deferred_side_effect_error_count = 0`
- `projection_outbox.error_count = 0`
- `projection_outbox.dead_letter_count = 0`

WARN 조건:

- 검증 구간에 `tr_response`가 없다.
- worker side-effect readiness가 false다.
- reconcile latest run이 없거나 stale/not ready다.
- pending outbox가 남아 worker run-once가 필요하다.

## SQL Spot Check

```sql
SELECT event_type, effective_skip_inline, COUNT(*) AS count
FROM market_data_projection_routing_decisions
WHERE event_type IN ('tr_response', 'condition_event')
GROUP BY event_type, effective_skip_inline;

SELECT event_id, status, json_extract(metadata_json, '$.last_worker_evidence.post_apply_side_effects.candidate_quote_refresh_enqueue_status') AS quote_refresh_status
FROM projection_outbox
WHERE projection_name = 'market_data'
  AND event_type = 'tr_response'
ORDER BY updated_at DESC
LIMIT 20;
```

## 후속 PR

PR-9에서만 `tr_response` effective skip 조건을 별도 feature flag와 duplicate side-effect guard 뒤에 검토한다.
