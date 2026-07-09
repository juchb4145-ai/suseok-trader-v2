# projection_outbox market_data apply pilot runbook

대상: `projection_outbox`의 `market_data` job만 제한적으로 재적용하는 PR-4 pilot.

## 안전 경계

- 이 runbook은 append-only 전환 절차가 아니다. Gateway POST inline projection은 계속 유지된다.
- background worker는 기본 disabled다.
- 주문, LIVE_SIM, LIVE_REAL, 매수 gate, safety gate는 이 절차에서 변경하지 않는다.
- `market_index`, `market_regime`, `market_scan`, `condition_fusion` job은 apply mode에서도 projection을 갱신하지 않고 `APPLY_NOT_ENABLED_FOR_PROJECTION`으로 skip된다.
- PR-13 이후 `market_reference`는 별도 `PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED=true`가 켜진 경우에만 `market_symbols` worker apply가 허용된다. market_data apply flag만으로는 market_reference를 갱신하지 않는다.

## 기본 설정

기본값:

```text
PROJECTION_OUTBOX_WORKER_ENABLED=false
PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false
PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=false
PROJECTION_OUTBOX_APPLY_BATCH_SIZE=50
PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC=1.0
```

`apply_projection=true`를 API에 넘겨도 아래 두 플래그가 모두 켜져 있지 않으면 projection side effect는 발생하지 않는다.

```text
PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=true
PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=true
```

## 사전 점검

```powershell
python -m pytest tests/test_projection_outbox_worker.py -q
python -m pytest tests/test_projection_outbox_market_data_apply_worker.py -q
```

운영 DB 상태 확인:

```sql
SELECT status, projection_name, COUNT(*) AS count
FROM projection_outbox
GROUP BY status, projection_name
ORDER BY status, projection_name;

SELECT *
FROM projection_outbox
WHERE status IN ('ERROR', 'DEAD_LETTER')
ORDER BY updated_at DESC
LIMIT 20;
```

## shadow verify run-once

기본 검증은 projection table을 갱신하지 않는다.

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/projection-outbox/run-once?limit=50" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

응답에서 확인할 값:

```text
shadow_mode=true
apply_projection=false
apply_projection_effective=false
projection_side_effects_allowed=false
read_only_projection=true
no_trading_side_effects=true
```

## market_data apply pilot run-once

명시적으로 두 apply 플래그를 켠 프로세스에서만 실행한다.

```powershell
$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED="true"
$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED="true"
$env:PROJECTION_OUTBOX_APPLY_BATCH_SIZE="50"
$env:PROJECTION_OUTBOX_APPLY_MIN_AGE_SEC="1.0"
```

run-once:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/projection-outbox/run-once?limit=50&apply_projection=true" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

응답에서 확인할 값:

```text
apply_projection_requested=true
apply_projection_effective=true
projection_side_effects_allowed=true
no_trading_side_effects=true
mutated_projection_names=["market_data"] 또는 []
projection_apply_error_count=0
```

`mutated_projection_names=[]`이면 inline projection에서 이미 반영된 job만 verify로 닫힌 것이다.

## 사후 검증

```sql
SELECT status, projection_name, COUNT(*) AS count
FROM projection_outbox
GROUP BY status, projection_name
ORDER BY status, projection_name;

SELECT COUNT(*) AS error_count
FROM projection_outbox
WHERE status IN ('ERROR', 'DEAD_LETTER');

SELECT event_id, last_error, metadata_json
FROM projection_outbox
WHERE status IN ('ERROR', 'DEAD_LETTER')
ORDER BY updated_at DESC
LIMIT 20;
```

`market_tick_samples` 누락 보정 여부:

```sql
SELECT o.event_id, o.status
FROM projection_outbox o
LEFT JOIN market_tick_samples s ON s.event_id = o.event_id
WHERE o.projection_name = 'market_data'
  AND o.event_type = 'price_tick'
  AND o.status = 'APPLIED'
  AND s.event_id IS NULL
LIMIT 20;
```

## rollback

즉시 비활성화:

```powershell
$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED="false"
$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED="false"
```

이미 처리된 outbox job은 상태 evidence를 남긴다. 오류 job 재시도는 원인 확인 후 `status`, `attempts`, `last_error`, `available_at`을 운영자가 명시적으로 재설정하는 방식으로만 진행한다.
