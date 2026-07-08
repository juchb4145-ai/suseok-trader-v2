# market_data projection reconcile runbook

대상: PR-5 `market_data` dual-run reconciliation.

## 목적

Gateway append-only 전환 전에 accepted `price_tick`, `condition_event`, `tr_response`가 기존 inline projection과 `projection_outbox`에서 일관되게 관측되는지 확인한다.

이 절차는 append-only 전환이 아니다. Gateway inline projection은 계속 켜져 있고, reconcile은 projection을 고치지 않는다. 결과는 `market_data_projection_reconcile_runs`와 `market_data_projection_reconcile_issues`에만 기록한다.

## 안전 경계

- `api/routes/gateway.py::post_gateway_event()` inline projection을 제거하지 않는다.
- `PROJECTION_OUTBOX_WORKER_ENABLED=false` 기본값을 유지한다.
- `PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=false` 기본값을 유지한다.
- `PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=false` 기본값을 유지한다.
- LIVE_REAL, 주문 정책, safety gate, 매수 gate를 변경하지 않는다.
- market_index, market_regime, market_reference, condition_fusion, market_scan apply를 실행하지 않는다.

## 실행 시점

- 장중 Core/Gateway가 OBSERVE 모드로 정상 수집 중일 때.
- PR-4 market_data apply pilot 검증 후.
- append-only dry-run flag를 추가하기 전.

## API 실행

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/market-data-projection-reconcile/run-once?limit=500" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

latest 조회:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/market-data-projection-reconcile/latest"
```

rowid 범위 지정:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8000/api/operator/market-data-projection-reconcile/run-once?min_event_rowid=1000&max_event_rowid=2000&limit=1000" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

## 운영 스크립트

```powershell
python -m tools.ops_market_data_projection_reconcile `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-once `
  --limit 500 `
  --out-dir reports/market_data_projection_reconcile
```

출력:

- `raw.json`
- `latest.json`
- `summary.md`

## PASS/WARN/FAIL 해석

PASS:

- checked event가 1건 이상이다.
- missing projection이 0건이다.
- outbox ERROR/DEAD_LETTER가 0건이다.
- watermark risk가 0건이다.
- synthetic child event id issue가 0건이다.
- 대상 market_data event의 outbox job이 APPLIED 또는 정당한 SKIPPED다.

WARN:

- pending backlog가 있지만 치명적 오류는 아니다.
- inline projection error가 error table에 기록되어 있다.
- 빈 TR response 또는 quote-only tick이 허용 가능한 skip으로 관측된다.

FAIL:

- accepted event에 artifact도 projection error도 없다.
- `projection_watermarks.market_data.last_event_rowid`가 누락 event를 지나갔다.
- market_data outbox job이 없다.
- market_data outbox가 ERROR/DEAD_LETTER다.
- synthetic price_tick이 parent event_id를 재사용하거나 metadata가 누락됐다.
- worker evidence가 market_data 외 projection mutation을 나타낸다.

`append_only_ready=True`는 PASS일 때만 의미가 있다. True가 되어도 자동 전환하지 않는다.

## 대응 SQL

최근 reconcile run:

```sql
SELECT *
FROM market_data_projection_reconcile_runs
ORDER BY created_at DESC
LIMIT 5;
```

상위 issue:

```sql
SELECT severity, reason_code, COUNT(*) AS count
FROM market_data_projection_reconcile_issues
WHERE run_id = :run_id
GROUP BY severity, reason_code
ORDER BY severity, count DESC;
```

accepted price_tick 누락:

```sql
SELECT ge.rowid, ge.event_id, ge.event_ts, ge.received_at, ge.payload_json
FROM gateway_events ge
LEFT JOIN market_tick_samples s ON s.event_id = ge.event_id
LEFT JOIN market_projection_errors e ON e.event_id = ge.event_id
WHERE ge.status = 'ACCEPTED'
  AND ge.event_type = 'price_tick'
  AND s.event_id IS NULL
  AND e.event_id IS NULL
ORDER BY ge.rowid DESC
LIMIT 50;
```

watermark risk:

```sql
SELECT ge.rowid, ge.event_id, pw.last_event_rowid
FROM gateway_events ge
JOIN projection_watermarks pw ON pw.projection_name = 'market_data'
LEFT JOIN market_tick_samples s ON s.event_id = ge.event_id
LEFT JOIN market_condition_signals c ON c.event_id = ge.event_id
LEFT JOIN market_tr_snapshots tr ON tr.event_id = ge.event_id
LEFT JOIN market_projection_errors e ON e.event_id = ge.event_id
WHERE ge.rowid <= pw.last_event_rowid
  AND ge.status = 'ACCEPTED'
  AND ge.event_type IN ('price_tick', 'condition_event', 'tr_response')
  AND s.event_id IS NULL
  AND c.event_id IS NULL
  AND tr.event_id IS NULL
  AND e.event_id IS NULL
ORDER BY ge.rowid DESC
LIMIT 50;
```

synthetic child event regression:

```sql
SELECT event_id, metadata_json
FROM market_tick_samples
WHERE json_extract(metadata_json, '$.synthetic_event') = 1
  AND event_id = json_extract(metadata_json, '$.parent_event_id')
LIMIT 50;
```

## 후속 PR 조건

- 장중 reconcile PASS를 여러 차례 확인한다.
- pending outbox backlog가 운영 허용 범위 안에 있거나 worker 처리 정책이 확정된다.
- synthetic child event regression이 0건임을 확인한다.
- 그 다음 PR에서만 market_data append-only dry-run flag를 추가한다.
