# 공통 Market Context 운영 Runbook

## 범위

PR-17은 KOSPI/KOSDAQ 지수 상태를 후보마다 다시 계산하지 않고, 동일 index watermark를 공유하는 공통 `market_context` snapshot으로 만든다. 이 단계는 `market_regime` 자체의 append-only cutover가 아니다. 주문, 정정, 취소, SOR, LIVE_SIM/LIVE_REAL 경로를 활성화하지 않는다.

## 데이터 계약

- schema 51은 `market_context_snapshots`와 market별 latest pointer인 `market_context_latest`를 추가한다.
- 한 watermark에서 KOSPI와 KOSDAQ snapshot이 같은 `source_watermark_hash`를 가져야 한다.
- watermark identity는 각 지수의 immutable event id/rowid/timestamp, parser status, data source로 계산한다. 동적으로 변하는 tick age나 freshness는 hash에 넣지 않는다.
- `parser_confidence_status`와 `data_quality_status`를 별도 컬럼과 evidence로 유지한다.
- `candidate_context_latest.market_context_snapshot_id`는 공통 snapshot을 참조한다. 후보 refresh는 market context나 market regime snapshot을 만들지 않는다.
- membership 미확인, snapshot 누락 또는 stale 상태는 `DATA_WAIT`, `trading_eligible=false`로 fail-closed한다.

## 자동 생성 경로

- Gateway inline market-index projection이 유지되는 동안 index apply 직후 공통 builder를 호출한다.
- 정상 pair의 새 watermark는 기본 5초 cadence로 제한한다. pair 누락, data usability 개선, parser confidence 개선은 cadence를 기다리지 않고 즉시 rebuild한다.
- PR-16 market-index effective skip에서는 projection outbox worker가 index apply, event-linked global regime, 공통 context pair를 함께 보장한다.
- 같은 watermark를 다시 처리하면 새 snapshot을 만들지 않고 latest pointer만 검증한다.
- worker refresh 예외, event-linked regime 누락은 retryable error이며 기존 inline rollback 정책을 유지한다.

## OBSERVE-safe 수동 점검

Core는 별도 safe env 또는 process-local 환경변수로 실행한다. 운영 `.env`를 수정하지 않는다.

```powershell
$env:TRADING_MODE = "OBSERVE"
$env:TRADING_PROFILE = "OBSERVE"
$env:TRADING_ALLOW_LIVE_SIM = "false"
$env:TRADING_ALLOW_LIVE_REAL = "false"
```

상태 조회는 read-only다.

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/operator/market-context/status
Invoke-RestMethod "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=market_context"
```

수동 rebuild는 local token과 OBSERVE-safe runtime을 모두 요구한다. `live_safe=false`, OBSERVE 이외 mode/profile, LIVE_SIM/LIVE_REAL 허용 상태에서는 HTTP 409로 거부된다.

```powershell
$headers = @{ "X-Local-Token" = $env:TRADING_CORE_TOKEN }
Invoke-RestMethod -Method Post `
  -Headers $headers `
  "http://127.0.0.1:8000/api/operator/market-context/rebuild?live_safe=true"
```

종합 evidence는 다음 도구로 생성한다.

```powershell
.\venv_64\Scripts\python.exe tools\ops_market_context_check.py `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --run-rebuild
```

결과는 `reports/market_context/<UTC timestamp>/raw.json`과 `summary.md`에 기록된다.

## 판정 기준

PASS:

- Core mode/profile이 OBSERVE이고 LIVE_SIM/LIVE_REAL 허용이 false다.
- KOSPI/KOSDAQ latest pair가 존재하고 같은 watermark hash를 공유한다.
- KOSPI/KOSDAQ latest pair가 같은 source regime snapshot을 공유한다.
- 두 snapshot이 fresh, parser VERIFIED, trading data usable이다.
- candidate의 non-null snapshot reference가 모두 유효하다.
- candidate context에 snapshot 미참조 row가 없다.
- projection outbox `ERROR/DEAD_LETTER=0/0`이다.
- 점검 전후 Gateway command와 order command 증가가 없다.

WARN:

- 지수 pair 누락, stale, parser unverified, data unusable 중 하나가 있다.
- 이 상태에서는 후보 context가 `DATA_WAIT`로 유지되며 후속 regime cutover evidence로 사용하지 않는다.

FAIL:

- watermark가 KOSPI/KOSDAQ 사이에서 불일치한다.
- candidate snapshot reference가 유실됐다.
- outbox ERROR/DEAD_LETTER가 발생했다.
- OBSERVE-safe 조건이 깨졌거나 command delta가 발생했다.

## SQL 확인

```sql
SELECT snapshot.market, snapshot.snapshot_id, snapshot.source_watermark_hash,
       parser_confidence_status, data_quality_status,
       trading_data_usable, snapshot_at
FROM market_context_latest AS latest
JOIN market_context_snapshots AS snapshot
  ON snapshot.snapshot_id = latest.snapshot_id
ORDER BY snapshot.market;

SELECT market_context_snapshot_id, COUNT(*) AS candidate_count
FROM candidate_context_latest
GROUP BY market_context_snapshot_id;

SELECT target_code, COUNT(*) AS snapshot_count
FROM market_regime_snapshots
GROUP BY target_code;
```

## KRX/NXT 경계

공통 context의 지수 근거는 KRX KOSPI/KOSDAQ index event다. NXT price tick은 market-data, worker, replay, dashboard의 venue-neutral 검증에는 사용할 수 있지만 KRX market-index 또는 market-regime PASS를 대체하지 않는다. NXT-only 시간에는 `PENDING_KRX_SESSION`을 유지하며 synthetic 장중 PASS를 만들지 않는다.

## 다음 단계

PR-17 PASS 후에도 Gateway의 일반 market-regime 경로가 완전히 append-only가 된 것은 아니다. 다음 PR에서 regime projection outbox worker, reconcile, dry-run routing을 준비하고, 별도 PR에서 제한 cutover를 진행한다.
