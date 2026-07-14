# Theme DB / Leadership Coherency Runbook

## 범위

이 점검은 Dashboard의 DB top theme와 in-memory leadership 결과가 어떤 관측 source와
snapshot을 사용했는지 같은 형식으로 노출한다. DB schema와 theme 계산 정책은 변경하지
않으며 조회 과정에서 Candidate, Gateway command, 주문, 정정 또는 취소를 생성하지 않는다.

표준 provenance 필드는 다음과 같다.

- `source`
- `snapshot_id`
- `calculated_at`
- `data_age_sec`
- `watchset_selection_source`

DB top row의 source는 `THEME_LATEST_SNAPSHOT`이다. market-scan flow snapshot을 다시
분류한 leadership row는 `THEME_FLOW_SNAPSHOT`과 원본 DB `snapshot_id/calculated_at`을
그대로 보존한다. realtime universe에서 즉시 계산한 결과는
`REALTIME_UNIVERSE_REBUILD`와 계산 시점 기반의 in-memory snapshot ID를 사용한다.

## 판정 기준

`GET /api/operator/theme-coherency/status`는 두 source summary, 표준화한 top item,
latest pointer 대조 결과를 반환한다.

- `PASS`: leadership이 참조한 persisted flow snapshot이 현재 latest pointer와 일치하고
  source/top-set/staleness 경고가 없다.
- `WARN`: DB top과 realtime leadership source가 다르거나, 두 top set이 다르거나,
  tradable DB top 또는 leadership 결과가 비어 있거나, snapshot이 stale하다.
- `FAIL`: `THEME_FLOW_SNAPSHOT`이 가리키는 `snapshot_id` 또는 `calculated_at`이 현재
  `theme_latest_snapshots` pointer와 다르거나 pointer가 없다.

주요 집계는 다음과 같다.

- `db_top_count`, `leadership_top_count`, `overlap_count`
- `top_set_mismatch_count`, `source_mismatch_count`
- `snapshot_mismatch_count`, `missing_snapshot_count`, `stale_count`
- `db_top`, `leadership`, `db_top_items`, `leadership_items`

`THEME_LEADERSHIP_SOURCE_DIFFERS_FROM_DB_TOP`은 source 차이를 숨기지 않는 경고이며
persisted pointer 손상을 뜻하지 않는다. 반면 `THEME_FLOW_SNAPSHOT_POINTER_MISMATCH`와
`THEME_FLOW_SNAPSHOT_CALCULATED_AT_MISMATCH`는 다음 평가 단계로 넘어가기 전에 원인을
확인해야 하는 구조적 실패다.

## API / Dashboard

- `GET /api/operator/theme-coherency/status?limit=10`
- Operator aggregate status: `theme_coherency`
- Dashboard full: `theme_coherency`, `themes.coherency`
- Dashboard fast section: `theme_coherency`
- `theme_coherency`를 함께 요청한 fast `pipeline_summary.theme_coherency`
- Theme leadership API row의 동일 provenance 필드

성능이 중요한 기본 fast Dashboard는 leadership rebuild를 자동 실행하지 않는다.
`theme_coherency` section을 명시적으로 요청한 경우에만 해당 read-only 계산을 수행한다.

## OBSERVE-safe 점검

Core는 다음 process-local 조건으로 실행한다.

```text
TRADING_MODE=OBSERVE
TRADING_ALLOW_LIVE_SIM=false
TRADING_ALLOW_LIVE_REAL=false
```

점검 명령:

```powershell
python -m tools.ops_theme_coherency_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --limit 10
```

보고서는 `reports/theme_coherency/<UTC>/raw.json`과 `summary.md`에 생성된다. ops 도구는
direct endpoint, operator aggregate, fast Dashboard verdict/provenance 일치와 점검 전후
command/order-command delta 0을 함께 확인한다.

## SQL 점검

```sql
SELECT l.theme_id,
       l.snapshot_id,
       l.calculated_at,
       l.state,
       s.snapshot_id AS history_snapshot_id,
       s.calculated_at AS history_calculated_at
FROM theme_latest_snapshots AS l
LEFT JOIN theme_snapshots AS s
  ON s.snapshot_id = l.snapshot_id
WHERE s.snapshot_id IS NULL
   OR s.theme_id <> l.theme_id
   OR s.calculated_at <> l.calculated_at;

SELECT l.theme_id,
       l.snapshot_id,
       l.calculated_at,
       l.state,
       l.quality_status
FROM theme_latest_snapshots AS l
ORDER BY l.calculated_at DESC, l.theme_id ASC;
```

## 장애 대응

1. `FAIL` 응답의 leadership item, current latest pointer, reason code를 보존한다.
2. 해당 theme snapshot history를 삭제하거나 pointer를 수동 덮어쓰지 않는다.
3. 원본 market-data/market-scan projection과 theme snapshot 계산 시점을 확인한다.
4. 동일한 OBSERVE evaluation lock 경계에서 작은 theme rebuild를 실행한다.
5. direct/operator/Dashboard status와 command/order-command delta 0을 재확인한다.

realtime source 차이만 있는 `WARN`은 persisted corruption이 아니다. 운영 목적에 따라
`MARKET_SCAN_ENABLED` source 선택을 확인하되 warning을 없애기 위해 source metadata를
위조하거나 LIVE_SIM/LIVE_REAL gate를 완화하지 않는다.

## NXT 사용 제한

명시적 `exchange=NXT` 관측은 venue-neutral ordering, source 표시, replay 및 시간축 진단에
사용할 수 있다. 그러나 현재 theme leadership의 realtime 가격/bar freshness 계약은 KRX를
기준으로 하므로 NXT tick을 KRX theme snapshot, KRX 정규장 freshness 또는 KOA Studio
parser evidence의 대체 근거로 사용하지 않는다. KRX와 NXT source가 섞이면 각각의 venue를
보존하고 별도 warning/evidence로 다룬다.
