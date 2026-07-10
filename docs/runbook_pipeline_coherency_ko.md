# Pipeline Source Lineage / Coherency Runbook

## 범위

schema 59는 Candidate -> Strategy -> Risk -> EntryTiming -> OrderPlan 관측 경로에
동일 source snapshot을 확인할 수 있는 lineage를 저장한다. 이 기능은 OBSERVE 평가와
read-only 진단만 확장하며 주문, 정정, 취소 또는 Gateway command를 생성하지 않는다.

표준 필드는 다음과 같다.

- `source_run_id`
- `source_watermark`, `source_watermark_hash`
- `source_event_id`, `source_observed_at`
- `trade_date`, `data_age_sec`, `generated_by`
- EntryTiming/OrderPlan의 `strategy_observation_id`, `risk_observation_id`
- OrderPlan의 `entry_timing_evaluation_id`

기존 row는 삭제하거나 추정 backfill하지 않는다. migration 이전 row의 nullable lineage는
`*_LINEAGE_MISSING`으로 표시하며 새 `PLAN_READY` 생성과 LIVE_SIM eligibility에서 fail-closed한다.

## 생성 규칙

Strategy는 candidate state/context, KRX latest tick과 1m/3m/5m bar,
common market-context reference, current theme/condition reference를 canonical JSON
watermark로 만들고 SHA-256 hash를 저장한다.
Risk는 자신이 참조하는 Strategy observation의 lineage를 상속한다.

EntryTiming은 다음 조건을 모두 만족해야 `PLAN_READY`를 허용한다.

1. Strategy/Risk latest row가 모두 존재한다.
2. Risk의 `strategy_observation_id`가 현재 Strategy latest ID와 같다.
3. 두 row의 `source_run_id`, `source_watermark_hash`, `trade_date`가 같다.
4. source age가 `ENTRY_TIMING_STALE_MAX_SECONDS` 이내다.
5. observe-cycle이 expected source run을 제공한 경우 그 run ID와도 같다.
6. EntryTiming 직전 current candidate source watermark가 Strategy watermark와 같다.

불일치하면 evaluation을 `DATA_WAIT`로 기록하고
`PIPELINE_COHERENCY_GUARD_BLOCKED`를 남긴다. near-miss draft가 남더라도 수량은 0이고
`PLAN_READY`가 아니므로 LIVE_SIM selection 대상이 아니다.

LIVE_SIM order-plan eligibility는 persisted plan을 현재 Strategy/Risk/EntryTiming latest와
다시 비교한다. 직접 FK, source run 또는 watermark가 없거나 달라지면
`ORDER_PLAN_LINEAGE_INVALID`로 차단한다.

## 상태 기준

`GET /api/operator/pipeline-coherency/status`는 candidate별 stage metadata와 집계를 반환한다.
기본 범위는 candidates와 네 stage에 존재하는 가장 최신 `trade_date`이며, 과거 날짜는
query parameter로 명시해 별도로 조회한다.

- `PASS`: 존재하는 5단계 row의 run/watermark/FK/trade date가 일치하고 source가 fresh다.
- `WARN`: pipeline row가 없거나 downstream EntryTiming/OrderPlan이 아직 없거나, non-ready
  관측 source가 stale하다.
- `FAIL`: FK/run/watermark/trade-date mismatch, lineage 누락 또는 stale `PLAN_READY`가 있다.

주요 집계:

- `candidate_count`, `coherent_count`, `warning_count`
- `mismatch_count`, `missing_lineage_count`, `stale_count`
- candidate별 `stages.strategy/risk/entry_timing/order_plan`

각 stage metadata에는 `source_run_id`, parsed `source_watermark`, `trade_date`,
`data_age_sec`, `generated_by`가 같은 형식으로 노출된다.

## API / Dashboard

- `GET /api/operator/pipeline-coherency/status?trade_date=YYYY-MM-DD&limit=100`
- Dashboard fast section: `pipeline_coherency`
- Dashboard `pipeline_summary.coherency`
- Operator aggregate status: `pipeline_coherency`

조회 API는 DB를 수정하지 않는다.

## OBSERVE-safe 점검

Core는 다음 process-local 조건으로 실행한다.

```text
TRADING_MODE=OBSERVE
TRADING_ALLOW_LIVE_SIM=false
TRADING_ALLOW_LIVE_REAL=false
```

점검 명령:

```powershell
python -m tools.ops_pipeline_coherency_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN
```

보고서는 `reports/pipeline_coherency/<UTC>/raw.json`과 `summary.md`에 생성된다.
Core mode, LIVE_SIM/LIVE_REAL 허용 여부, operator/Dashboard 일치, command 및
order-command delta 0을 함께 검사한다.

## SQL 점검

```sql
SELECT candidate_instance_id,
       strategy_observation_id,
       source_run_id,
       source_watermark_hash,
       source_observed_at,
       data_age_sec,
       generated_by
FROM strategy_observations_latest
ORDER BY evaluated_at DESC;

SELECT r.candidate_instance_id,
       s.strategy_observation_id AS strategy_latest_id,
       r.strategy_observation_id AS risk_strategy_id,
       s.source_run_id AS strategy_run_id,
       r.source_run_id AS risk_run_id,
       s.source_watermark_hash AS strategy_watermark,
       r.source_watermark_hash AS risk_watermark
FROM strategy_observations_latest AS s
JOIN risk_observations_latest AS r
  ON r.candidate_instance_id = s.candidate_instance_id
WHERE r.strategy_observation_id <> s.strategy_observation_id
   OR r.source_run_id <> s.source_run_id
   OR r.source_watermark_hash <> s.source_watermark_hash;

SELECT order_plan_id,
       status,
       entry_timing_evaluation_id,
       strategy_observation_id,
       risk_observation_id,
       source_run_id,
       source_watermark_hash,
       source_observed_at,
       generated_by
FROM order_plan_drafts_latest
ORDER BY created_at DESC;
```

## 장애 대응 / Rollback

1. `FAIL` candidate의 stage IDs, run ID, watermark, source event와 timestamp를 보존한다.
2. 해당 candidate의 새 EntryTiming/LIVE_SIM 진입을 계속 차단한다.
3. candidate context와 Strategy/Risk를 같은 evaluation lock 경계에서 다시 평가한다.
4. 작은 EntryTiming batch를 실행한 뒤 operator/Dashboard status를 재확인한다.
5. command/order-command delta가 0인지 확인한다.

lineage 컬럼을 수동 덮어쓰거나 legacy row를 임의 backfill하지 않는다. 긴급 rollback은
EntryTiming/order-plan loop를 중지하고 기존 row와 진단 evidence를 보존하는 방식으로 한다.
LIVE_REAL 또는 주문 gate를 완화해 mismatch를 우회하지 않는다.
