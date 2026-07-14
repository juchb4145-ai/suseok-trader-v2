# LIVE_SIM Order-Plan Uniqueness Runbook

## 목적

`live_sim_intents.order_plan_id`는 하나의 EntryTiming order plan이 둘 이상의 LIVE_SIM intent로 변환되지 않도록 DB에서 일대일 관계를 보장한다. 이 기능은 LIVE_SIM이나 주문 전송을 활성화하지 않는다.

## Schema 계약

- `order_plan_id`는 nullable이다. candidate 기반 일반 intent에는 `NULL`을 허용한다.
- `uq_live_sim_intents_order_plan_id`는 `order_plan_id IS NOT NULL`인 row만 대상으로 하는 partial UNIQUE index다.
- order-plan intent 생성 시 `order_plan_id` 컬럼과 `evidence_json.order_plan_id`를 함께 기록한다.
- 조회는 `WHERE order_plan_id = ?` 직접 index lookup만 사용한다. 최근 N건 JSON scan은 사용하지 않는다.

## Startup migration

Core DB 초기화는 migration savepoint 안에서 다음 순서로 실행한다.

1. nullable `order_plan_id` 컬럼을 추가한다.
2. 기존 `evidence_json.order_plan_id`를 읽어 backfill 후보를 만든다.
3. duplicate, 컬럼/JSON mismatch, 유효하지 않은 order-plan evidence를 검사한다.
4. 문제가 없을 때만 backfill과 partial UNIQUE index 생성을 완료한다.

duplicate나 mismatch가 있으면 migration 전체를 rollback하고 Core startup을 중단한다. 운영 DB에서 임의로 row를 삭제하거나 index를 강제 생성하지 않는다.

## API 확인

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/operator/live-sim/order-plan-uniqueness/status" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

정상 기준:

- `status=PASS`
- `column_exists=true`
- `unique_index_exists/unique/partial=true`
- `duplicate_group_count=0`
- `mismatch_count=0`
- `missing_backfill_count=0`
- `lookup_strategy=DIRECT_ORDER_PLAN_ID_INDEX_LOOKUP`

Dashboard fast path:

```powershell
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/dashboard/snapshot?fast=true&sections=live_sim_order_plan_uniqueness,pipeline_summary" `
  -Headers @{"X-Local-Token"=$env:TRADING_CORE_TOKEN}
```

## Ops report

```powershell
python -m tools.ops_live_sim_order_plan_uniqueness_check `
  --core-url http://127.0.0.1:8000 `
  --token $env:TRADING_CORE_TOKEN `
  --out-dir reports/live_sim_order_plan_uniqueness
```

이 점검은 읽기 전용이다. intent 생성, command enqueue, LIVE_SIM/LIVE_REAL 전환을 수행하지 않는다.

## 장애 처리

- `DUPLICATE_ORDER_PLAN_ID`: Core를 OBSERVE-safe로 유지하고 duplicate intent의 lifecycle과 idempotency evidence를 조사한다.
- `ORDER_PLAN_ID_EVIDENCE_MISMATCH`: 컬럼과 JSON 중 어느 값이 원본 order plan을 가리키는지 EntryTiming lineage로 확인한다.
- `ORDER_PLAN_ID_BACKFILL_MISSING`: schema migration이 완료되지 않은 상태이므로 신규 intent 경로를 사용하지 않는다.
- `ORDER_PLAN_UNIQUE_INDEX_CONTRACT_INVALID`: index를 수동 교체하지 말고 복제 DB에서 migration 재현 후 수정 PR로 복구한다.

운영 DB row 삭제나 destructive migration은 금지한다. 복구 검증은 복제한 격리 DB에서 먼저 수행한다.
