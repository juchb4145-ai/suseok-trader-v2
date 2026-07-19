# FAST-1 Pure LIVE_SIM Preview

## 목적

`POST /api/live-sim/pilot/preview`는 현재 KRX 거래일의 기존 `PLAN_READY`를 LIVE_SIM
order-plan eligibility, pipeline lineage, safety gate, reconcile, broker-boundary, duplicate intent,
active order/position 상태와 함께 계산한다. 기존 Strategy/Risk/EntryTiming을 다시 실행하지 않는다.

## FAST-0 전환 경계

- 과거 FAST-0 pipeline blocker는 `DEFERRED_HISTORICAL`로 보존한다.
- 과거 row를 해결·삭제·수정하거나 `PASS`로 바꾸지 않는다.
- FAST-1 예외는 current-trade-date-only 무기록 계산에만 적용한다.
- Operational Gate C1, LIVE_SIM/LIVE_REAL 활성화, 주문 또는 broker 호출은 승인하지 않는다.

## 호출

```text
POST /api/live-sim/pilot/preview?limit=5
X-Local-Token: <local operator token>
```

`trade_date`를 생략하면 현재 KRX 거래일을 사용한다. 다른 날짜를 지정하면 HTTP 422와
`FAST1_CURRENT_TRADE_DATE_ONLY`를 반환한다. `limit`은 1~20이며 기본값은 5다.

## 읽기 전용 계약

- SQLite URI `mode=ro`
- 안정된 WAL/SHM pair 또는 sidecar 없는 main만 허용
- `PRAGMA query_only=ON`
- 단일 `BEGIN DEFERRED` snapshot
- 기존 `evaluate_live_sim_order_plan_eligibility()` 재사용
- selection 순서: `priority_score DESC`, `created_at DESC`, `order_plan_id ASC`
- AI advisory는 import하거나 selection input으로 사용하지 않음
- raw account ID는 응답에 포함하지 않음

다음 테이블과 모든 `dry_run_*` 테이블의 전후 count를 응답의 `side_effect_guard`에서 비교한다.

```text
entry_timing_evaluations
order_plan_drafts
order_plan_drafts_latest
live_sim_intents
live_sim_orders
live_sim_runs
live_sim_operating_runs
live_sim_rejections
gateway_commands
gateway_command_events
gateway_command_dedupe_keys
dry_run_*
```

하나라도 delta가 0이 아니면 응답 성공으로 처리하지 않는다. read-only/query-only 경계 때문에 이
endpoint 내부 DML은 SQLite 수준에서도 거부된다.

## 선택과 판정

Preview는 조회한 plan을 deterministic 순서로 반환한다. 기존 eligibility가 `eligible=true`인 plan만
`selectable=true`이며 첫 selectable plan을 `top_candidate`로 정한다. Lineage FAIL, duplicate intent,
reconcile mismatch, effective broker-boundary blocker 또는 기존 safety reason이 있는 plan은 선택하지
않는다.

`canary_ready=true`는 현재 입력에 대해 선택 가능한 Preview plan이 있다는 뜻일 뿐이다. 다음을
승인하지 않는다.

- `LiveSimIntent` 또는 rejection 저장
- pilot/operating run 저장
- `GatewayCommand` 생성
- broker 호출
- Operational Gate C1
- LIVE_SIM/LIVE_REAL 활성화

## 검증

```powershell
python -B -m pytest -q tests/test_fast1_pure_preview.py
python -B -m pytest -q tests/test_live_sim_order_plan_pipeline.py tests/test_live_sim_api_dashboard.py
```

테스트는 current-date 제한, local token, deterministic top candidate, 기존 eligibility 일치, duplicate
intent 차단, query-only DML 거부, 금지 테이블 delta 0과 raw account ID 비노출을 검증한다.
