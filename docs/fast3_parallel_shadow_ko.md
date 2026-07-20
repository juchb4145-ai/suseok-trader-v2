# FAST-3 Parallel Shadow Execution 계약

## 목적

현재 거래일의 모든 coherent `PLAN_READY`를 FAST-2B와 동일한 보수적
execution/exit/cost policy로 shadow 처리하고, 선택된 LIVE_SIM canary 최대 한 건의 실제
증거와 비교한다. FAST-3 도구는 JSON 파일만 읽고 JSON/Markdown 보고서만 생성한다.
운영 DB, GatewayCommand, LIVE_SIM table, 브로커 API에는 접근하지 않는다.

## 입력 snapshot

입력 format은 `parallel-shadow-input/v1`이다. 신뢰 가능한 snapshot producer가 같은 시점의
plan universe, 시장 tick, LIVE_SIM evidence와 preflight 상태를 한 파일로 고정해야 한다.

- `plan_coverage_complete=true`
- `source_plan_count`와 실제 plan 수 일치
- 정렬된 전체 order plan ID의 canonical `source_plan_ids_sha256` 일치
- tick sequence/event ID 중복 0, availability 단조 증가
- 모든 tick의 `event_at <= available_at <= generated_at`
- `live_real_allowed=false`
- AI는 advisory-only이며 plan selection/sizing/routing/exit 영향 0

각 plan은 다음 lineage가 모두 있어야 한다.

- order plan ID
- entry timing evaluation ID
- strategy observation ID
- risk observation ID
- source run ID
- canonical source watermark와 일치하는 SHA-256

plan의 `trade_date`는 `created_at`의 Asia/Seoul 날짜와 같아야 한다. 같은 order plan ID가
두 번 나오면 shadow 결과를 만들지 않고 입력을 거부한다.

## 실행 모델

`status=PLAN_READY`이고 `coherent=true`인 모든 plan을 shadow 대상으로 삼는다. FAST-2B의
`simulate_conservative_execution` 순수 함수를 직접 호출하므로 다음 규칙을 별도로 복제하지
않는다.

- signal/trigger와 같은 tick 체결 금지
- configured entry/exit latency와 TTL
- BUY/SELL LIMIT 가격 제약
- STOP SELL 이후 첫 유효 tick과 gap 보존
- 공유 stop/take/trailing/min/max hold/EOD close-only 정책
- KRX tick-size, commission, sell tax, slippage 비용

각 대상에는 deterministic `shadow_execution_id`를 부여한다. 진입이 체결되면
`shadow_fill_id`와 `shadow_position_id`도 생성하며 원 plan lineage를 결과에 그대로 연결한다.
이는 보고서 식별자일 뿐 운영 table row나 주문 객체가 아니다.

## preflight와 kill switch

preflight가 `BLOCK`, kill switch active 또는 `live_buy_allowed=false`여도 shadow 계산은
중단하지 않는다. 같은 snapshot에 LIVE_SIM BUY 증거가 하나라도 있으면
`LIVE_BUY_PRESENT_WHEN_BLOCKED`로 `BLOCKED`다. shadow 결과 수가 coherent PLAN_READY 수보다
적어도 `SHADOW_PLAN_COVERAGE_GAP`으로 차단한다.

## LIVE_SIM 비교

LIVE_SIM observation은 서로 다른 plan 기준 최대 한 건만 허용한다. account ID, broker
account number, token은 입력하지 않는다. 비교에는 다음 식별자와 수치를 사용한다.

- live-sim intent/order/execution/position ID
- requested/filled quantity와 partial fill
- 평균 진입 체결가와 최초 체결시각
- exit reason, holding time, gross/net PnL

보고서는 다음을 계산한다.

- fill/no-fill disagreement
- shadow 대비 LIVE_SIM fill time delta
- signed KRX slippage ticks와 pct
- partial fill
- exit reason disagreement
- holding time, gross/net PnL delta

LIVE_SIM 증거가 가리키는 plan의 shadow 결과가 없거나, 체결 증거에 execution/position
lineage가 빠지거나, requested quantity가 plan과 다르면 `COMPARISON_LINKAGE_GAP`으로
차단한다. fill 또는 exit disagreement는 숨기지 않고 WARN으로 기록하며 자동 threshold
완화나 결과 보정은 하지 않는다.

## 판정

다음은 `BLOCKED` 조건이다.

- plan coverage 또는 shadow coverage 불완전
- 동일 plan shadow 중복
- LIVE_SIM canary 2건 이상
- preflight/kill-switch BLOCK 중 LIVE_SIM BUY 존재
- AI가 execution 판단에 영향
- comparison linkage gap
- 미확정 또는 0인 비용 모델

LIVE_SIM comparison이 아직 없으면 `NO_LIVE_CANARY_COMPARISON` WARN이다. 이는 shadow 구조
검증 결과일 뿐 fill 괴리 PASS 증거가 아니다. 비용 설정은 FAST-2B와 같은
`ProfitLabConfig`를 사용하며 검증된 version과 모든 비용 항목이 있어야 한다.

## 실행

```powershell
python -B -m tools.ops_parallel_shadow `
  --input-file <parallel-shadow-input.json> `
  --config-file <verified-profit-lab-config.json> `
  --out-dir reports/parallel_shadow
```

출력은 실행별 `raw.json`과 `summary.md`다. 기본 비용 설정은 `UNCONFIRMED`이므로 별도
검증 config가 없으면 `COST_MODEL_MISSING`으로 차단된다. 도구에는 DB path, API URL,
account, order 실행 인수가 없고 주문·브로커 호출 코드도 없다.
