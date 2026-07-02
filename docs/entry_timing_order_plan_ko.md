# EntryTimingEngine & OrderPlanDraft

## 범위

PR-3는 주문 PR이 아니다. EntryTiming 계층은 PR-2 ThemeLeadership watchset 또는 candidate source에서 올라온 후보가 실제 진입 가능한 가격 위치인지 관측하고, PR-4 LIVE_SIM safety-gated path가 다시 검토할 수 있는 `OrderPlanDraft` 초안을 만든다.

`OrderPlanDraft`는 OrderIntent가 아니며, broker 주문이나 gateway command가 아니다. `PLAN_READY`도 주문 승인이나 매수 신호가 아니다. 수동 또는 자동 LIVE_SIM 주문 연결은 PR-4 범위다.

## 흐름

```text
ThemeLeadership watchset
  -> candidate_context_latest / strategy_observations_latest / risk_observations_latest
  -> EntryTimingInput
  -> PriceLocationClassifier
  -> EntryTimingEngine
  -> OrderPlanDraftBuilder
  -> entry_timing_evaluations / order_plan_drafts_latest
```

ThemeLeadership의 `LEADING`, `SPREADING`, `LEADER`, `CO_LEADER`는 매수 신호가 아니다. Strategy의 `MATCHED_OBSERVATION`과 Risk의 `OBSERVE_PASS`도 계속 observe-only 근거일 뿐 주문 승인으로 해석하지 않는다.

## 상태 분류

허용 가능한 timing 후보는 `GOOD_PULLBACK`, `PULLBACK_RECLAIM`, `VWAP_RECLAIM`,
`MOMENTUM_CONTINUATION`이다. 이 상태도 바로 주문으로 이어지지 않고 `OrderPlanDraft`
초안으로만 남는다.

`MOMENTUM_CONTINUATION`은 BREAKOUT/LEADER condition role 또는 `BREAKOUT_RETEST`
strategy setup이 있는 후보에만 적용된다. 신고가 부근이라는 이유만으로 `CHASE_HIGH`가
되지는 않지만, VWAP 과열은 계속 차단한다. 이 route는 size multiplier 0.5, shorter TTL,
tight stop requirement를 draft evidence에 남긴다.

차단 또는 보류 상태는 `CHASE_HIGH`, `VWAP_OVEREXTENDED`, `FAILED_BREAKOUT`, `NO_SETUP`, `DATA_WAIT`, `STALE`, `BLOCKED_CONTEXT`이다. `DATA_WAIT`, VWAP warmup, momentum warmup, strategy/risk observation 미생성은 폐기하지 않고 near-miss evidence로 남긴다.

## OrderPlanDraft

Draft는 `BUY` + `LIMIT`만 표현한다. 시장가 매수는 기본 금지이며 `ENTRY_TIMING_ALLOW_MARKET_ORDER=true`는 PR-3에서 허용하지 않는다.

주요 필드는 다음 의미를 가진다.

- `status`: `PLAN_READY`, `WAIT_RETRY`, `DATA_WAIT` 등 PR-4 safety gate 입력 상태
- `setup_type`: `THEME_LEADER_PULLBACK`, `VWAP_RECLAIM`, `BREAKOUT_RETEST`, `MOMENTUM_CONTINUATION`, `THEME_FOLLOWER_EXPANSION`, `NO_SETUP`
- `entry_timing_state`, `price_location_state`: 가격 위치와 timing 판정
- `limit_price`: best ask와 spread가 정상일 때 best ask, 아니면 current price 기반 KRX tick 정규화 가격
- `suggested_quantity`, `suggested_notional`: pilot notional 기준 초안 값이며 PR-4에서 재검증 필요
- `idempotency_key`: trade date, code, theme, setup, timing, side, candidate id 기반 안정 key
- `observe_only=true`, `not_order_intent=true`: 초안이 주문 의도가 아님을 고정

## Strategy/Risk 참고 방식

StrategyObservation은 setup 근거로만 사용한다. `MATCHED_OBSERVATION`은 매수 신호가 아니다.

RiskObservation은 caution/block reason으로만 사용한다. `OBSERVE_BLOCK`이면 `PLAN_READY`가 될 수 없고, `OBSERVE_PASS`여도 주문 승인으로 해석하지 않는다. `DATA_WAIT`은 `DATA_WAIT` 또는 `WAIT_RETRY` near-miss로 남긴다.

## 운영 확인

API:

- `GET /api/entry-timing/status`
- `GET /api/entry-timing/plans/latest`
- `GET /api/entry-timing/plans/{order_plan_id}`
- `GET /api/entry-timing/evaluations`
- `POST /api/entry-timing/evaluate`

Tool:

```powershell
python -m tools.evaluate_entry_timing
python -m tools.evaluate_entry_timing --candidate-instance-id CAND-...
python -m tools.inspect_order_plan_draft --order-plan-id OPD-...
```

## 남은 리스크

PR-2 ThemeLeadership latest snapshot persistence는 여전히 on-demand 성격이다. PR-3는 candidate source metadata를 우선 사용하고, 없으면 theme latest 또는 on-demand watchset 결과로 보강한다. 별도 ThemeLeadership persistence 강화는 후속 작업으로 남긴다.

Threshold는 초기 pilot 값이다. 실장 관측 데이터로 VWAP tolerance, pullback range, spread/turnover/execution strength 기준을 calibration해야 한다.

## PR-4 연결

PR-4는 `order_plan_drafts_latest`에서 `PLAN_READY`만 선택하되, 다음을 다시 확인한 뒤에만 `LiveSimIntent`로 변환한다.

- LIVE_SIM safety gate, kill switch, simulation account/server/broker
- latest tick freshness와 draft price 대비 drift
- candidate `CONTEXT_READY`
- strategy `MATCHED_OBSERVATION`
- risk `OBSERVE_PASS`
- duplicate `order_plan_id`/code active intent/order
- daily notional, daily order count, active order/position limit

`queue_commands=false`가 기본이며, `--queue-commands`와 `LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=true`가 함께 있어야 `GatewayCommand(send_order)`가 생성된다. PR-4 pipeline은 run_once만 제공하고 scheduler는 후속 PR로 남긴다.

자세한 운영 절차는 [LIVE_SIM Pilot Pipeline from OrderPlanDraft](live_sim_pilot_pipeline_ko.md)를 따른다.

## PR-6 AI Advisory 참고

AI Candidate Scorer는 `order_plan_drafts_latest`의 `PLAN_READY`, `WAIT_RETRY`, `DATA_WAIT` 후보를 읽어 operator용 annotation을 만든다. 이 과정은 `OrderPlanDraft` status, quantity, notional, limit price를 변경하지 않는다.

AI selected가 있어도 `PLAN_READY`로 승격하지 않고, AI selected가 아니어도 기존 rule-based `PLAN_READY`를 차단하지 않는다. AI risk/reward는 clamp된 제안으로만 저장되며 EntryTiming/Exit 설정에 자동 반영되지 않는다.

자세한 내용은 [AI Candidate Scorer Advisory](ai_candidate_scorer_advisory_ko.md)를 따른다.
