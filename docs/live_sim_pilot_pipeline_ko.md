# LIVE_SIM Pilot Pipeline from OrderPlanDraft

## 범위

PR-4는 `order_plan_drafts_latest`의 `PLAN_READY` 초안을 LIVE_SIM 모의투자 전용 safety gate로 다시 검증한 뒤 `LiveSimIntent`, `LiveSimOrderRecord`, `GatewayCommand(send_order)`까지 연결하는 run_once 파일럿이다.

- LIVE_SIM 모의투자 전용이다.
- LIVE_REAL은 구현하지 않는다.
- `OrderPlanDraft`는 주문 의도가 아니며 `PLAN_READY`만으로 주문하지 않는다.
- scheduler, daemon, APScheduler, while-loop 반복 자동매매는 PR-4 범위가 아니다.
- Naver importer는 run_once 안에서 자동 fetch하지 않는다.
- PR-5 이후에도 신규 BUY는 이 PR-4 path에서만 생성된다.

## 설정

기본값은 모두 disabled 또는 safe다.

첫 번째 선택지는 `TRADING_PROFILE`이다. `TRADING_MODE`, `TRADING_ALLOW_LIVE_SIM`은
legacy compatibility/safety switch로 남아 있지만 capability source of truth가 아니다.
`TRADING_ALLOW_LIVE_REAL=false`는 계속 안전 잠금으로 유지한다.

```env
TRADING_PROFILE=OBSERVE
LIVE_SIM_PILOT_PIPELINE_ENABLED=false
LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=false
LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=false
LIVE_SIM_ORDER_PLAN_REQUIRE_PLAN_READY=true
LIVE_SIM_ORDER_PLAN_REQUIRE_FRESH_TICK=true
LIVE_SIM_ORDER_PLAN_STALE_SEC=30
LIVE_SIM_ORDER_PLAN_MAX_PRICE_DRIFT_PCT=0.8
LIVE_SIM_ORDER_PLAN_REQUIRE_STRATEGY_MATCHED=true
LIVE_SIM_ORDER_PLAN_REQUIRE_RISK_OBSERVE_PASS=true
LIVE_SIM_ORDER_PLAN_REQUIRE_CANDIDATE_CONTEXT_READY=true
LIVE_SIM_ORDER_PLAN_REQUIRE_DRY_RUN_EVIDENCE=false
LIVE_SIM_ORDER_PLAN_MAX_PLANS_PER_RUN=3
LIVE_SIM_ORDER_PLAN_MAX_COMMANDS_PER_RUN=1
LIVE_SIM_ORDER_PLAN_MIN_NOTIONAL=10000
LIVE_SIM_ORDER_PLAN_DEFAULT_NOTIONAL=100000
LIVE_SIM_ORDER_PLAN_MAX_NOTIONAL=100000
LIVE_SIM_ORDER_PLAN_ALLOW_MARKET_ORDER=false
LIVE_SIM_ORDER_PLAN_ALLOWED_SIDE=BUY
```

command queue까지 허용하려면 위 설정 외 기존 LIVE_SIM gate도 통과해야 한다.

```env
TRADING_PROFILE=LIVE_SIM_PILOT
# legacy compatibility/safety switches below
TRADING_MODE=LIVE_SIM
TRADING_ALLOW_LIVE_SIM=true
TRADING_ALLOW_LIVE_REAL=false
LIVE_SIM_ENABLED=true
LIVE_SIM_ORDER_ROUTING_ENABLED=true
LIVE_SIM_GATEWAY_COMMAND_ENABLED=true
LIVE_SIM_KILL_SWITCH=false
LIVE_SIM_ACCOUNT_ID=SIM-ACCOUNT-ID
LIVE_SIM_ACCOUNT_MODE=SIMULATION
LIVE_SIM_BROKER_ENV=SIMULATION
LIVE_SIM_SERVER_MODE=SIMULATION
LIVE_SIM_PILOT_PIPELINE_ENABLED=true
LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=true
LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=true
```

## Eligibility

`evaluate_live_sim_order_plan_eligibility()`는 다음을 검사한다.

- 기존 LIVE_SIM safety gate: kill switch, LIVE_REAL false, simulation-like account/server/broker, fresh heartbeat, gateway orderable, queue healthy, AI order tools disabled
- OrderPlanDraft: latest 일치, `PLAN_READY`, `observe_only=true`, `not_order_intent=true`, BUY, LIMIT, 수량/가격/금액 정상, 만료 전, allowed entry timing state
- Candidate/Strategy/Risk: candidate 존재, `CONTEXT_READY`, latest strategy `MATCHED_OBSERVATION`, latest risk `OBSERVE_PASS`
- Tick: latest tick 존재, fresh, 가격 drift 한도 이내, limit price 대비 과도한 상방 이탈 없음, spread 과다 차단
- Duplicate/Limit: 같은 `order_plan_id` intent 중복, 같은 code active order, daily count/notional, active order/position limit
- PR-5 lifecycle: reconcile mismatch block, same-code open position, active exit/cancel, unresolved lifecycle error
- optional dry-run evidence: `LIVE_SIM_ORDER_PLAN_REQUIRE_DRY_RUN_EVIDENCE=true`일 때만 요구

모든 rejection은 `live_sim_rejections.evidence_json`에 `order_plan_id`, reason category, safety evidence를 남긴다.

## run_once 흐름

```text
operator Naver import, 별도 실행
  -> ThemeLeadership / Candidate / Strategy / Risk / EntryTiming
  -> order_plan_drafts_latest PLAN_READY
  -> run_live_sim_pilot_pipeline_once
  -> OrderPlan eligibility
  -> LiveSimIntent
  -> optional GatewayCommand(send_order)
```

`queue_commands=false`가 기본이다. `--queue-commands` 또는 API `queue_commands=true`를 줘도 `LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=true`와 기존 safety gate가 모두 켜져야 command가 생성된다.

## PR-6 AI Advisory 연결

`run_live_sim_pilot_pipeline_once` 응답은 최신 AI Candidate Scorer run을 `ai_advisory_summary`로 포함할 수 있다. 이 값은 read-only summary이며 routing, eligibility, command queue 판단에 사용되지 않는다.

AI advisory가 없거나 `DISABLED`, `FAILED`, `INVALID_SCHEMA`, `TIMEOUT`이어도 PR-4/PR-5 rule-based LIVE_SIM pipeline은 계속 독립적으로 동작한다. AI score가 낮아도 기존 `PLAN_READY`를 차단하지 않고, AI selected가 있어도 plan을 자동 승격하지 않는다.

자세한 설정과 rollback은 [AI Candidate Scorer Advisory](ai_candidate_scorer_advisory_ko.md)를 따른다.

## API

Read-only:

- `GET /api/live-sim/order-plan-eligibility?order_plan_id=OPD-...`
- `GET /api/live-sim/pilot/runs`

Local-token protected:

- `POST /api/live-sim/intents/from-order-plan/{order_plan_id}`
- `POST /api/live-sim/orders/from-order-plan/{order_plan_id}`
- `POST /api/live-sim/pilot/run-once?queue_commands=false`
- `POST /api/live-sim/pilot/run-once?queue_commands=true`

모든 응답은 `live_sim_only=true`, `live_real_allowed=false`, `broker_order_path=LIVE_SIM_ONLY`, `real_order_allowed=false`를 포함한다.

## CLI

```powershell
python -m tools.evaluate_live_sim_order_plan --order-plan-id OPD-...
python -m tools.create_live_sim_intent_from_order_plan --order-plan-id OPD-...
python -m tools.run_live_sim_pilot_once
python -m tools.run_live_sim_pilot_once --queue-commands
python -m tools.run_live_sim_pilot_once --trade-date 2026-06-27 --limit 3
```

## 운영 순서

```powershell
python -m tools.import_naver_themes --dry-run --limit-themes 20
python -m tools.import_naver_themes --limit-themes 20
python -m tools.inspect_theme_leadership
python -m tools.evaluate_entry_timing
python -m tools.run_live_sim_pilot_once
python -m tools.run_live_sim_pilot_once --queue-commands
```

Naver 등락률, 네이버 순위, condition hit만으로는 주문을 만들지 않는다. 반드시 `OrderPlanDraft`와 PR-4 safety gate를 통과해야 한다.

## Rollback

```powershell
$env:LIVE_SIM_KILL_SWITCH = "true"
$env:LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND = "false"
$env:LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED = "false"
$env:LIVE_SIM_PILOT_PIPELINE_ENABLED = "false"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "false"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"
```

확인:

- `GET /api/live-sim/status`
- `GET /api/live-sim/rejections`
- `GET /api/live-sim/pilot/runs`
- `gateway_commands`에 신규 `send_order`가 없는지 확인

## PR-5 연결 항목

- 미체결 BUY TTL cancel은 `python -m tools.run_live_sim_cancel_unfilled_once` 또는 `POST /api/live-sim/cancel/run-once`로 실행한다.
- close-only SELL exit은 `python -m tools.run_live_sim_exit_once` 또는 `POST /api/live-sim/exit/run-once`로 실행한다.
- reconcile mismatch는 신규 BUY를 차단한다.
- 장중 5~10초 scheduler, 운영자 승인 workflow, 대시보드 실행 버튼은 후속 PR 범위다.
