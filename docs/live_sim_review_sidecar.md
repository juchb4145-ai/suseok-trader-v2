# LIVE_SIM Review Sidecar

## 요약

PR AI-6 LIVE_SIM Review Sidecar는 LIVE_SIM session, order, reconcile, incident를 장후 복기하는 read-only report workflow다. simulation order가 왜 생성, 거부, 실패, ack, fill, unfilled, mismatch 상태가 되었는지 설명한다. Review output은 order input, retry instruction, cancel instruction, modify instruction, reconcile command가 아니다.

## Deterministic Review vs AI Insight

| 구분 | 의미 |
| --- | --- |
| deterministic review | SQLite row와 settings로 항상 생성 가능한 report |
| AI insight | `run_ai=true`를 명시했을 때만 optional로 연결되는 review enrichment |

기본은 `run_ai=false`다. AI disabled, unavailable, invalid, policy rejected여도 deterministic report는 유지된다.

## Workflow

- `build_live_sim_session_review(connection, trade_date, run_ai=false)`
- `build_live_sim_order_review(connection, live_sim_order_id, run_ai=false)`
- `build_live_sim_reconcile_review(connection, reconcile_id, run_ai=false)`
- `build_live_sim_incident_review(connection, trade_date=None, related_entity_id=None, run_ai=false)`
- `build_live_sim_order_reviews_for_trade_date(connection, trade_date, limit=50, run_ai=false)`

Section은 safety/config posture, intents, orders, executions, rejections, reconcile snapshots, gateway command status, command ack/failure, execution events, errors, idempotency evidence, dashboard safety reminder를 포함한다.

## Root Cause Categories

- `SAFETY_GATE`
- `ELIGIBILITY`
- `ORDER_COMMAND_QUEUE`
- `GATEWAY_TRANSPORT`
- `BROKER_ACK`
- `BROKER_REJECTION`
- `EXECUTION_EVENT`
- `PARTIAL_FILL`
- `NO_FILL`
- `RECONCILE_MISMATCH`
- `LOCAL_ONLY_RECONCILE`
- `DUPLICATE_IDEMPOTENCY`
- `LIMIT_GUARD`
- `ACCOUNT_GUARD`
- `CONFIGURATION`
- `AI_EXECUTION`
- `UNKNOWN`

## Storage

- `ai_live_sim_review_reports`
- `ai_live_sim_review_sections`
- `ai_live_sim_review_links`
- `ai_live_sim_review_errors`

Reports enforce:

- `observe_only=true`
- `review_only=true`
- `no_trading_side_effects=true`
- `live_real_allowed=false`
- `order_action_allowed=false`
- `gateway_command_allowed=false`

## API

- `GET /api/ai-sidecar/live-sim-review/status`
- `POST /api/ai-sidecar/live-sim-review/session/{trade_date}?run_ai=false`
- `POST /api/ai-sidecar/live-sim-review/order/{live_sim_order_id}?run_ai=false`
- `POST /api/ai-sidecar/live-sim-review/reconcile/{reconcile_id}?run_ai=false`
- `POST /api/ai-sidecar/live-sim-review/incident`
- `POST /api/ai-sidecar/live-sim-review/orders/batch`
- `GET /api/ai-sidecar/live-sim-review/reports`
- `GET /api/ai-sidecar/live-sim-review/reports/{review_id}`
- `GET /api/ai-sidecar/live-sim-review/errors`

POST endpoint는 report만 생성한다. LIVE_SIM order creation, Gateway command queue, broker API를 호출하지 않는다.

## CLI

```powershell
python tools/build_live_sim_session_review.py --trade-date 2026-06-27
python tools/build_live_sim_order_review.py --live-sim-order-id live_sim_order_x
python tools/build_live_sim_reconcile_review.py --reconcile-id live_sim_reconcile_x
python tools/build_live_sim_incident_review.py --trade-date 2026-06-27
python tools/build_live_sim_order_review_batch.py --trade-date 2026-06-27 --limit 20
```

`--run-ai`는 명시적 manual AI insight attempt가 필요할 때만 사용한다.

## Dashboard Display Policy

Dashboard snapshot:

- `live_sim.live_sim_review_available`
- `live_sim.live_sim_review_report_count`
- `live_sim.latest_live_sim_review_reports`
- `live_sim.latest_live_sim_review_errors`

AI Explanation Cards:

- `LIVE_SIM_SESSION_REVIEW`
- `LIVE_SIM_ORDER_REVIEW`
- `LIVE_SIM_RECONCILE_REVIEW`
- `LIVE_SIM_INCIDENT_REVIEW`

Dashboard는 read-only다. review generation button, AI run button, retry button, cancel button, modify button, order execution button이 없다.

## LIVE_REAL Boundary

PR AI-6은 LIVE_REAL safety project가 아니다. real account mode, real server mode, Kiwoom real order call, real broker order routing, production readiness를 enable하지 않는다.

## 운영자 체크포인트

- LIVE_SIM Review는 복기 report다.
- Review output으로 주문 retry, cancel, modify를 하지 않는다.
- AI output은 review enrichment일 뿐이다.
- LIVE_REAL은 현재 구현되어 있지 않다.
