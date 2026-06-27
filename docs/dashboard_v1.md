# Dashboard V1

## 요약

Dashboard V1은 Gateway -> Market Data -> Theme -> Candidate -> Strategy -> Risk observe-only pipeline을 한 화면에서 확인하는 운영 대시보드다. 목적은 “어디서 막혔는지 보는 것”이다. Dashboard는 읽기 전용이며 실행 버튼이 없다.

## Read-only 원칙

- Dashboard는 Core API의 read-only GET endpoint만 사용한다.
- Dashboard UI에는 주문, 매수, 매도, 취소, 정정, rebuild, evaluate, import 실행 버튼이 없다.
- Dashboard는 DRY_RUN intent/order/fill을 생성하지 않는다.
- Dashboard는 live `OrderIntent`, `GatewayCommand`, broker order path를 만들거나 전송하지 않는다.
- Dashboard는 `LIVE_SIM`, `LIVE_REAL` flag를 변경하지 않는다.
- Dashboard는 OpenAI API를 호출하지 않고 AI 실행 POST도 보내지 않는다.

## 운영자가 보는 순서

1. Safety Banner에서 `trading_mode`, live flag, order routing 상태를 본다.
2. System Status에서 Core/Gateway/AI Sidecar/token 필요 여부를 본다.
3. Pipeline Summary에서 어느 단계 count가 줄어드는지 본다.
4. Market Data와 Theme에서 데이터 freshness와 snapshot을 본다.
5. Candidate Funnel에서 `DATA_WAIT`, `STALE`, `CONTEXT_READY` 분포를 본다.
6. Strategy Observations에서 `MATCHED_OBSERVATION`을 보되 매수 신호로 해석하지 않는다.
7. Risk Notes에서 `OBSERVE_PASS`, `OBSERVE_CAUTION`, `OBSERVE_BLOCK`을 보되 주문 승인으로 해석하지 않는다.
8. DRY_RUN/LIVE_SIM panel은 read-only 결과만 확인한다.
9. AI Explanation Cards는 review-only 설명으로만 읽는다.

## 화면 구성

| Section | 의미 |
| --- | --- |
| Safety Banner | OBSERVE/live flag/order routing/AI execution disabled/read-only 경고 |
| System Status | `/api/status`, Gateway, AI Sidecar, token 필요 여부 |
| Pipeline Summary | Gateway Events -> Market Ticks -> Theme Snapshots -> Candidates -> Strategy -> Risk funnel |
| Theme Board | latest theme snapshot, state, quality, leader, 거래대금, 상승 비율 |
| Candidate Funnel | candidate state count와 candidate episode |
| Strategy Observations | latest `StrategyObservation`, setup classifier 결과 |
| Risk Notes | latest `RiskObservation`, severity, reason code |
| DRY_RUN OMS | internal simulation status, recent intents/orders, paper positions |
| DRY_RUN Exit | exit evaluation과 simulated signal |
| LIVE_SIM | simulation-account-only status, intents, orders, executions, rejections, reconcile |
| Recent Events / Errors | recent Gateway events와 projection/evaluation error |
| AI Explanation Cards | RCA, AI insight, Codex prompt draft, LIVE_SIM review card |

## Snapshot API

`GET /api/dashboard/snapshot`

Query:

- `detail`: `summary` 또는 `full`
- `limit`: 기본값 `DASHBOARD_SNAPSHOT_DEFAULT_LIMIT`, 최대 `DASHBOARD_MAX_LIMIT`

Top-level section:

- `generated_at`
- `safety`
- `system`
- `gateway`
- `market_data`
- `themes`
- `candidates`
- `strategy`
- `risk`
- `dry_run`
- `live_sim`
- `ai_sidecar`
- `ai_explanations`
- `recent_events`
- `errors`
- `pipeline_summary`

Additional endpoints:

- `GET /api/dashboard/status`
- `GET /api/dashboard/funnel`
- `GET /api/dashboard/errors`
- `GET /api/dashboard/ai-explanations`
- `GET /api/dashboard/ai-explanations/status`
- `GET /api/dashboard/ai-explanations/candidate/{candidate_instance_id}`
- `GET /api/dashboard/ai-explanations/no-trade/{trade_date}`
- `GET /`
- `GET /dashboard`

## Safety Banner 의미

Dashboard safety section은 다음 값을 명시한다.

- `trading_mode`
- `live_sim_allowed`
- `live_real_allowed`
- `order_routing_enabled=false`
- `order_controls_available=false`
- `gateway_order_commands_allowed=false`
- `ai_sidecar_enabled`
- `ai_execution_available=false`
- `observe_only_pipeline=true`
- `dry_run_only=true`
- `broker_order_sent=false`

필수 해석:

- `MATCHED_OBSERVATION`은 매수 신호가 아니다.
- `OBSERVE_PASS`는 주문 승인이 아니다.
- Dashboard는 읽기 전용이며 실행 버튼이 없다.

## AI Sidecar 표시 정책

Dashboard는 저장된 `ai_requests`, `ai_insights`, RCA report, Codex prompt draft, LIVE_SIM review report를 보여줄 수 있다. 하지만 AI 실행 버튼, RCA 생성 버튼, Codex 실행 버튼, GitHub write 버튼, 주문 버튼은 없다.

AI/RCA/Codex 결과는 Strategy/Risk/OMS 자동 입력이 아니다.

## DRY_RUN / LIVE_SIM 표시 정책

DRY_RUN panel은 내부 모의 회계 상태만 보여준다. DRY_RUN은 브로커 주문이 아니다.

LIVE_SIM panel은 simulation-account-only 상태와 결과를 read-only로 보여준다. LIVE_SIM은 모의투자 전용이며 실계좌 주문이 아니다.

Dashboard JavaScript는 display GET만 사용하며 DRY_RUN/LIVE_SIM POST endpoint를 호출하지 않는다.

## Local Runbook

```powershell
python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000 --reload
```

브라우저:

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/dashboard`

Mock observe flow:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots
python -m tools.rebuild_candidates
python -m tools.evaluate_strategy
python -m tools.evaluate_risk
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/status
Invoke-RestMethod http://127.0.0.1:8000/api/dashboard/snapshot
```

## Troubleshooting

| 증상 | 먼저 볼 곳 |
| --- | --- |
| Dashboard가 비어 있음 | `/api/gateway/status`, `/api/market-data/status` |
| Theme가 비어 있음 | theme membership import, snapshot rebuild 여부 |
| Candidate가 `DATA_WAIT` | latest tick, 1m bar, VWAP readiness |
| Strategy가 `DATA_WAIT` | Candidate context, Market Data stale 설정 |
| Risk가 비어 있음 | latest Strategy observation 존재 여부 |
| AI insight가 비어 있음 | 정상일 수 있음. AI 실행은 수동이고 기본 disabled |
| LIVE_SIM blocked | `/api/live-sim/status` safety gate reason |

## 운영자 체크포인트

- Dashboard는 실행 화면이 아니라 관찰 화면이다.
- 버튼이 없다는 점이 안전 설계다.
- Strategy/Risk status를 주문 상태로 읽지 않는다.
- DRY_RUN과 LIVE_SIM 결과를 실계좌 주문으로 읽지 않는다.
- `LIVE_REAL`은 현재 구현되어 있지 않다.
