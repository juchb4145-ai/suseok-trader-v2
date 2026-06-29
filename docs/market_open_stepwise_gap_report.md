# Market Open Stepwise Gap Report

작성일: 2026-06-29  
대상 저장소: `suseok-trader-v2` (`C:\Users\juchn\주식3`)  
확인 브랜치: `main`

## Step 0 결론

현재 `suseok-trader-v2`는 Core/Gateway/MarketData/Theme/Candidate/Strategy/Risk/EntryTiming/LIVE_SIM 계층이 모두 존재한다. 다만 기본 Core 실행만으로 Gateway event 이후 Theme -> Candidate -> Strategy -> Risk -> EntryTiming이 장중 자동 연쇄 실행되지는 않는다. 장중 복구의 핵심은 threshold 완화가 아니라 다음 두 흐름을 운영자가 명확히 분리해 실행하는 것이다.

- 자동: Gateway event 저장, price_tick/condition_event/tr_response의 MarketData projection, Dashboard read-only 조회
- 수동: Theme import/snapshot/leadership, Candidate rebuild, Strategy/Risk/EntryTiming evaluate, observe cycle run_once

## Gap Table

| 항목 | 현재 존재 여부 | 자동/수동 | 확인한 파일/API | Gap 또는 운영상 주의 | 판정 |
| --- | --- | --- | --- | --- | --- |
| Core API startup | 존재 | 수동 | `apps/core_api.py`, `tools/start_core.ps1`, `/health`, `/api/status` | `start_core.ps1`은 uvicorn 실행만 하며 장중 OBSERVE env profile은 별도 helper가 담당 | PASS |
| 32-bit Kiwoom Gateway | 존재 | 수동 | `apps/kiwoom_gateway.py`, `gateway/kiwoom_runtime.py`, `gateway/kiwoom_client.py` | 32-bit Python/PyQt5/Kiwoom 환경이 필요하며 Core와 별도 프로세스로 실행 | PASS |
| Gateway token/env | 존재 | 수동 | `apps/kiwoom_gateway.py`, `tools/start_market_open_observe.ps1`, `api/dependencies/auth.py` | Gateway 기본 token은 `GATEWAY_CORE_TOKEN` 우선, 없으면 `TRADING_CORE_TOKEN` fallback이다. helper가 두 env를 맞춰 준다. | PASS |
| Market Data projection | 존재 | 자동 | `api/routes/gateway.py`, `services/market_data_service.py`, `/api/market-data/*` | `price_tick`, `condition_event`, `tr_response`는 Gateway event 수신 시 projection된다. projection error는 별도 table/API로 확인 가능 | PASS |
| Theme membership/import | 존재 | 수동 | `api/routes/themes.py`, `tools/import_theme_memberships.py`, `tools/import_naver_themes.py` | membership이 비면 Theme 이후 단계는 살아나지 않는다. Naver import도 자동 sync가 아니라 수동 import다. | WARN |
| Theme snapshot/leadership | 존재 | 수동 | `services/theme_service.py`, `services/theme_leadership/*`, `tools/rebuild_theme_snapshots.py` | Gateway event 후 자동 snapshot/leadership rebuild는 없다. observe cycle에서 run_once 가능 | WARN |
| Candidate FSM/rebuild | 존재 | 수동 | `services/candidate_service.py`, `api/routes/candidates.py`, `tools/rebuild_candidates.py` | condition/theme source가 있어도 rebuild가 돌지 않으면 후보가 갱신되지 않는다. | WARN |
| Strategy evaluate | 존재 | 수동 | `services/strategy_engine.py`, `api/routes/strategy.py`, `tools/evaluate_strategy.py` | Candidate 이후 자동 evaluate는 없다. observe cycle에서 run_once 가능 | WARN |
| Risk evaluate | 존재 | 수동 | `services/risk_gate.py`, `api/routes/risk.py`, `tools/evaluate_risk.py` | Strategy 이후 자동 evaluate는 없다. threshold 완화 없이 관찰 결과만 기록 | WARN |
| EntryTiming/order plan draft | 존재 | 수동 | `services/entry_timing/*`, `api/routes/entry_timing.py`, `tools/evaluate_entry_timing.py` | `OrderPlanDraft`는 주문 intent가 아니다. observe-only draft로만 생성된다. | PASS |
| Dashboard operator summary | 존재 | 자동 조회 | `services/dashboard_service.py`, `web/templates/dashboard.html`, `web/static/dashboard.js` | pipeline stage summary에 `OrderSafety`, endpoint, last_updated_at가 포함된다. 기존 작업 트리의 UI wrapping 변경은 보존 필요 | PASS |
| ops_check/alerts | 존재 | 수동 | `services/operator/no_buy_sentinel.py`, `api/routes/operator.py`, `tools/ops_market_open_rca.py` | RCA가 `OrderSafety`, error details, `--out-dir`를 지원하며 stage별 reason code를 report로 남긴다. | PASS |
| LIVE_SIM status/preflight/operator cycle | 존재 | 수동/read-only 가능 | `api/routes/live_sim.py`, `services/runtime/preflight.py`, `services/runtime/market_open_observe_cycle.py`, `tools/run_market_open_observe_cycle.py` | observe cycle은 preflight/status만 읽고 `queue_commands=false`로 호출한다. CLI 실행 시 JSON/Markdown run report를 남긴다. | PASS |
| 주문 command 차단 정책 | 존재 | 자동 검증 | `storage/gateway_command_store.py`, `gateway/command_handlers.py`, `services/runtime/market_open_observe_cycle.py` | observe cycle은 `send_order` delta가 0이어야 정상이다. `LIVE_REAL`은 구현/활성화 대상이 아니다. | PASS |

## 자동으로 도는 단계

1. Core API lifespan에서 DB schema 초기화
2. Gateway `POST /api/gateway/events` 수신과 event store 저장
3. `price_tick`, `condition_event`, `tr_response`의 MarketData projection
4. Dashboard snapshot과 pipeline summary의 read-only 집계
5. Gateway command status/count 조회

## 수동 실행이 필요한 단계

1. Core OBSERVE profile 준비: `.\tools\start_market_open_observe.ps1`
2. Theme membership import: `python -m tools.import_theme_memberships --file data/themes/sample_themes.json`
3. Theme/Naver import: `python -m tools.import_naver_themes ...`
4. Theme snapshot rebuild: `python -m tools.rebuild_theme_snapshots`
5. Candidate rebuild: `python -m tools.rebuild_candidates`
6. Strategy evaluate: `python -m tools.evaluate_strategy`
7. Risk evaluate: `python -m tools.evaluate_risk`
8. EntryTiming evaluate: `python -m tools.evaluate_entry_timing`
9. 통합 observe-only cycle: `python -m tools.run_market_open_observe_cycle --trade-date 2026-06-29 --out-dir reports/market_open_observe_cycle`
10. 장중 RCA: `python -m tools.ops_market_open_rca --core-url http://127.0.0.1:8000 --token $env:TRADING_CORE_TOKEN --trade-date 2026-06-29 --out-dir reports/market_open_rca`

## 운영상 핵심 판단

- 장중에 price tick은 들어오는데 Candidate/Strategy/Risk/EntryTiming이 비어 있으면, 우선 자동 projection 문제와 수동 rebuild/evaluate 미실행 문제를 분리해야 한다.
- Theme membership이 0이면 Candidate가 생기지 않는 것은 정상적인 BLOCK이다.
- Strategy/Risk/EntryTiming이 WARN이어도 threshold를 낮추지 않는다. 먼저 run_once 여부, 입력 count, stale tick, projection error를 확인한다.
- `LIVE_SIM_DISABLED_EXPECTED`, `LIVE_SIM_KILL_SWITCH_ON_EXPECTED`, `ORDER_COMMAND_ZERO_EXPECTED`는 OBSERVE 운용에서 정상 안전 상태다.
- AI/External LLM 결과는 advisory-only이며 주문, 리스크, 진입계획에 자동 반영하지 않는다.
