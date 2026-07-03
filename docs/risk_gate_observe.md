# Risk Gate Observe-Only

## 요약

Risk Gate는 PR7 `StrategyObservation`과 Candidate, Market Data, Theme Snapshot context를 읽어 deterministic risk observation을 저장한다. `OBSERVE_PASS`는 주문 승인이 아니다. Risk Gate는 OrderIntent, EntryPlan, PositionSizing, OMS state, GatewayCommand, send/cancel/modify order call을 만들지 않는다.

## Observe-only 원칙

- Risk observation은 `observe_only=true`로 직렬화된다.
- `OBSERVE_PASS`는 주문 승인이 아니다.
- `OBSERVE_CAUTION`은 운영자 검토 note다.
- `OBSERVE_BLOCK`은 관찰된 block reason이지 execution command가 아니다.
- AI Sidecar output은 Risk evaluation input이 아니다.
- Risk threshold는 config/code-review 입력이지 intraday 자동 변경 값이 아니다.

## 읽는 데이터

- `strategy_observations_latest`
- `strategy_observations`
- `strategy_setup_observations`
- `candidates`
- `candidate_context_latest`
- `market_ticks_latest`
- `market_minute_bars`
- `market_cross_exchange_observations` (`RISK_CROSS_EXCHANGE_DIVERGENCE_BP > 0`일 때만)
- `theme_latest_snapshots`
- `theme_snapshot_members`

`MATCHED_OBSERVATION`은 Strategy classifier 결과다. Risk가 이를 strategy-context check로 기록해도 매수 준비가 아니다.

## RiskObservationStatus

| Status | 의미 | 주문 관련 주의 |
| --- | --- | --- |
| `NOT_EVALUATED` | 의미 있는 평가가 없음 | 주문 판단 아님 |
| `DATA_WAIT` | 중요한 관찰 데이터 부족 | 데이터 대기 |
| `OBSERVE_PASS` | block/caution이 관측되지 않음 | 주문 승인이 아님 |
| `OBSERVE_CAUTION` | caution check 관측 | 검토 note |
| `OBSERVE_BLOCK` | block check 관측 | 실행 명령 아님 |
| `INVALID_CONTEXT` | context 불일치/누락 | 데이터 점검 |
| `STALE_CONTEXT` | context가 오래됨 | freshness 점검 |

## RiskCategory

| Category | 보는 내용 |
| --- | --- |
| `DATA_QUALITY` | latest tick, readiness, 1m bar, VWAP |
| `MARKET_CONTEXT` | market-wide context placeholder |
| `THEME_CONTEXT` | theme state, coverage, rising ratio, leader evidence |
| `CANDIDATE_CONTEXT` | `CONTEXT_READY`, stale/closed/data-wait, active source |
| `STRATEGY_CONTEXT` | matched/forming/no-setup/stale/invalid strategy observation |
| `CHASE_OVERHEAT` | high change rate, near-day-high, VWAP extension, shallow pullback |
| `LIQUIDITY_SPREAD` | spread, trade-value flow, cumulative trade value, execution strength |
| `DUPLICATE_COOLDOWN` | duplicate active candidate, recent risk observation cooldown |
| `PORTFOLIO_PLACEHOLDER` | portfolio/holding context unavailable |

## Check Rule 요약

- `DATA_QUALITY`: critical data가 missing/stale이면 `DATA_WAIT` 또는 `STALE_CONTEXT`가 될 수 있다.
- `THEME_CONTEXT`: weak theme state, low fresh coverage, weak rising ratio를 관찰한다.
- `CANDIDATE_CONTEXT`: non-`CONTEXT_READY`, stale/closed/data-wait, active source 부족을 관찰한다.
- `STRATEGY_CONTEXT`: `MATCHED_OBSERVATION` 여부를 읽지만 주문 승인으로 바꾸지 않는다.
- `MARKET_CONTEXT`: opt-in cross-exchange 가격 괴리를 관찰할 수 있다. 임계치 미설정 시 기존 Risk 동작을 유지한다.
- `CHASE_OVERHEAT`: 과열/추격 위험처럼 보이는 관측치를 기록한다.
- `LIQUIDITY_SPREAD`: 거래대금, spread, execution strength 부족을 관찰한다.
- `DUPLICATE_COOLDOWN`: 같은 code의 중복 active candidate나 최근 observation을 관찰한다.
- `PORTFOLIO_PLACEHOLDER`: PR10 이전 또는 portfolio 미구현 상태를 명시한다.

## PR10 DRY_RUN 연결

PR10 DRY_RUN은 latest `OBSERVE_PASS`를 eligibility input 중 하나로 읽을 수 있다. 그래도 live order approval이 아니다. Candidate state, Strategy status, fresh tick, duplicate check, limit, explicit DRY_RUN settings, PR10 safety gate가 모두 필요하며 결과는 내부 `DryRunIntent` simulation record뿐이다.

## Cross-Exchange Divergence

`RISK_CROSS_EXCHANGE_DIVERGENCE_BP` 기본값은 `0`이며 disabled다. 0보다 큰 값으로 설정하면 latest KRX/NXT regular 1분 관측치의 `abs(divergence_bp)`가 임계치를 초과할 때 `CROSS_EXCHANGE_DIVERGENCE` CAUTION을 기록한다.

이 CAUTION은 관찰 기록이다. 주문 차단, 주문 승인, routing 변경이 아니며 `OBSERVE_PASS`와 `MATCHED_OBSERVATION`의 의미를 바꾸지 않는다. 한쪽 시장만 있어 `divergence_bp=null`이면 cross-exchange check는 평가 불가로 남는다.

AI Sidecar, RCA, Codex Prompt Draft는 review-only이며 Risk나 OMS 자동 입력이 아니다.

## Storage

- `risk_observations`
- `risk_observations_latest`
- `risk_check_observations`
- `risk_evaluation_runs`
- `risk_evaluation_errors`

## API

- `GET /api/risk/status`
- `GET /api/risk/observations/latest`
- `GET /api/risk/candidates/{candidate_instance_id}`
- `GET /api/risk/candidates/{candidate_instance_id}/history`
- `GET /api/risk/observations/{risk_observation_id}`
- `GET /api/risk/observations/{risk_observation_id}/checks`
- `GET /api/risk/runs`
- `GET /api/risk/errors`
- `POST /api/risk/evaluate`

`POST /api/risk/evaluate`는 observation projection endpoint다. response는 `order_routing_enabled=false`를 유지한다.

## CLI

```powershell
python -m tools.evaluate_risk
python -m tools.evaluate_risk --candidate-instance-id CAND-2026-06-27-005930-1
python -m tools.evaluate_risk --strategy-observation-id strategy_observation_xxx
python -m tools.inspect_risk_observation `
  --candidate-instance-id CAND-2026-06-27-005930-1
```

## 운영자 체크포인트

- `OBSERVE_PASS`를 주문 승인으로 해석하지 않는다.
- `OBSERVE_BLOCK`을 주문 취소나 execution command로 해석하지 않는다.
- Risk가 비어 있으면 Strategy latest observation이 있는지 먼저 본다.
- Risk Gate는 `OrderIntent`, `GatewayCommand`, `send_order`, `cancel_order`, `modify_order`를 만들지 않는다.
