# Candidate FSM

## 요약

Candidate FSM은 observe-only 후보 관찰 에피소드를 관리한다. 여기서 `Candidate`는 매수 후보가 아니다. condition, theme, market source가 한 종목에 대해 관찰된 “에피소드”다. `CONTEXT_READY`는 매수 준비가 아니라 Strategy 평가 입력 준비다.

## 하는 일 / 하지 않는 일

| 구분 | 내용 |
| --- | --- |
| 하는 일 | condition/theme/market/manual/mock source를 candidate source로 기록 |
| 하는 일 | `candidate_instance_id`별 lifecycle 상태 저장 |
| 하는 일 | Strategy/Risk가 읽을 context latest 저장 |
| 하지 않는 일 | Strategy score, Risk approval, OrderIntent 생성 |
| 하지 않는 일 | EntryPlan, PositionSizing, `GatewayCommand`, broker order 생성 |

## Source Types

- `CONDITION_ENTER`
- `CONDITION_EXIT`
- `THEME_LEADER`
- `THEME_CO_LEADER`
- `THEME_FOLLOWER`
- `THEME_SPREADING_MEMBER`
- `MARKET_OBSERVED`
- `MANUAL_WATCH`
- `MOCK`

Condition source는 `market_condition_signals`에서 읽는다. Theme source는 `theme_latest_snapshots`와 `theme_snapshot_members`에서 읽고 `CANDIDATE_THEME_SOURCE_STATES`, `CANDIDATE_THEME_MEMBER_ROLES` 설정으로 필터링한다.

## State

| State | 의미 | 주문 관련 주의 |
| --- | --- | --- |
| `DETECTED` | source가 에피소드를 처음 관찰 | 매수 후보 확정 아님 |
| `HYDRATING` | tick, bar, theme/source context 수집 중 | 데이터 준비 중 |
| `WATCHING` | 최소 관찰 데이터 존재 | Strategy 입력 전 단계 |
| `CONTEXT_READY` | Strategy 평가 입력 준비 | 매수 준비 아님 |
| `DATA_WAIT` | 필요한 관찰 데이터 부족 | 주문 차단/승인 상태 아님 |
| `BLOCKED_OBSERVATION` | 명확한 이유로 관찰 제외/보류 | 실행 command 아님 |
| `STALE` | source 또는 tick freshness 초과 | 오래된 관찰 |
| `COOLDOWN` | episode churn control 예약 | 주문 cooldown 아님 |
| `CLOSED` | source exit, TTL, rotation 등으로 종료 | 주문 결과 아님 |

## Identity

active episode key는 `(trade_date, code)`다. active episode가 없으면 다음 형식으로 생성된다.

```text
CAND-{trade_date}-{code}-{generation}
```

같은 code가 active 상태에서 다시 감지되면 기존 candidate에 source를 merge한다.

## Transition Rules

| 전이 | 조건 |
| --- | --- |
| new source -> `DETECTED` -> `HYDRATING` | active candidate가 없을 때 |
| `HYDRATING` -> `DATA_WAIT` | latest tick, readiness, required bar, required theme context 부족 |
| `HYDRATING` -> `WATCHING` | latest tick, non-missing readiness, source attribution 존재 |
| `DATA_WAIT` -> `WATCHING` | 부족한 데이터 회복 |
| `WATCHING` -> `CONTEXT_READY` | readiness, 1m bar, VWAP, source context 조건 충족 |
| active -> `STALE` | source/tick age가 stale threshold 초과 |
| active -> `CLOSED` | active source 0, TTL, condition exit, theme rotation 등 |

state가 바뀌지 않는 반복 refresh는 duplicate transition row를 만들지 않는다.

## Storage

- `candidates`
- `candidate_source_events`
- `candidate_sources_latest`
- `candidate_state_transitions`
- `candidate_context_latest`
- `candidate_projection_errors`

`candidate_state_transitions`는 Candidate FSM의 event log다. Gateway event log와 별도이며 `gateway_commands`를 만들지 않는다.

## Context Refresh

`refresh_candidate_context()`가 읽는 값:

- latest tick과 readiness
- 60/180/300 second bar 존재 여부
- VWAP readiness
- active condition source
- active theme source의 snapshot/member row
- active/total source count

결과는 `candidate_context_latest`의 `theme_context_json`, `market_context_json`, `source_context_json`, `readiness_json`에 저장된다.

## Strategy / Risk 연결

Strategy Engine은 `CONTEXT_READY` candidate context를 읽어 `StrategyObservation`을 저장한다. Candidate state를 바꾸지 않는다.

Risk Gate는 Candidate, Strategy, Market Data, Theme Snapshot을 읽어 `RiskObservation`을 저장한다. Candidate state를 바꾸지 않는다.

`MATCHED_OBSERVATION`은 매수 신호가 아니고 `OBSERVE_PASS`는 주문 승인이 아니다.

## API

- `GET /api/candidates/status`
- `GET /api/candidates`
- `GET /api/candidates/{candidate_instance_id}`
- `GET /api/candidates/{candidate_instance_id}/sources`
- `GET /api/candidates/{candidate_instance_id}/transitions`
- `GET /api/candidates/by-code/{code}`
- `GET /api/candidates/projection-errors`
- `POST /api/candidates/rebuild`

`POST /api/candidates/rebuild`는 projection rebuild endpoint다. 주문 endpoint가 아니다.

## Rebuild / Inspect

```powershell
python -m tools.rebuild_candidates
```

```powershell
python -m tools.inspect_candidate --candidate-instance-id CAND-2026-06-27-005930-1 `
  --include-context --include-sources --include-transitions
```

## 운영자 체크포인트

- Candidate는 매수 후보가 아니라 관찰 에피소드다.
- `CONTEXT_READY`는 Strategy 평가 입력 준비다.
- Candidate FSM은 `GatewayCommand`, `OrderIntent`, `send_order`를 만들지 않는다.
- Candidate가 막히면 readiness, source freshness, theme context를 먼저 확인한다.
