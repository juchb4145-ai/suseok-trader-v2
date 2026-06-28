# Theme Leadership Service

## 요약

PR-2 RT-TLS Minimal Core Port는 기존 Theme Service 위에 실시간 테마 리더십 관찰 계층을 추가한다.

흐름은 다음이다.

```text
ThemeUniverse
  -> RealtimeSnapshot
  -> ThemeLeadershipRanking
  -> ThemeState
  -> StockRole
  -> Watchset
  -> observe-only CandidateSourceEvent
```

이 서비스는 주문 PR이 아니다. `LEADING`/`SPREADING` theme은 매수 신호가 아니고, `LEADER`/`CO_LEADER` role도 매수 신호가 아니다. watchset은 장중 관찰 대상 목록이며, Strategy/EntryTiming/OrderPlan/LIVE_SIM auto pipeline 입력으로 직접 승격되지 않는다.

## PR-3 EntryTiming 연결

PR-3 EntryTiming 계층은 watchset 또는 observe-only candidate source metadata를 입력으로 가격 위치를 다시 평가하고 `OrderPlanDraft` 초안만 만든다. `PLAN_READY` draft도 주문 승인이나 매수 신호가 아니며 PR-4 LIVE_SIM safety gate 입력 후보로만 사용한다.

Candidate source metadata에 저장된 `theme_state`, `stock_role`, `priority_score`를 우선 사용하며, 누락 시 theme latest 또는 on-demand watchset rebuild 결과로 보강한다.

## 범위

하는 일:

- 기존 `themes`/`theme_members` active membership에서 ThemeUniverse를 만든다.
- 기존 Market Data projection인 `market_ticks_latest`, `market_minute_bars`, `market_condition_latest`를 읽어 종목 realtime snapshot을 만든다.
- deterministic rule 기반으로 theme score, state, member role, watchset을 계산한다.
- `THEME_LEADERSHIP_WRITE_CANDIDATE_SOURCES=true`일 때만 기존 Candidate FSM에 observe-only source event를 저장할 수 있다.

하지 않는 일:

- `OrderIntent`, `GatewayCommand`, `send_order`, `cancel_order`, `modify_order` 생성 또는 호출.
- `EntryTimingEngine`, `OrderPlanBuilder`, `LIVE_SIM_AUTO_PIPELINE`, AI Candidate Scorer 구현.
- 조건검색 hit만으로 `LEADING`, `READY`, 매수 가능, 주문 가능 상태 생성.
- `DATA_WAIT`를 hard block이나 폐기 상태로 처리.
- `LIVE_REAL` 관련 구현.

## State와 Role

RT-TLS 전용 `ThemeState`:

- `DATA_WAIT`: valid member 또는 fresh coverage 부족. 폐기가 아니라 보류/재평가 상태다.
- `WATCH`: 관찰 가능하지만 확산/주도 조건이 약함.
- `LEADER_ONLY`: leader는 강하지만 breadth가 약함.
- `SPREADING`: 상승 member breadth와 거래대금이 확산 중.
- `LEADING`: 확산 조건에 leader strength, weighted return, freshness가 충분함.
- `FADING`: reserved.
- `WEAK`: fresh data는 있으나 약한 theme.

RT-TLS 전용 `StockRole`:

- `LEADER`
- `CO_LEADER`
- `FOLLOWER`
- `LATE_LAGGARD`
- `WEAK_MEMBER`
- `OVERHEATED`
- `STALE`
- `UNKNOWN`

기존 `domain.theme.state` 값과 충돌하지 않도록 별도 enum을 사용하며, 응답에는 legacy mapping도 함께 제공한다. 예를 들어 `LEADER_ONLY`와 `WEAK`는 기존 Theme Service 호환 표현으로는 `WATCH`에 가깝다.

## Watchset 정책

기본값:

- `THEME_LEADERSHIP_TOP_THEME_COUNT=5`
- `THEME_LEADERSHIP_MAX_STOCKS_PER_THEME=3`
- `THEME_LEADERSHIP_MAX_TOTAL_WATCHSET=20`
- `THEME_LEADERSHIP_MIN_VALID_MEMBERS=2`
- `THEME_LEADERSHIP_MIN_FRESH_COVERAGE_RATIO=0.4`

선정:

- `LEADING`: `LEADER`, `CO_LEADER`, `FOLLOWER`
- `SPREADING`: `LEADER`, `CO_LEADER`, `FOLLOWER`
- `LEADER_ONLY`: `LEADER`, `CO_LEADER`

제외 또는 near-miss:

- `DATA_WAIT`, `WATCH`, `WEAK`, `FADING` theme
- `STALE`, `OVERHEATED`, `LATE_LAGGARD`, `WEAK_MEMBER`, `UNKNOWN`
- VI active, upper limit near, abnormal spread

## Candidate Source

`build_candidate_source_events()`는 watchset item을 기존 Candidate FSM source event로 변환한다.

source type:

- `THEME_LEADER`
- `THEME_CO_LEADER`
- `THEME_FOLLOWER`
- `THEME_SPREADING_MEMBER`

metadata에는 `theme_id`, `theme_name`, `theme_state`, `theme_rank`, `stock_role`, `priority_score`, `reason_codes`, `observe_only=true`, `not_order_signal=true`가 들어간다.

기본 설정인 `THEME_LEADERSHIP_WRITE_CANDIDATE_SOURCES=false`에서는 계산만 하고 DB에는 저장하지 않는다. `true`여도 Candidate는 관찰 episode이며, `CONTEXT_READY`는 Strategy 평가 입력 준비일 뿐 주문 가능 상태가 아니다.

## API와 Tool

API:

- `GET /api/theme-leadership/snapshots/latest`
- `GET /api/theme-leadership/watchset`
- `POST /api/theme-leadership/rebuild`

Tool:

```bash
python -m tools.inspect_theme_leadership
python -m tools.inspect_theme_leadership --write-candidate-sources
```

`--write-candidate-sources`는 observe-only source event만 저장하며 주문 테이블이나 command queue를 변경하지 않는다.

## 다음 PR 범위

PR-3/PR-4에서 이어질 수 있는 일:

- EntryTiming 관찰 계층.
- OrderPlan 초안 계층.
- LIVE_SIM 수동 safety-gated 경로와의 연결.
- Naver/외부 importer 보강.

이 PR의 결과만으로 자동 주문을 켜면 안 된다.
