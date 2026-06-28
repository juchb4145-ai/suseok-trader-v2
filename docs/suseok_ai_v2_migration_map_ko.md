# suseok_ai Functional Inventory & v2 Migration Map

## 1. 요약

이 문서는 `suseok_ai` 코드와 문서를 기준으로, 실전 자동매매에 필요했던 기능을 inventory화하고 `suseok-trader-v2`로 가져올 것, 버릴 것, 재설계할 것을 분리한다. v2를 장중에 충분히 돌린 결과 분석이 아니며, v2 runtime gap report나 RCA가 아니다.

비범위는 명확하다.

- `LIVE_REAL` 구현 또는 설계 확장.
- `LIVE_SIM_AUTO_PIPELINE` 구현.
- AI가 주문 의사결정을 내리는 구조 추가.
- suseok_ai legacy code 단순 복사.
- v2 장중 실행 결과가 있었던 것처럼 원인 분석 작성.

핵심 결론은 다음과 같다.

| 구분 | 결론 |
| --- | --- |
| 가져올 것 | 실시간 테마 판단력, Theme universe, realtime snapshot, theme leadership ranking, theme state classification, stock role classification, watchset selection, `GOOD_PULLBACK`, `PULLBACK_RECLAIM`, `VWAP_RECLAIM`, stable idempotency key 원칙, LIVE_SIM 후보 생성에 필요한 최소 lifecycle |
| 버릴 것 | legacy hybrid final grade 중심 구조, threshold A/B, shadow small-entry report 중심 구조, 조건검색 이름 의존 매수 판단, review/debug panel 과다 확장, `DATA_INSUFFICIENT` 일괄 hard block, AI 직접 주문 판단 |
| 재설계할 것 | suseok_ai의 기능을 v2 `Core/Gateway/Service` 경계에 맞춰 deterministic service와 interface로 재구성한다. Gateway는 Kiwoom 통신과 command polling만 담당하고, Core는 판단/정책/상태 저장만 담당한다. |
| PR-2 최소 범위 | `RT-TLS` port의 최소 단위는 `ThemeUniverse -> RealtimeSnapshot -> ThemeLeadershipRanking -> ThemeState -> StockRole -> Watchset`이다. 주문, LIVE_SIM 자동 생성, AI 기능은 제외한다. |

PR-2 구현 메모:

- v2에는 `services/theme_leadership`가 추가된다.
- 기존 Theme Service를 대체하지 않고 `themes/theme_members`와 Market Data projection 위에서 계산한다.
- condition include는 discovery/boost source일 뿐이며 단독으로 `LEADING`이나 매수 가능 상태를 만들지 않는다.
- `DATA_WAIT`는 hard block이 아니라 보류/재평가 상태다.
- CandidateSourceEvent 연결은 observe-only이며 `THEME_LEADERSHIP_WRITE_CANDIDATE_SOURCES=false`가 기본이다.
- EntryTiming, OrderPlan, LIVE_SIM auto pipeline은 PR-3/PR-4 범위다.

근거로 확인한 suseok_ai 주요 영역은 다음이다.

- `trading/theme_engine/*`: Naver theme universe, evidence/membership, seed signal, cohort, state machine, stock role, leadership handover, focused expansion, candidate bridge.
- `trading/strategy/*`: candidate ingestion/FSM/hydration/readiness, entry engine, setup router, order risk/order manager/exit.
- `trading_app/order_enqueue_service.py`: runtime order enqueue, LIVE_SIM guard, idempotency, lifecycle/reconcile guard.
- `trading/broker/*`, `apps/kiwoom_gateway.py`, `kiwoom/*`: Gateway command queue, Kiwoom TR/realtime/condition/chejan integration.
- `docs/strategy_reboot_v2_theme_core_v3.md`, `docs/dynamic_theme_engine.md`, `docs/intraday_theme_rotation.md`, `docs/setup_router_v3_observe.md`, `docs/candidate_instance_identity.md`, `docs/dashboard_panel_inventory.md`.

v2 기준 경계는 다음 문서/코드에 맞춘다.

- `docs/live_sim_pilot_roadmap_ko.md`
- `docs/architecture.md`
- `docs/theme_service.md`
- `docs/candidate_fsm.md`
- `docs/strategy_engine_observe.md`
- `docs/risk_gate_observe.md`
- `docs/live_sim_enablement.md`
- `domain/theme/*`, `services/theme_service.py`, `services/candidate_service.py`, `services/strategy_engine.py`, `services/risk_gate.py`, `services/live_sim/*`

## 2. suseok_ai 실전 흐름 개요

suseok_ai의 장중 자동매매 흐름은 크게 네 계층으로 나뉜다.

1. Kiwoom/Gateway 계층
   - Kiwoom OpenAPI+에서 realtime tick, condition include/remove, TR response, chejan/order event를 받는다.
   - Core로 `price_tick`, `condition_event`, `tr_response`, `chejan` 성격의 event를 전달한다.
   - Core가 만든 command를 polling해서 `tr_request`, `send_order`, `cancel_order` 등을 수행할 수 있는 구조가 있었다.

2. 시장/테마 판단 계층
   - Naver theme universe와 theme membership을 외부 reference로 구성한다.
   - Kiwoom realtime tick, `opt10032`, condition include, manual watch, holding, pending order를 live seed signal로 합친다.
   - Theme cohort, state machine, leadership handover, stock role classifier로 주도 테마와 주도주/동반주를 판단한다.

3. 후보/watchset 계층
   - ThemeCore output은 focused expansion/watchset과 candidate source event로 변환된다.
   - Candidate ingestion/FSM은 source event를 candidate instance lifecycle로 관리한다.
   - Hydration과 realtime subscription readiness를 통해 `WATCHING`, `WAIT_DATA`, `READY` 또는 observe-ready 계열로 갈라진다.

4. 진입/리스크/주문/청산 계층
   - EntryEngine과 SetupRouter는 pullback, reclaim, breakout retest 같은 setup/timing을 관찰한다.
   - OrderRiskManager, order enqueue service, LIVE_SIM guard, lifecycle/reconcile guard가 주문 가능성을 제한한다.
   - Exit engine과 reconcile/cancel scheduler가 stop loss, take profit, max hold, 장마감 청산, 미체결 취소를 다룬다.

이 중 v2로 그대로 옮기면 안 되는 부분이 있다. suseok_ai에는 observe-only로 잘 정리된 Theme Core V3 경로와, legacy hybrid final grade, threshold A/B, shadow small-entry, review/dashboard report가 섞여 있다. v2는 전자를 선별적으로 가져오고, 후자는 원칙 또는 데이터 항목만 남긴다.

장중 흐름을 v2 관점으로 축약하면 다음이다.

```text
Kiwoom event
  -> Gateway event store
  -> MarketData projection
  -> Theme universe + realtime snapshot
  -> Theme leadership/state/role classification
  -> Watchset/Candidate source observation
  -> Candidate context
  -> Strategy/Risk observe-only
  -> manual LIVE_SIM safety-gated path, if enabled by existing v2 flow
```

이번 PR의 대상은 위 흐름 중 `Theme leadership/state/role classification -> Watchset/Candidate source observation`까지의 migration map이다.

## 3. 시장/테마 엔진 구성 요소

### 3.1 주도 테마 생성 방식

suseok_ai의 주도 테마는 조건검색 hit 목록만으로 만들지 않았다. 주도 테마 생성의 중심은 다음 조합이다.

| 입력 | 역할 |
| --- | --- |
| Naver theme universe | theme name, detail link, stock membership, reason text를 제공한다. |
| Kiwoom realtime tick | 현재가, 등락률, 거래량, 거래대금, 체결강도, 호가, momentum, stale 여부를 제공한다. |
| `opt10032` turnover seed | 장초반/장중 거래대금 seed로 active universe를 넓힌다. |
| condition include/remove | discovery source 또는 boost로만 사용한다. 핵심 매수 판단이 아니다. |
| manual watch, holding, pending order | 관찰 universe를 보강하는 seed source다. |

주도 테마 판단은 대략 다음 순서로 이뤄졌다.

1. Theme membership을 읽어 theme별 구성 종목 universe를 만든다.
2. realtime tick과 seed signal을 종목별 snapshot으로 합친다.
3. theme별 cohort metric을 계산한다.
   - strong/leader count
   - alive/strong/leader ratio
   - turnover, turnover share, weighted return
   - breadth, fresh coverage
   - leader concentration
   - leader-only 여부
4. state machine이 theme state를 분류한다.
   - `DATA_WAIT`
   - `WATCH_THEME`
   - `EMERGING_THEME`
   - `LEADER_ONLY_THEME`
   - `SPREADING_THEME`
   - `LEADING_THEME`
   - `FADING_THEME`
   - `WEAK_THEME`
5. leadership handover가 cycle 단위 spike를 줄이고 주도 테마 교체를 확인한다.
6. stock role classifier가 leader, co-leader, follower, laggard, overheated를 분리한다.

가져올 핵심은 이 흐름의 "realtime theme leadership selection"이다. v2에는 단순 theme snapshot이 이미 있으므로, PR-2는 이를 `RT-TLS` service로 확장하는 방향이 맞다.

### 3.2 Theme membership 구성 방식

suseok_ai의 theme membership은 다음 계층으로 구성됐다.

| 구성 요소 | 역할 |
| --- | --- |
| `NaverThemeUniverseSource` | Naver theme list/detail page를 읽어 theme과 member stock evidence를 만든다. |
| `ThemeCanonicalResolver` | source theme name을 canonical theme으로 normalize/resolve한다. |
| `ThemeEvidenceService` | source theme과 member evidence를 저장한다. |
| `ThemeMembershipBuilder` | evidence confidence, relation type, source bonus, freshness를 조합해 membership score를 만든다. |
| `ThemeUniverseBuilder` | active/trade-eligible theme universe snapshot을 만든다. |

Naver source의 사용 범위가 중요하다.

- Naver는 theme name, detail link, 종목 membership, 관련 reason text를 얻는 reference source다.
- Naver의 등락률, 거래량, 거래대금, 화면상 주도주 순위는 realtime 판단에 사용하지 않는 것이 원칙이다.
- realtime scoring/classification은 Kiwoom realtime price tick과 Core projection에서 계산한다.

v2에 가져올 때는 Naver crawler 자체를 PR-2에 넣기보다, `ThemeUniverseProvider` interface를 먼저 만들고 현재 v2 `theme_members`/`theme_latest_snapshots`를 input으로 쓰는 것이 적절하다. Naver importer는 별도 PR로 분리 가능하다.

### 3.3 조건검색의 역할

suseok_ai에서 조건검색은 core buy decision이 아니라 discovery/boost다.

확인된 원칙은 다음이다.

- condition include는 active seed source가 될 수 있다.
- condition include는 candidate source event가 될 수 있다.
- condition name/profile은 source attribution과 boost 수준에만 사용한다.
- condition include 단독으로 `READY`, `EntryPlan`, `OrderIntent`, Gateway order command를 만들면 안 된다.
- condition profile이 비어 있어도 `opt10032 seed + realtime snapshot + theme membership`만으로 Theme Core가 동작해야 한다.

v2 이식 원칙도 동일해야 한다. 조건검색은 "발견했다" 또는 "관찰 우선순위를 올린다"는 신호이지, "매수 가능"을 뜻하지 않는다.

### 3.4 Realtime snapshot

suseok_ai의 `LiveSeedSignal`과 realtime snapshot은 다음 속성을 포함했다.

- `source_type`: `OPT10032`, `CONDITION_INCLUDE`, `MANUAL_WATCH`, `HOLDING`, `PENDING_ORDER`, `REALTIME_TICK`, `HYDRATION`
- `change_rate_pct`
- `turnover_krw`
- `turnover_speed`
- `execution_strength`
- `market`
- `momentum_1m`, `momentum_3m`, `momentum_5m`
- `realtime_valid`, `tr_backfill_valid`
- `freshness`, `data_quality`
- `vi_active`, `upper_limit_near`, `overheated`

v2에는 `MarketDataService`가 tick, condition, TR response를 projection하고, `ThemeService`가 latest tick/readiness/bar/VWAP를 읽어 theme snapshot을 만든다. PR-2는 새 raw data path를 만들기보다 이 projection을 읽는 `RealtimeSnapshotBuilder` interface를 추가하는 것이 맞다.

### 3.5 Theme state와 stock role

suseok_ai의 장점은 theme state와 stock role을 분리한 점이다.

Theme state의 의미는 다음처럼 가져온다.

| suseok_ai state | v2 최소 표현 | 의미 |
| --- | --- | --- |
| `DATA_WAIT`, `SEED_WAIT`, `UNIVERSE_EMPTY` | `DATA_WAIT` | 판단 재료 부족. 후보 폐기 아님. |
| `WATCH_THEME`, `EMERGING_THEME` | `WATCH` | 관심 유지, 아직 watchset 핵심 아님. |
| `LEADER_ONLY_THEME` | 신규 세부 상태 또는 metadata | 단일 리더만 강함. follower 확산 매수는 금지. |
| `SPREADING_THEME` | `SPREADING` | 다수 구성원이 동반 상승/거래대금 확산. |
| `LEADING_THEME` | `LEADING` | 지속성 있는 주도 테마. |
| `FADING_THEME`, `WEAK_THEME` | `FADING` 또는 `WATCH/blocked reason` | 신규 watchset 제외 또는 낮은 우선순위. |

Stock role의 의미는 다음처럼 가져온다.

| suseok_ai role | v2 최소 표현 | watchset 정책 |
| --- | --- | --- |
| `LEADER_CONFIRMED` | `LEADER_CANDIDATE` | `LEADING/SPREADING/LEADER_ONLY`에서 우선 선별 |
| `CO_LEADER_CONFIRMED` | `CO_LEADER_CANDIDATE` | 리더 다음 우선순위 |
| `FOLLOWER_ALLOWED` | `FOLLOWER_CANDIDATE` | `SPREADING/LEADING`이고 market phase가 확산일 때만 |
| `FOLLOWER_BLOCKED_LEADER_ONLY` | metadata/block reason | `LEADER_ONLY`에서는 follower 제외 |
| `LATE_LAGGARD_BLOCKED` | `LAGGARD` | watchset 제외 |
| `OVERHEATED_BLOCKED` | `STALE` 또는 block reason | watchset 제외 |

## 4. 후보 생성/watchset 구성 요소

### 4.1 candidate/watchset 생성 흐름

suseok_ai의 후보 생성은 조건검색 단일 진입이 아니라 여러 source를 하나의 candidate instance로 합치는 구조였다.

```text
ActiveSeedRegistry
  -> ThemeCoreV3RuntimePipeline
  -> FocusedExpansionPlanner
  -> CandidateBridge / CandidateBridgeReconciler
  -> CandidateIngestionService
  -> Candidate FSM / Hydrator / Subscription Readiness
```

각 구성 요소의 역할은 다음이다.

| 구성 요소 | 역할 | v2 이식 방향 |
| --- | --- | --- |
| `ActiveSeedRegistry` | realtime tick, opt10032, condition, manual, holding seed를 TTL과 함께 관리 | v2 MarketData projection과 Candidate source를 읽는 input adapter로 재설계 |
| `ThemeCoreV3RuntimePipeline` | theme membership과 seed signal을 결합해 board/state/role 생성 | v2 `ThemeLeadershipService`로 축소 port |
| `FocusedExpansionPlanner` | theme별 realtime subscription/watch 대상 확장 | PR-2에서는 watchset selector로만 구현 |
| `CandidateBridge` | Theme Core output을 candidate source event로 변환 | v2 `CandidateSourceEvent` 생성 interface로 재설계 |
| `CandidateIngestionService` | source별 candidate create/merge/remove | v2 `services/candidate_service.py` 경계 사용 |

### 4.2 후보 종목 lifecycle

suseok_ai의 candidate lifecycle은 관찰 episode와 주문 lifecycle이 섞여 있었으나, observe-only 계약에서는 다음처럼 정리된다.

| lifecycle 단계 | 의미 | v2 대응 |
| --- | --- | --- |
| `SOURCE_DETECTED` | condition/theme/opening/manual source가 관찰됨 | `CandidateSourceEvent` |
| `CANDIDATE_CREATED` | trade_date/code/generation 기반 candidate instance 생성 | `DETECTED` |
| `HYDRATING` | TR/기본정보 보강 대기 | `HYDRATING` |
| `WAIT_DATA` / `DATA_WAIT` | realtime tick, hydration, theme context, readiness 부족 | `DATA_WAIT` |
| `WATCHING` | 관찰 유지 가능 | `WATCHING` |
| `CONTEXT_READY` 또는 observe-ready | Strategy 관찰 입력 준비 | `CONTEXT_READY` |
| `READY` / `ORDER_DECIDED` / `ORDER_SENT` | legacy/live-sim order path | PR-2 제외 |
| `EXPIRED` / `REMOVED` / `CLOSED` | source 제거, TTL 만료, stale 정리 | `STALE` 또는 `CLOSED` |

v2에서는 "candidate"를 매수 후보가 아니라 관찰 episode로 유지해야 한다. `CONTEXT_READY`도 매수 가능 상태가 아니라 Strategy/Risk 관찰 입력이 준비됐다는 뜻이다.

### 4.3 매수 가능 상태와 관찰 상태의 분기

suseok_ai에서 관찰 상태와 매수 가능 상태는 다음 지점에서 갈렸다.

- Theme Core/CandidateBridge: `ready_allowed=false`, `order_intent_allowed=false`인 observe-only candidate source를 만들 수 있다.
- Candidate FSM: active source, hydration, realtime freshness가 있으면 관찰/평가 가능 상태로 이동한다.
- EntryEngine/SetupRouter: price location/setup이 맞으면 observe-ready 성격의 decision을 만들 수 있다.
- OrderManager/OrderEnqueue: config, broker, risk, lifecycle/reconcile, LIVE_SIM guard를 통과해야 command로 이어질 수 있다.

v2의 PR-2에서는 첫 번째와 두 번째만 다룬다. 즉 watchset과 candidate source까지가 범위이며, 매수 가능 상태 또는 주문 생성은 만들지 않는다.

### 4.4 watchset selection 원칙

PR-2의 watchset은 다음 기준으로 최소화한다.

- theme state는 `LEADING`, `SPREADING`, 제한적 `LEADER_ONLY`만 우선한다.
- `LEADER_ONLY`에서는 leader/co-leader만 포함하고 follower는 제외한다.
- theme별 최대 3개, 전체 최대 20개 또는 30개로 제한한다.
- stale tick, VI, upper-limit-near, overheated, late laggard는 제외하거나 낮은 우선순위로 둔다.
- condition include는 boost factor로만 반영한다.
- `DATA_WAIT`는 hard delete가 아니라 reason code와 함께 watch 보류 상태로 남긴다.

## 5. 진입 타이밍 구성 요소

suseok_ai의 EntryEngine은 price location을 다음처럼 분류했다.

- `GOOD_PULLBACK`
- `PULLBACK_RECLAIM`
- `VWAP_RECLAIM`
- `BREAKOUT_CONTINUATION`
- `CHASE_HIGH`
- `VWAP_OVEREXTENDED`
- `FAILED_BREAKOUT`
- `DEEP_PULLBACK`
- `UNKNOWN`
- `DATA_WAIT`

이번 migration에서 가져올 최소 EntryTiming은 다음 세 가지다.

| EntryTiming | 의미 | v2 사용 방향 |
| --- | --- | --- |
| `GOOD_PULLBACK` | 강한 테마/역할 안에서 과열 추격이 아닌 정상 눌림 | Strategy observe input 또는 watchset annotation |
| `PULLBACK_RECLAIM` | 눌림 이후 기준 가격/단기 저항 회복 | Strategy observe input |
| `VWAP_RECLAIM` | VWAP 하회 후 회복 | Strategy observe input |

단, PR-2는 RT-TLS port가 목적이므로 EntryTiming은 interface/model 또는 watchset annotation 수준으로 제한한다. 주문 의사결정, position sizing, LIVE_SIM 자동 intent 생성은 하지 않는다.

SetupRouter V3에서 참고할 setup은 다음이다.

- `LEADER_FIRST_PULLBACK`
- `VWAP_RECLAIM`
- `BREAKOUT_RETEST`

가져올 원칙은 "setup 관찰"이지 "주문 판단"이 아니다. v2 `Strategy Engine`의 기존 observe-only 경계에 맞춰, `MATCHED_OBSERVATION`은 매수 신호가 아니라고 유지한다.

## 6. risk/order/exit 구성 요소

### 6.1 order intent와 idempotency

suseok_ai에는 stable idempotency key 기반 중복 주문 방지 원칙이 여러 계층에 있었다.

| 영역 | 예시 |
| --- | --- |
| Entry intent | `buy:{trade_date}:{candidate_id}:{code}:{bucket}:{price_bucket}` |
| Exit intent | `reboot_live_sim_sell:{trade_date}:{position_id}:{exit_reason}:{bucket}` |
| Runtime LIVE_SIM entry | `runtime:livesim:{phase}:{trade_date}:{account}:{candidate_instance_id}:...:{code}:{price}:{quantity}` |
| Cancel | `runtime:livesim:cancel:{trade_date}:{account}:{code}:{broker_order_id}:{original_order_id}:{cancel_qty}:{cancel_reason}` |
| Gateway command store | command id와 dedupe key를 저장하고 duplicate rejected event를 남김 |

v2에는 이미 `services/live_sim/live_sim_service.py`에서 `live_sim:{trade_date}:{code}:{candidate_instance_id}:{strategy_observation_id}:{risk_observation_id}` 형태의 idempotency key와 command payload `live_real_allowed=false`가 있다. PR-2에서는 주문을 만들지 않지만, 향후 LIVE_SIM 후보 수동 전환을 위해 다음 원칙만 문서화한다.

- idempotency key는 trade_date, code, candidate_instance_id, theme/setup/risk observation identity를 포함한다.
- price/quantity를 key에 넣을지는 order layer에서만 결정한다.
- watchset/candidate source 단계는 order idempotency가 아니라 source event idempotency를 쓴다.

### 6.2 risk/data gate 흐름

suseok_ai의 risk gate는 보수적이었다. 안전 측면에서 가져올 원칙과, 무매수/복잡도를 키울 수 있어 버릴 구조를 분리해야 한다.

가져올 safety 원칙:

- real broker 차단.
- simulation broker/account 확인.
- kill switch.
- stale heartbeat/gateway unhealthy 차단.
- duplicate pending order/open position 차단.
- daily order/code/theme exposure limit.
- reconcile required 상태에서 신규 buy 차단.
- VI, upper-limit-near, stale tick, chase risk, excessive spread 차단 또는 보류.

그대로 가져오면 안 되는 구조:

- `DATA_INSUFFICIENT`, `INDICATOR_DATA_INSUFFICIENT`, `INDEX_DATA_INSUFFICIENT`를 모든 후보의 final gate hard block으로 합치는 정책.
- support readiness, latest tick readiness, market breadth readiness를 하나의 `DATA_INSUFFICIENT`로 뭉개는 정책.
- full theme coverage 부족을 곧바로 `DATA_WAIT` hard block으로 처리하는 정책.
- review report/shadow small-entry promotion 상태를 runtime gate에 과도하게 섞는 정책.
- Entry/Risk/Order lifecycle reason code가 너무 많아져 watch/ready/order 불가 사유가 한 단계에서 해석되지 않는 구조.

v2에서는 `DATA_WAIT`를 "관찰 계속, 주문 금지"로 유지하고, reason code를 category별로 나눠야 한다. `DATA_WAIT`가 watchset에서 삭제 또는 전역 hard block으로 번역되면 안 된다.

### 6.3 order lifecycle

suseok_ai의 order lifecycle은 다음 계층을 거쳤다.

```text
Entry/Exit decision
  -> ManagedOrderIntent
  -> OrderRiskDecision
  -> ManagedOrder
  -> GatewayCommand(send_order/cancel_order)
  -> command_started/command_ack/command_failed
  -> chejan/reconcile
```

v2 PR-2에서는 이 계층을 구현하지 않는다. 다만 "stable idempotency key", "Gateway command queue에서 dedupe", "Core가 order state를 저장"이라는 원칙은 future LIVE_SIM 수동 흐름에 이미 맞아 있다.

### 6.4 exit/reconcile

suseok_ai의 exit/reconcile 기능은 다음을 포함했다.

- stop loss
- take profit
- max hold
- 장마감 청산
- 미체결 cancel
- pending cancel/new buy block
- unknown submit/reconcile required block
- external position 감지 시 신규 buy block

v2 PR-2에는 exit/reconcile 이식이 필요하지 않다. 다만 watchset 이후의 LIVE_SIM 수동 운영이 늘어날 때, reconcile required가 신규 buy를 막는 원칙은 유지해야 한다.

### 6.5 dashboard/review/shadow/A-B/report 계열

suseok_ai의 dashboard/review/report 계열은 운영 가시성 측면에서 많은 정보를 제공했지만, PR-2 이식 대상은 아니다.

버릴 또는 보류할 항목:

- threshold A/B machinery.
- shadow small-entry report 중심 promotion 구조.
- hybrid ready canary.
- review-only panel 과다 확장.
- dashboard debug panel 난립.
- legacy ThemeLab/hybrid score/final grade를 primary panel로 노출하는 구조.

v2에는 필요한 summary만 나중에 추가한다. PR-2는 API/model/test 중심으로 끝내고 dashboard 확장은 하지 않는 것이 좋다.

## 7. 가져올 기능 목록

| 기능 | suseok_ai 근거 | v2 이식 형태 | PR-2 포함 |
| --- | --- | --- | --- |
| Theme universe | Naver source, canonical resolver, evidence/membership builder | `ThemeUniverseProvider` interface와 기존 v2 theme_members adapter | 부분 포함 |
| Theme membership | relation/confidence/source/freshness 기반 active membership | v2 `ThemeMembership` metadata/weight 확장 방향 문서화 | 부분 포함 |
| Realtime snapshot | `LiveSeedSignal`, realtime tick, opt10032, condition/manual/holding seed | `RealtimeSnapshotBuilder`가 v2 MarketData projection을 읽음 | 포함 |
| Theme leadership ranking | cohort, turnover flow, leadership handover | `ThemeLeadershipRanker` 또는 `RTTLSRanker` | 포함 |
| Theme state classification | `DATA_WAIT`, `WATCH`, `LEADER_ONLY`, `SPREADING`, `LEADING` | v2 state + metadata/reason code | 포함 |
| Stock role classification | leader/co-leader/follower/laggard/overheated | v2 `ThemeMemberRole`에 mapping | 포함 |
| Watchset selection | Focused expansion, CandidateBridge | `WatchsetSelector`와 candidate source event adapter | 포함 |
| Condition discovery/boost | condition include source event, seed boost | condition은 boost/source attribution만 | 포함 |
| EntryTiming 최소 enum | `GOOD_PULLBACK`, `PULLBACK_RECLAIM`, `VWAP_RECLAIM` | Strategy observe annotation/interface | 모델 또는 문서만 |
| Stable idempotency key 원칙 | order manager, runtime enqueue, gateway dedupe | source event/order intent identity 설계 원칙 | 문서만 |
| Candidate lifecycle 최소 | source detected, hydration, watching, context ready | 기존 v2 Candidate FSM 사용 | 포함 |

## 8. 버릴 기능 목록

| 버릴 기능 | 이유 | v2 대체 |
| --- | --- | --- |
| legacy hybrid final grade 중심 구조 | theme/role/timing/risk가 final grade 하나로 섞이면 판단 경계가 흐려진다. | Theme/Strategy/Risk observe를 분리한다. |
| threshold A/B machinery | PR-2 목적보다 복잡하고 runtime 판단을 흐린다. | deterministic threshold와 테스트 fixture만 둔다. |
| shadow small-entry report 중심 구조 | report/promotion이 order gate와 섞이면 운영 복잡도가 커진다. | LIVE_SIM은 기존 v2 manual safety gate만 사용한다. |
| 조건검색 이름 의존 매수 판단 | 조건검색은 discovery/boost일 뿐 핵심 판단이 아니다. | condition source type과 boost metadata만 저장한다. |
| review-only panel 과다 확장 | migration 핵심이 아니라 UI/debug 노이즈를 늘린다. | PR-2는 service/model/test 중심으로 제한한다. |
| dashboard debug panel 난립 | 운영 화면이 primary 판단과 debug를 구분하지 못하게 된다. | summary metric만 별도 future PR에서 검토한다. |
| `DATA_INSUFFICIENT` 일괄 hard block | 후보 삭제/무매수 위험을 키운다. | category별 `DATA_WAIT` reason으로 유지한다. |
| AI 직접 주문 의사결정 | v2 원칙과 맞지 않는다. | AI는 advisory-only, review-only다. |
| LIVE_REAL 관련 구현 | 현재 roadmap 범위 밖이고 safety project가 따로 필요하다. | 구현하지 않는다. |
| suseok_ai legacy code 복사 | v2 Core/Gateway/Service 경계와 맞지 않는다. | interface와 deterministic service로 재설계한다. |

## 9. v2 신규 service/interface mapping

### 9.1 경계 원칙

v2 경계는 다음과 같이 유지한다.

| 계층 | 담당 | 금지 |
| --- | --- | --- |
| Gateway | Kiwoom login, realtime/TR/condition/chejan 수신, Core command polling, command execution | 판단/정책/상태 저장, AI 판단, 자체 매수 판단 |
| Core service | market/theme/candidate/strategy/risk/live_sim 상태 저장과 deterministic 판단 | Kiwoom 직접 호출, QAxWidget import |
| AI Sidecar | advisory-only review, RCA draft, prompt draft | Risk/OMS/LIVE_SIM 자동 입력 |
| LIVE_SIM | simulation account only, manual/safety-gated path | 자동 pipeline, LIVE_REAL |

### 9.2 suseok_ai -> v2 mapping

| suseok_ai 구성 요소 | v2 신규/기존 service | 설명 |
| --- | --- | --- |
| `ThemeUniverseBuilder`, `ThemeMembershipBuilder` | `services/theme_leadership/universe_provider.py` | 기존 v2 `themes/theme_members`를 읽는 provider부터 시작한다. |
| `LiveSeedSignal`, `ActiveSeedRegistry` | `services/theme_leadership/realtime_snapshot.py` | MarketData latest tick/bar/readiness와 condition source metadata를 snapshot으로 합친다. |
| `ThemeCohortEngine` | `services/theme_leadership/cohort.py` | fresh coverage, rising ratio, leader count, turnover delta를 계산한다. |
| `ThemeStateMachine` | `services/theme_leadership/state_classifier.py` | `DATA_WAIT/WATCH/LEADER_ONLY/SPREADING/LEADING/FADING` 분류. |
| `ThemeLeadershipRanker`, handover | `services/theme_leadership/ranker.py` | PR-2에서는 persistence가 간단한 ranking부터, handover는 metadata 또는 후속 PR로 둔다. |
| `StockRoleEngine` | `services/theme_leadership/stock_role.py` | leader/co-leader/follower/laggard/overheated mapping. |
| `FocusedExpansionPlanner` | `services/theme_leadership/watchset.py` | theme별/전체 limit을 가진 watchset selector. |
| `CandidateBridge` | 기존 `services/candidate_service.py` adapter | watchset item을 `CandidateSourceEvent`로 변환한다. |
| `EntryEngine` | 기존 `services/strategy_engine.py` | PR-2에서는 EntryTiming enum/annotation만 연결한다. |
| `OrderRiskManager`, order enqueue | 기존 `services/risk_gate.py`, `services/live_sim/*` | PR-2 범위 밖. 원칙만 문서화한다. |

### 9.3 최소 domain/interface 모델

PR-2에서 필요한 최소 모델은 다음이다.

```text
ThemeUniverseItem
  theme_id
  theme_name
  members[code, name, weight, source_type, metadata]

RealtimeStockSnapshot
  code
  price
  change_rate
  cumulative_trade_value
  trade_value_delta_1m/3m/5m
  execution_strength
  vwap
  tick_age_sec
  readiness_status
  condition_boost
  flags[vi_active, upper_limit_near, overheated, stale]

ThemeLeadershipSnapshot
  theme_id
  state
  leadership_rank
  score
  fresh_coverage_ratio
  rising_ratio
  leader_code
  co_leader_codes
  reason_codes

WatchsetItem
  trade_date
  code
  theme_id
  theme_state
  stock_role
  rank
  score
  entry_timing_hint
  source_event_id
  reason_codes
```

State/role enum은 v2 기존 값과 호환되게 시작한다.

- Theme state: `DATA_WAIT`, `WATCH`, `LEADER_ONLY`, `SPREADING`, `LEADING`, `FADING`
- Stock role: `LEADER_CANDIDATE`, `CO_LEADER_CANDIDATE`, `FOLLOWER_CANDIDATE`, `LAGGARD`, `STALE`, `OVERHEATED`
- EntryTiming hint: `GOOD_PULLBACK`, `PULLBACK_RECLAIM`, `VWAP_RECLAIM`, `DATA_WAIT`, `NONE`

`LEADER_ONLY`와 `OVERHEATED`가 v2 기존 enum에 없으면 PR-2에서는 schema 확장 또는 metadata reason으로 처리한다. 대규모 migration을 피하려면 metadata/reason으로 시작하고 후속 PR에서 enum 확장을 검토한다.

### 9.4 조건검색 interface

조건검색은 다음 필드로만 사용한다.

- `condition_source_present`
- `condition_names`
- `condition_boost_score`
- `condition_last_event_ts`
- `condition_reason_codes`

조건검색이 없어도 theme leadership ranking은 동작해야 한다. 조건검색만 있고 theme membership/realtime snapshot이 없으면 candidate source는 만들 수 있지만 watchset 핵심 후보로 승격하지 않는다.

### 9.5 DATA_WAIT 정책

v2의 최소 정책은 다음이다.

- `DATA_WAIT`는 hard block이 아니라 "판단 보류"다.
- `DATA_WAIT`는 reason category를 유지한다.
  - membership 부족
  - realtime tick 없음
  - tick stale
  - fresh coverage 부족
  - TR backfill only
  - candle/VWAP 부족
  - market data projection 오류
- `DATA_WAIT` theme은 order/livesim과 연결하지 않는다.
- `DATA_WAIT` stock은 watchset에서 제외하거나 낮은 우선순위 보류 bucket에 둔다.
- `DATA_WAIT`를 `WEAK_THEME`로 자동 강등하지 않는다.

## 10. PR-2 구현 범위 제안

PR-2 제목은 다음처럼 잡는 것이 적절하다.

`PR-2. Minimal RT-TLS Port: Theme Leadership Snapshot & Watchset`

### 10.1 포함 범위

1. Domain model 추가 또는 확장
   - Theme leadership snapshot DTO.
   - Realtime stock snapshot DTO.
   - Watchset item DTO.
   - EntryTiming hint enum은 주문과 분리된 annotation으로만 둔다.

2. Universe provider
   - 기존 v2 `themes`, `theme_members`를 읽는다.
   - Naver crawler는 구현하지 않는다.
   - source metadata에 `NAVER_REFERENCE`, `IMPORTED`, `MANUAL`, `CONDITION_DERIVED`를 보존한다.

3. Realtime snapshot builder
   - v2 `market_latest_ticks`, `market_minute_bars`, readiness를 읽는다.
   - condition latest signal은 boost metadata로만 읽는다.
   - TR backfill only는 `DATA_WAIT` reason으로 남긴다.

4. Theme leadership ranker/state classifier
   - fresh coverage, rising ratio, leader score, turnover delta를 사용한다.
   - `LEADER_ONLY`, `SPREADING`, `LEADING`, `WATCH`, `DATA_WAIT`를 분리한다.
   - single leader spike는 follower 확산으로 보지 않는다.

5. Stock role classifier
   - leader/co-leader/follower/laggard/stale/overheated를 분리한다.
   - `LEADER_ONLY`에서는 follower를 제외한다.

6. Watchset selector
   - theme별 최대 3개, 전체 최대 20-30개 제한.
   - `LEADING/SPREADING` 우선, `LEADER_ONLY`는 leader/co-leader만.
   - condition boost는 tie-breaker 또는 score bonus만.

7. Candidate source adapter
   - watchset item을 기존 v2 `CandidateSourceEvent`로 변환한다.
   - Candidate FSM state는 기존 `DETECTED -> HYDRATING -> WATCHING/CONTEXT_READY/DATA_WAIT` 흐름을 사용한다.
   - `OrderIntent`, `GatewayCommand`, `send_order`는 생성하지 않는다.

8. 테스트
   - deterministic fixture로 theme membership과 realtime snapshot을 구성한다.
   - 조건검색 없음에도 theme leadership이 계산되는 테스트.
   - condition include 단독으로 watchset core 후보가 되지 않는 테스트.
   - `LEADER_ONLY`에서 follower가 제외되는 테스트.
   - stale tick/TR backfill only가 `DATA_WAIT` reason으로 남는 테스트.
   - `DATA_WAIT`가 `WEAK_THEME`로 자동 강등되지 않는 테스트.
   - watchset limit과 idempotent source_event_id 테스트.

### 10.2 제외 범위

- `LIVE_SIM_AUTO_PIPELINE`.
- `LIVE_REAL`.
- AI 판단/주문 기능.
- Naver crawler 신규 구현.
- order intent/order manager 변경.
- exit/reconcile 구현.
- threshold A/B, shadow report, hybrid final grade.
- dashboard panel 확장.
- suseok_ai legacy code 복사.

### 10.3 PR-2 Definition of Done

- v2 Core 안에 RT-TLS 최소 service/interface가 생긴다.
- 기존 Theme Service와 Candidate FSM 경계를 침범하지 않는다.
- Gateway 변경이 없거나, 있어도 command/order path와 무관한 projection read만 사용한다.
- condition search는 discovery/boost로만 쓰인다.
- watchset은 deterministic하게 재생 가능하다.
- `DATA_WAIT`는 category별 reason으로 남고 hard delete/final block으로 번역되지 않는다.
- 주문 관련 output은 생성되지 않는다.
- 테스트가 "조건검색 없이도 theme membership + realtime snapshot으로 watchset 생성 가능"을 증명한다.

### 10.4 PR-2 이후 순서 제안

| 후속 PR | 내용 |
| --- | --- |
| PR-3 | EntryTiming observe integration: `GOOD_PULLBACK`, `PULLBACK_RECLAIM`, `VWAP_RECLAIM`을 Strategy observation에 연결 |
| PR-4 | Naver theme universe importer 또는 suseok_ai membership export importer |
| PR-5 | Theme leadership persistence/handover 강화 |
| PR-6 | LIVE_SIM manual candidate review UX 정리, 자동 pipeline 제외 |

이 순서는 먼저 "장중 무엇을 볼 것인가"를 안정화하고, 이후 "어떤 setup으로 관찰할 것인가"를 붙이는 흐름이다. 주문 자동화는 이 migration map의 목표가 아니다.
