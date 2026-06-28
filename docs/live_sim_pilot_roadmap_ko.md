# suseok-trader-v2 LIVE_SIM Pilot Roadmap

> 목적: `suseok_ai`의 실전 자동매매 아이디어 중 **시장/테마 판단력, 후보 생성, 진입 타이밍, 주문 lifecycle**은 살리고, 무매수와 복잡도를 키운 **legacy/hybrid/A-B/shadow/review-only 과다 구조**는 제외하여 `suseok-trader-v2`를 키움 모의투자 전용 실전 자동매매 v2로 단계 전환한다.

이 문서는 Codex 작업을 한 번에 크게 던지지 않기 위한 기준 문서다. 각 PR은 아래 순서와 범위를 지킨다.

---
## v2의 목표 구조

[08:00~08:50 / 장전 준비]
  네이버 테마 membership 갱신
  전일/최근 테마 지속성 계산
  관심 universe 압축
  Kiwoom 실시간 등록 후보 준비

[09:00~장중 / 실시간 루프]
  Kiwoom tick / 체결강도 / 거래대금 / 호가 / 1m·3m·5m / VWAP 수집
      ↓
  ThemeLeadershipRanker
      ↓
  StockRoleClassifier
      ↓
  WatchsetSelector
      ↓
  EntryTimingEngine
      ↓
  TradeSetupClassifier
      ↓
  RiskGate
      ↓
  AI Candidate Scorer, 선택적
      ↓
  OrderPlanBuilder
      ↓
  LIVE_SIM OrderIntent
      ↓
  Gateway send_order command
      ↓
  체결/미체결/취소/청산/리컨실
  
---

---

## PR-3 EntryTiming / OrderPlanDraft 업데이트

PR-3는 주문 PR이 아니다. ThemeLeadership watchset과 candidate/strategy/risk 관측 결과를 `EntryTimingInput`으로 모아 가격 위치를 분류하고, PR-4 LIVE_SIM safety-gated path가 읽을 수 있는 `OrderPlanDraft` 초안만 만든다.

`OrderPlanDraft`는 OrderIntent가 아니며 `PLAN_READY`도 주문 승인이나 매수 신호가 아니다. `MATCHED_OBSERVATION`과 `OBSERVE_PASS`는 계속 observe-only 근거로만 사용한다. VWAP warmup, momentum warmup, observation 미생성은 `DATA_WAIT` 또는 `WAIT_RETRY` near-miss로 남긴다.

운영 확인은 `GET /api/entry-timing/plans/latest` 또는 `python -m tools.evaluate_entry_timing`으로 수행한다. LIVE_SIM 자동 주문 연결과 safety gate 최종 판정은 PR-4 범위다.

---

## PR-3.5 Naver Theme Importer 업데이트

PR-3.5는 네이버 테마를 theme membership reference source로 가져오는 작업이다. 네이버 theme universe와 구성 종목은 `NAVER_REFERENCE/naver_theme` membership으로 저장되지만, 네이버 등락률/순위는 metadata로만 남긴다.

장중 주도 테마 판단, 대장주 판단, 매수 후보 판단은 계속 Kiwoom realtime tick, MarketData projection, ThemeLeadershipService가 담당한다. 네이버 theme hit만으로 `LEADING`, `READY`, `PLAN_READY`, OrderIntent, GatewayCommand를 만들지 않는다.

운영 확인:

```powershell
python -m tools.import_naver_themes --dry-run --limit-themes 20
python -m tools.import_naver_themes --limit-themes 20
python -m tools.inspect_theme_leadership
```

네이버 importer 실패 또는 empty fetch는 기존 membership을 삭제하지 않는다. `--replace`는 명시 실행 시에만 사용하고, 같은 `NAVER_REFERENCE/naver_theme` scope에서만 inactive 처리한다. PR-4 LIVE_SIM 주문 파이프라인과 직접 연결하지 않는다.

---

## PR-4 LIVE_SIM Pilot Pipeline 업데이트

PR-4는 `order_plan_drafts_latest`의 `PLAN_READY` 초안을 LIVE_SIM 모의투자 전용 safety gate로 다시 검증해 `LiveSimIntent`와 선택적 `GatewayCommand(send_order)`를 만드는 run_once 파일럿이다.

핵심 원칙:

- LIVE_REAL은 구현하지 않는다.
- `PLAN_READY`는 주문 승인이나 매수 신호가 아니다.
- OrderPlanDraft 없이 candidate만으로 자동 주문을 만들지 않는다.
- `DATA_WAIT`, `WAIT_RETRY`를 강제로 `PLAN_READY`로 승격하지 않는다.
- market order, SELL, cancel, modify는 PR-4 범위가 아니다.
- scheduler는 PR-5 이후로 남긴다.

운영 순서:

```powershell
python -m tools.import_naver_themes --dry-run --limit-themes 20
python -m tools.import_naver_themes --limit-themes 20
python -m tools.inspect_theme_leadership
python -m tools.evaluate_entry_timing
python -m tools.run_live_sim_pilot_once
python -m tools.run_live_sim_pilot_once --queue-commands
```

command queue는 `TRADING_PROFILE=LIVE_SIM_PILOT`, `LIVE_SIM_PILOT_PIPELINE_ENABLED=true`, `LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=true`, `LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=true`, 기존 LIVE_SIM safety gate가 모두 통과한 경우에만 가능하다.

자세한 구현/운영 문서는 [LIVE_SIM Pilot Pipeline from OrderPlanDraft](live_sim_pilot_pipeline_ko.md)를 따른다.

---

## 0. 이번 로드맵의 전제

이번 로드맵의 출발점은 `suseok-trader-v2` 장중 실행 결과 분석이 아니다. 아직 v2로 장을 충분히 돌린 상태가 아니기 때문이다.

따라서 PR-1의 목적은 v2 런타임 RCA가 아니라, **`suseok_ai`에서 무엇을 가져오고 무엇을 버릴지 코드/문서 기준으로 선별하는 것**이다.

핵심 질문은 다음이다.

```text
suseok_ai에서 실제 장중 자동매매 흐름을 만들던 구성 요소는 무엇인가?
그중 v2에 가져와야 할 실전 기능은 무엇인가?
무매수와 복잡도를 만든 기능은 무엇이라 제외해야 하는가?
v2의 안전한 Core/Gateway 경계 안에서 어떻게 재구성할 것인가?
```

v2의 최종 목표 흐름은 다음이다.

```text
Kiwoom Gateway event
  -> MarketData projection
  -> Theme leadership
  -> Candidate/Watchset
  -> Entry timing
  -> Strategy/Risk decision
  -> Order plan
  -> LIVE_SIM intent
  -> Gateway send_order command
  -> Fill/position/exit/reconcile
```

---

## 1. 절대 유지할 원칙

### 1.1 LIVE_REAL 금지

- `LIVE_REAL`은 이번 로드맵 범위에서 구현하지 않는다.
- 실계좌/실서버 감지 시 주문은 hard block한다.
- `TRADING_ALLOW_LIVE_REAL=true` 또는 유사 설정은 테스트에서도 실패해야 한다.

### 1.2 Gateway/Core 경계 유지

- Gateway는 Kiwoom 통신, 이벤트 수집, command polling만 담당한다.
- Core는 상태 저장, 판단, 정책, API, dashboard를 담당한다.
- Core 내부에 PyQt5, QAxWidget, Kiwoom concrete client import를 추가하지 않는다.

### 1.3 AI는 advisory only

- AI/GPT는 후보 점수화, 리스크/손익비 제안, 요약만 수행한다.
- AI/GPT는 `send_order`, `cancel_order`, `modify_order`, `order_intent`, `gateway_command`를 직접 생성할 수 없다.
- AI 결과는 JSON schema 검증, threshold, 코드 risk gate를 통과해야만 참고된다.

### 1.4 무매수 방지와 계좌 보호를 동시에 만족

- 무매수를 막기 위해 모든 데이터 부족을 hard block으로 처리하지 않는다.
- 단, 계좌/주문 안전성과 관련된 조건은 반드시 hard block한다.
- `DATA_WAIT`, `WARMUP`, `VWAP 부족`, `theme coverage 부족`은 기본적으로 `WAIT_RETRY` 또는 `DATA_WAIT`로 남긴다.

---

## 2. suseok_ai에서 가져올 것과 버릴 것

### 2.1 가져올 것

#### A. RT-TLS / Theme Leadership

- Theme universe
- Realtime snapshot
- Theme leadership ranking
- Theme state classification
- Stock role classification
- Watchset selection

PR-2에서는 위 항목만 observe-only로 구현한다. `LEADING_THEME`/`SPREADING_THEME`은 매수 신호가 아니며, `LEADER`/`CO_LEADER` role도 매수 신호가 아니다. condition hit는 boost/discovery source로만 쓰고, `DATA_WAIT`는 hard block이 아니라 보류/재평가 상태로 남긴다. EntryTiming, OrderPlan, LIVE_SIM auto pipeline은 뒤 PR 범위다.

필수 snapshot 필드:

```text
current_price
change_rate
turnover_krw
cum_volume
execution_strength
best_bid
best_ask
spread_ticks
day_high
day_low
open_price
prev_close
momentum_1m
momentum_3m
momentum_5m
vwap
pullback_from_high_pct
```

#### B. Theme State

```text
LEADING_THEME      : 주도 테마
SPREADING_THEME    : 확산 테마
LEADER_ONLY_THEME  : 대장주만 강한 테마
WATCH_THEME        : 관찰 테마
DATA_WAIT          : 데이터 부족으로 보류
WEAK_THEME         : 약한 테마
```

초기 매매 허용 범위:

- `LEADING_THEME`: 허용
- `SPREADING_THEME`: 허용
- `LEADER_ONLY_THEME`: leader/co-leader만 허용
- `WATCH_THEME`: 관찰만
- `DATA_WAIT`: 보류 후 재평가
- `WEAK_THEME`: 제외

#### C. Stock Role

```text
LEADER        : 테마 내 대장주
CO_LEADER     : 공동대장
FOLLOWER      : 후발/확산주
LATE_LAGGARD  : 늦은 후발주
WEAK_MEMBER   : 약한 구성 종목
OVERHEATED    : 과열 종목
```

초기 매매 허용 범위:

- `LEADER`: 허용
- `CO_LEADER`: 허용
- `FOLLOWER`: 확산 테마에서만 소액 허용
- `LATE_LAGGARD`: 제외
- `WEAK_MEMBER`: 제외
- `OVERHEATED`: 제외

#### D. Entry Timing

허용:

```text
GOOD_PULLBACK
PULLBACK_RECLAIM
VWAP_RECLAIM
```

보류 또는 제외:

```text
CHASE_HIGH
VWAP_OVEREXTENDED
FAILED_BREAKOUT
NO_SETUP
DATA_WAIT
```

#### E. Stable idempotency key

중복 주문 방지를 위해 아래 요소를 포함한 안정적인 key를 사용한다.

```text
trading_date
code
theme_id or theme_name
setup_type
entry_timing_state
side
```

### 2.2 버릴 것

다음은 v2 초기 실전형 전환 범위에서 제외한다.

- legacy hybrid final grade 중심 구조
- threshold A/B machinery
- shadow small-entry report 중심 구조
- 조건검색 이름에 의존하는 핵심 매수 판단
- review-only panel 과다 확장
- dashboard debug panel 난립
- DATA_INSUFFICIENT를 일괄 hard block하는 정책
- AI가 직접 주문 의사결정을 내리는 구조

---

## 3. Hard Block / Soft Wait 기준

### 3.1 Hard Block

아래는 반드시 주문 차단한다.

```text
LIVE_REAL requested
real account detected
real server detected
kill switch active
gateway heartbeat stale
broker login unavailable
account unavailable
current_price <= 0
latest tick missing or stale
VI active
limit-up proximity
abnormal spread
duplicate idempotency key
max daily order count exceeded
max active positions exceeded
max order notional exceeded
extreme systemic risk off
```

### 3.2 Soft Wait / Retry

아래는 초기부터 hard block으로 보지 않는다.

```text
1m candle warmup insufficient
3m/5m momentum warmup insufficient
VWAP warmup insufficient
theme coverage partial
condition search not received
execution_strength missing temporarily
turnover rank not stable yet
early session noisy data
market split state not confirmed
```

처리 방식:

- `WAIT_RETRY` 또는 `DATA_WAIT`로 남긴다.
- subscription TTL을 유지한다.
- near-miss 후보로 집계한다.
- 일정 시간 후 재평가한다.

---

## 4. 단계별 PR 계획

## PR-0. Roadmap 문서 추가/정정

목표:

- 이 문서를 기준 문서로 추가한다.
- PR-1의 목적을 v2 runtime gap report가 아니라 `suseok_ai` 기능 선별/이식 기준 수립으로 명확히 한다.

산출물:

```text
docs/live_sim_pilot_roadmap_ko.md
```

Definition of Done:

- 문서가 repo에 추가되어 있다.
- 다음 PR부터 이 문서의 PR 번호를 참조한다.
- PR-1이 `suseok_ai` 분석 중심으로 정의되어 있다.

---

## PR-1. suseok_ai Functional Inventory & Migration Map

목표:

`v2`를 장에서 돌려본 결과를 분석하는 PR이 아니다. 이번 PR은 `suseok_ai`에서 실전 자동매매에 필요했던 기능을 코드/문서 기준으로 inventory화하고, v2로 가져올 것/버릴 것/재설계할 것을 분리하는 PR이다.

작업:

- `suseok_ai`의 시장/테마 엔진, 후보 생성, watchset, entry timing, risk/order/exit 흐름을 파일/함수/데이터 모델 단위로 조사한다.
- `suseok_ai`에서 무매수를 유발했거나 복잡도를 키운 구조를 분리한다.
- `suseok_ai` 기능을 v2에 그대로 복사하지 않고, v2의 Core/Gateway/Service 경계에 맞춰 어떤 신규 service/interface로 재구성할지 mapping한다.
- 조건검색, 네이버 테마, Kiwoom 실시간 tick/TR, theme membership, candidate lifecycle의 역할을 구분한다.
- 다음 PR에서 바로 구현 가능한 최소 이식 단위를 정의한다.

권장 산출물:

```text
docs/suseok_ai_v2_migration_map_ko.md
```

문서 구조:

```text
1. 요약
2. suseok_ai 실전 흐름 개요
3. 시장/테마 엔진 구성 요소
4. 후보 생성/watchset 구성 요소
5. 진입 타이밍 구성 요소
6. risk/order/exit 구성 요소
7. 가져올 기능 목록
8. 버릴 기능 목록
9. v2 신규 service/interface mapping
10. PR-2 구현 범위 제안
```

반드시 확인할 것:

- suseok_ai에서 주도 테마를 어떻게 만들었는가
- theme membership을 어떻게 구성했는가
- 조건검색은 핵심 판단인지 discovery/boost인지
- 후보 종목은 어떤 상태로 lifecycle을 탔는가
- 매수 가능 상태와 관찰 상태가 어디서 갈렸는가
- 어떤 risk/data gate가 과도하게 무매수를 만들었는가
- v2에는 어떤 최소 모델이 필요한가

Definition of Done:

- `docs/suseok_ai_v2_migration_map_ko.md`가 생성되어 있다.
- suseok_ai에서 가져올 것/버릴 것이 명확히 분리되어 있다.
- PR-2에서 구현할 최소 RT-TLS port 범위가 정리되어 있다.
- v2를 장에서 돌린 결과처럼 단정하지 않는다.
- 코드는 수정하지 않거나, 문서/테스트 보조 수준으로만 수정한다.

금지:

- v2 장중 실행 결과가 있는 것처럼 RCA 작성
- 자동 주문 활성화
- LIVE_SIM_AUTO_PIPELINE 구현
- AI 기능 추가
- suseok_ai legacy code 단순 복사
- LIVE_REAL 관련 구현

---

## PR-2. RT-TLS Minimal Core Port

목표:

PR-1에서 정리한 내용을 기준으로 `suseok_ai`의 시장/테마 엔진 장점만 v2 구조에 맞게 최소 이식한다.

추가/수정 후보:

```text
services/theme_leadership/
  universe.py
  snapshot.py
  ranker.py
  classifier.py
  stock_role.py
  watchset.py
```

작업:

- ThemeUniverseBuilder
- RealtimeSnapshotBuilder
- ThemeLeadershipRanker
- ThemeStateClassifier
- StockRoleClassifier
- WatchsetSelector
- condition search는 boost/discovery source로만 사용
- theme membership은 기존 v2 Theme Service와 충돌하지 않게 adapter로 연결

초기 제한값:

```text
TOP_THEME_COUNT = 5
MAX_STOCKS_PER_THEME = 3
MAX_TOTAL_WATCHSET = 20~30
MIN_VALID_MEMBERS_FOR_THEME = 2 또는 3
```

Definition of Done:

- mock data로 주도 테마 TOP5와 종목 role이 계산된다.
- `LEADER`, `CO_LEADER`, `FOLLOWER`, `LATE_LAGGARD`, `WEAK_MEMBER`, `OVERHEATED`가 구분된다.
- `DATA_WAIT`는 hard block이 아니라 보류 상태로 남는다.
- 조건검색 hit만으로 매수 가능 상태가 되지 않는다.

금지:

- 주문 intent 생성
- LIVE_SIM 자동 주문 연결
- legacy hybrid final grade 복사

---

## PR-3. EntryTimingEngine & OrderPlanBuilder

목표:

테마/종목 후보를 실제 진입 가능한 setup으로 분류하고, 주문 계획을 만든다.

추가/수정 후보:

```text
services/entry_timing/
  engine.py
  order_plan.py
  price_location.py
```

EntryTiming 상태:

```text
GOOD_PULLBACK
PULLBACK_RECLAIM
VWAP_RECLAIM
CHASE_HIGH
VWAP_OVEREXTENDED
FAILED_BREAKOUT
NO_SETUP
DATA_WAIT
```

OrderPlan 필드 예시:

```text
code
side
quantity
notional
order_type
limit_price
price_source
setup_type
theme_state
stock_role
entry_timing_state
risk_budget
idempotency_key
expires_at
reason
```

초기 주문 정책:

```text
매수: LIMIT only
시장가 매수 금지
price_offset_ticks: 0~1
max_order_notional: 설정값 사용
```

Definition of Done:

- `LEADER/CO_LEADER + GOOD_PULLBACK/PULLBACK_RECLAIM/VWAP_RECLAIM` 조합에서 order plan이 생성된다.
- `CHASE_HIGH`, `VWAP_OVEREXTENDED`, `FAILED_BREAKOUT`은 order plan을 만들지 않는다.
- `DATA_WAIT`는 near-miss로 남는다.
- idempotency key가 안정적으로 생성된다.

금지:

- Gateway command queue 직접 호출
- AI score를 필수 조건으로 만들기
- 시장가 매수 기본 허용

---

## PR-4. LIVE_SIM Auto Pipeline Pilot

목표:

장중 자동으로 `candidate -> strategy/risk -> order plan -> LIVE_SIM intent -> gateway send_order command`까지 이어지는 파일럿 루프를 만든다.

추가/수정 후보:

```text
services/runtime/live_sim_auto_pipeline.py
api/routes/live_sim_auto.py 또는 기존 live_sim route 확장
tools/run_live_sim_auto_pipeline_once.py
tests/test_live_sim_auto_pipeline.py
```

설정 추가:

```text
TRADING_PROFILE=OBSERVE | LIVE_SIM_PILOT
LIVE_SIM_AUTO_PIPELINE_ENABLED=false
LIVE_SIM_AUTO_ORDER_ENABLED=false
LIVE_SIM_MAX_ORDER_NOTIONAL=100000
LIVE_SIM_MAX_DAILY_ORDER_COUNT=3
LIVE_SIM_MAX_ACTIVE_POSITIONS=1
LIVE_SIM_ALLOW_MARKET_ORDER=false
LIVE_SIM_DEFAULT_ORDER_TYPE=LIMIT
LIVE_SIM_PRICE_OFFSET_TICKS=0
LIVE_SIM_REQUIRE_DRY_RUN_EVIDENCE=true
```

파일럿 루프:

```text
run_once:
  load latest market data
  build theme leadership
  select watchset
  evaluate entry timing
  evaluate strategy/risk
  build order plan
  create live_sim intent
  queue gateway send_order command
  write audit event
```

초기 실행 방식:

- scheduler보다 `run_once`를 먼저 구현한다.
- `run_once`가 안정화된 후 5~10초 주기 scheduler를 붙인다.
- default는 항상 disabled다.

Definition of Done:

- mock gateway event 기반 통합 테스트에서 LIVE_SIM command가 queue된다.
- hard block 조건에서는 command가 queue되지 않는다.
- idempotency key로 중복 주문이 방지된다.
- 설정이 꺼져 있으면 절대 주문 command가 생성되지 않는다.

금지:

- LIVE_REAL 지원
- kill switch 우회
- Gateway에서 전략 판단 수행
- AI 결과로 직접 command 생성

---

## PR-5. LIVE_SIM Execution, Cancel, Exit, Reconcile

목표:

매수 이후 운영이 가능하도록 미체결 취소, 체결 반영, 자동 청산, 리컨실을 보강한다.

작업:

- broker ack event 저장
- partial fill / fill event 처리
- local position update
- unfilled order TTL cancel
- stop-loss
- take-profit
- trailing stop
- max hold
- 장마감 전 정리 옵션
- local/broker reconcile report

이번 PR-5의 확정 범위:

- scheduler는 구현하지 않고 `cancel_unfilled_once`, `evaluate_exit_once`, `reconcile_once`만 제공한다.
- `cancel_order`는 미체결 BUY 취소 전용이다.
- SELL은 open position close-only exit 전용이다.
- `modify_order`는 구현하지 않는다.
- Dashboard에는 buy/sell/cancel 버튼을 추가하지 않는다.

기본 정책:

```text
매수 주문 TTL: 30~90초
손절: 기본 2.5~3.5%, 설정 가능
익절: 기본 4~7%, 설정 가능
트레일링: 수익 전환 후 활성화
max hold: setup별 설정
```

Definition of Done:

- 매수 후 포지션 상태가 dashboard/API에 반영된다.
- 미체결 주문이 TTL 이후 cancel command로 이어진다.
- stop/take/trailing/max_hold 조건에서 exit order가 생성된다.
- 재시작 후 broker 상태와 local 상태 차이를 보고한다.
- reconcile mismatch가 신규 BUY를 차단한다.
- LIVE_REAL, real account/server, stale heartbeat, kill switch에서는 주문성 command가 reject된다.

금지:

- 실계좌 청산 지원
- 손절 조건 무시
- 미체결 방치
- scheduler/daemon/while-loop 상시 반복

---

## PR-6. AI Candidate Scorer Advisory

목표:

블로그의 ChatGPT 활용 아이디어를 v2에 맞게 advisory-only로 도입한다.

AI 역할:

- 상위 후보 5~10개 점수화
- 후보별 장단점 요약
- 동적 손절/익절/트레일링 제안
- 최근 성과 기반 조언

AI 금지:

- 주문 생성
- gateway command 생성
- kill switch 변경
- 수량/계좌 한도 직접 변경

출력 JSON schema 예시:

```json
{
  "selected": ["005930"],
  "analysis": {"005930": "reason"},
  "score": {"005930": 82},
  "confidence": {"005930": 74},
  "risk_reward": {
    "005930": {
      "stop_loss_pct": 2.5,
      "take_profit_pct": 5.5,
      "trailing_stop_pct": 2.0
    }
  },
  "summary": "summary"
}
```

Definition of Done:

- AI output은 strict schema로 검증된다.
- AI 실패 시 pipeline은 기본 rule-based 정책으로 동작한다.
- AI가 0개를 선택해도 허용된다.
- AI 관망과 시스템 무매수는 reason이 분리된다.

금지:

- AI score를 유일한 매수 조건으로 만들기
- AI가 주문 command를 직접 만들기
- 외부 네트워크 실패가 pipeline 전체 장애로 번지기

---

## PR-7. No-Buy Sentinel & Operator Dashboard Simplification

목표:

무매수 원인을 실시간으로 분리하고, 운영자가 볼 화면을 단순화한다.

No-Buy Sentinel 조건:

```text
장 시작 후 20분 이상 경과
Gateway 정상
MarketData 정상
Theme snapshot 정상
후보 존재
LIVE_SIM intent 0건
```

계산 항목:

```text
top near-miss 후보 10개
막힌 단계
hard block 비율
soft wait 비율
data wait 비율
threshold miss 비율
system safety block 여부
market no-trade 여부
pipeline over-conservative 여부
```

Dashboard 메인 패널:

```text
1. 시스템 상태
2. 오늘 시장
3. 주도 테마 TOP5
4. 매수 후보
5. 주문/포지션
6. 왜 안 샀나
```

Definition of Done:

- 운영자 메인 화면에서 지금 살 수 있는 후보와 못 산 이유가 보인다.
- 무매수 원인이 시장 문제인지, 데이터 문제인지, 과보수 게이트 문제인지 분리된다.
- 한글 다크 UI 기준으로 정리된다.

금지:

- debug panel을 메인 화면에 계속 추가
- reason을 하나의 DATA_INSUFFICIENT로 뭉개기
- 매수 후보와 주문 상태를 같은 의미로 표시하기

---

## 5. PR 진행 순서

권장 순서:

```text
PR-0 Roadmap 문서 추가/정정
PR-1 suseok_ai Functional Inventory & Migration Map
PR-2 RT-TLS Minimal Core Port
PR-3 EntryTimingEngine & OrderPlanBuilder
PR-3.5 Naver Theme Importer / Theme Membership Auto Refresh
PR-4 LIVE_SIM Auto Pipeline Pilot
PR-5 Execution/Cancel/Exit/Reconcile
PR-6 AI Candidate Scorer Advisory
PR-7 No-Buy Sentinel & Dashboard Simplification
```

중요:

- PR-1은 v2 장중 gap report가 아니라 `suseok_ai` 분석/이식 기준 수립이다.
- PR-1 없이 PR-2로 가지 않는다.
- PR-2 없이 PR-3로 가지 않는다.
- PR-3와 PR-3.5 없이 PR-4로 가지 않는다.
- PR-4 전까지는 자동 주문 command queue를 만들지 않는다.
- PR-5 전에는 소액 파일럿만 허용한다.
- PR-6 AI는 PR-4 이후 붙인다. AI를 먼저 붙이면 무매수/오매수 원인이 흐려진다.

---

## 6. 다음 장 전 최소 목표

다음 장에서 “suseok_ai 이전에 돌아가던 만큼”에 가까워지려면 최소한 아래까지 필요하다.

```text
필수: PR-1, PR-2, PR-3, PR-4
권장: PR-5 일부, PR-7 일부
후순위: PR-6
```

단, 다음 장 전 시간이 부족하면 PR-1 결과를 바탕으로 PR-2/PR-3/PR-4를 더 작게 쪼갠다.
