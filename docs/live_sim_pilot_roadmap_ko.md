# suseok-trader-v2 LIVE_SIM Pilot Roadmap

> 목적: `suseok_ai`의 장점인 실시간 시장/테마 판단력과 실전형 후보 생성 흐름은 살리고, 과도한 legacy/hybrid/A-B/shadow/review-only 복잡도는 제외하여 `suseok-trader-v2`를 키움 모의투자 전용 실전 자동매매 v2로 단계적으로 전환한다.

이 문서는 한 번에 대규모 PR을 만들지 않기 위한 기준 문서다. Codex 작업은 반드시 아래 PR 단위로 나누어 진행한다.

---

## 0. 현재 판단

현재 `suseok-trader-v2`는 Core/Gateway 분리, MarketData, Theme, Candidate, Strategy, Risk, Dashboard, DRY_RUN, LIVE_SIM 구조를 갖고 있지만 기본 목적이 OBSERVE/read-only/manual 중심이다.

따라서 다음 장에서 실전형 자동매매 프로그램처럼 동작하려면 다음 연결이 필요하다.

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

핵심 방향은 `suseok_ai`를 그대로 복사하는 것이 아니다. v2의 안전한 경계를 유지하면서, 실제 자동매매에 필요한 최소 실행 루프를 만든다.

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

## 3. v2 목표 파이프라인

```text
[장전]
  Theme membership 갱신
  전일/최근 theme persistence 계산
  universe 압축
  realtime subscription 후보 준비

[장중]
  Kiwoom tick/TR/condition event 수신
    -> RealtimeSnapshotBuilder
    -> ThemeLeadershipRanker
    -> ThemeStateClassifier
    -> StockRoleClassifier
    -> WatchsetSelector
    -> EntryTimingEngine
    -> Strategy/Risk Gate
    -> OrderPlanBuilder
    -> LIVE_SIM intent
    -> Gateway send_order command
    -> Execution/Position/Exit/Reconcile
```

---

## 4. 주문 관련 상태 모델

기존 observation 상태와 실제 주문 가능 상태를 분리한다.

```text
OBSERVED
THEME_READY
WATCH_READY
ENTRY_READY
RISK_PASS
TRADE_READY
ORDER_PLAN_READY
LIVE_SIM_INTENT_CREATED
LIVE_SIM_COMMAND_QUEUED
BROKER_ACKED
PARTIALLY_FILLED
FILLED
EXIT_MONITORING
EXIT_SENT
CLOSED
BLOCKED
WAIT_RETRY
DATA_WAIT
```

중요:

- `MATCHED_OBSERVATION`은 바로 주문 신호가 아니다.
- `OBSERVE_PASS`는 바로 주문 승인도 아니다.
- `TRADE_READY`부터 주문 후보로 본다.
- `ORDER_PLAN_READY` 이후에만 LIVE_SIM intent를 만들 수 있다.

---

## 5. Hard Block / Soft Wait 기준

### 5.1 Hard Block

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

### 5.2 Soft Wait / Retry

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

## 6. 단계별 PR 계획

## PR-0. Roadmap 문서 추가

목표:

- 이 문서를 추가한다.
- 향후 PR이 이 문서를 기준으로 범위를 지키게 한다.

산출물:

- `docs/live_sim_pilot_roadmap_ko.md`

Definition of Done:

- 문서가 repo에 추가되어 있다.
- 다음 PR부터 이 문서의 PR 번호를 참조한다.

---

## PR-1. Current State Inventory & Gap Report

목표:

현재 v2가 왜 OBSERVE/manual 중심인지 코드 기준으로 정리하고, 자동 LIVE_SIM pipeline이 끊긴 지점을 명확히 한다.

작업:

- README, docs, settings/config, Strategy, Risk, DRY_RUN, LIVE_SIM, Gateway command queue 점검
- `candidate -> strategy -> risk -> order plan -> live_sim intent -> gateway command` 연결 상태 확인
- 자동 실행 루프가 없는 지점 또는 flag로 막힌 지점 식별
- hard block과 observe-only block을 분리

권장 산출물:

```text
docs/live_sim_pilot_gap_report_ko.md
```

반드시 확인할 것:

- Strategy/Risk가 observation에 머무는 이유
- LIVE_SIM intent가 자동 생성되지 않는 이유
- send_order command가 어떤 조건에서 queue되는지
- Gateway가 command를 polling해서 Kiwoom으로 넘기는 흐름
- 현재 테스트로 보장되는 safety 범위

Definition of Done:

- 자동매매 pipeline의 missing link가 파일/함수 단위로 정리되어 있다.
- 다음 PR에서 수정할 최소 지점이 명확하다.
- 코드는 최소 수정한다. 이 PR은 분석/문서화 중심이다.

금지:

- 대규모 구조 변경
- AI 기능 추가
- 주문 자동화 활성화

---

## PR-2. RT-TLS Minimal Core Port

목표:

`suseok_ai`의 시장/테마 엔진 장점만 v2 구조에 맞게 최소 이식한다.

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

금지:

- 실계좌 청산 지원
- 손절 조건 무시
- 미체결 방치

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

입력 예시:

```text
theme_state
stock_role
current_price
change_rate
turnover_krw
execution_strength
momentum_1m/3m/5m
vwap_deviation
pullback_from_high_pct
entry_timing_state
risk_reasons
recent_trade_stats
```

출력 JSON schema:

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

개발자/검증 탭으로 이동:

```text
raw events
debug tables
A/B style reports
review-only panels
long audit logs
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

## 7. PR 진행 순서

권장 순서:

```text
PR-0 Roadmap 문서 추가
PR-1 Current State Inventory & Gap Report
PR-2 RT-TLS Minimal Core Port
PR-3 EntryTimingEngine & OrderPlanBuilder
PR-4 LIVE_SIM Auto Pipeline Pilot
PR-5 Execution/Cancel/Exit/Reconcile
PR-6 AI Candidate Scorer Advisory
PR-7 No-Buy Sentinel & Dashboard Simplification
```

중요:

- PR-1 없이 PR-4로 가지 않는다.
- PR-2 없이 PR-3로 가지 않는다.
- PR-3 없이 PR-4로 가지 않는다.
- PR-4 전까지는 자동 주문 command queue를 만들지 않는다.
- PR-5 전에는 소액 파일럿만 허용한다.
- PR-6 AI는 PR-4 이후 붙인다. AI를 먼저 붙이면 무매수/오매수 원인이 흐려진다.

---

## 8. 다음 장 전 최소 목표

다음 장에서 “suseok_ai 이전에 돌아가던 만큼”에 가까워지려면 최소한 아래까지 필요하다.

```text
필수: PR-1, PR-2, PR-3, PR-4
권장: PR-5 일부, PR-7 일부
후순위: PR-6
```

최소 파일럿 조건:

```text
LIVE_SIM only
max order notional <= 100,000 KRW
max daily order count <= 3
max active positions <= 1
market buy disabled
kill switch respected
duplicate order blocked
unfilled order cancel available or explicitly blocked after TTL
```

PR-5가 완료되지 않았으면 장중 자동매수는 매우 제한적으로만 허용한다.

---

## 9. Codex 작업 공통 프롬프트 헤더

각 PR을 Codex에 던질 때 아래 헤더를 붙인다.

```text
너는 Kiwoom OpenAPI+ 기반 KOSPI/KOSDAQ 자동매매 시스템을 운영하는 시니어 퀀트 전략 개발자, 리스크 엔지니어, Python 백엔드 개발자다.

대상 repo: suseok-trader-v2
기준 문서: docs/live_sim_pilot_roadmap_ko.md

이번 작업은 기준 문서의 해당 PR 범위만 수행한다.
절대 다른 PR 범위까지 한 번에 구현하지 마라.
LIVE_REAL은 절대 구현하거나 활성화하지 마라.
AI/GPT는 advisory-only이며 주문 command/order intent를 직접 만들 수 없다.
Core 내부에 PyQt5/QAxWidget/Kiwoom concrete client import를 추가하지 마라.
DATA_WAIT/WARMUP/VWAP_MISSING을 일괄 hard block으로 처리하지 마라.
계좌/주문 안전 관련 hard block은 반드시 유지하라.

작업 후 반드시 아래를 보고하라.
1. 변경 파일 목록
2. 기준 문서의 어떤 항목을 구현했는지
3. 구현하지 않은 항목과 이유
4. 자동매매 pipeline에 미치는 영향
5. safety 영향
6. 테스트 결과
7. 다음 PR로 넘길 항목
```

---

## 10. PR-1 Codex 프롬프트

```text
너는 Kiwoom OpenAPI+ 기반 KOSPI/KOSDAQ 자동매매 시스템을 운영하는 시니어 퀀트 전략 개발자, 리스크 엔지니어, Python 백엔드 개발자다.

대상 repo: suseok-trader-v2
기준 문서: docs/live_sim_pilot_roadmap_ko.md
이번 작업 범위: PR-1 Current State Inventory & Gap Report

목표:
현재 v2가 왜 OBSERVE/manual 중심인지 코드 기준으로 정리하고, 자동 LIVE_SIM pipeline이 끊긴 지점을 명확히 해라.
이번 PR은 분석/문서화 중심이며, 대규모 구현을 하지 않는다.

점검 대상:
- README.md
- docs/architecture.md
- settings/config 관련 파일
- Strategy/Risk evaluator
- Candidate FSM
- DRY_RUN OMS
- LIVE_SIM route/service
- Gateway command queue
- Dashboard status/snapshot API
- tests

확인할 흐름:
candidate -> strategy -> risk -> order plan -> live_sim intent -> gateway send_order command

구체적으로 확인할 것:
1. Strategy/Risk가 observation에 머무는 이유
2. LIVE_SIM intent가 자동 생성되지 않는 이유
3. send_order GatewayCommand가 어떤 조건에서 queue되는지
4. Gateway가 command를 polling해서 Kiwoom으로 넘기는 흐름
5. 현재 hard block, observe-only block, config-disabled block 구분
6. 자동매매 pipeline에서 missing link인 파일/함수/설정
7. PR-2/PR-3/PR-4에서 수정해야 할 최소 지점

산출물:
- docs/live_sim_pilot_gap_report_ko.md 신규 작성

문서 구조:
1. 요약
2. 현재 pipeline 구조
3. 현재 끊긴 지점
4. config/flag 현황
5. Strategy/Risk/LIVE_SIM 상태
6. Gateway command queue 상태
7. hard block vs observe-only vs disabled 구분
8. 다음 PR별 수정 포인트
9. 테스트/검증 방법
10. 남은 리스크

금지:
- 자동 주문 활성화
- LIVE_SIM_AUTO_PIPELINE 구현
- AI 기능 추가
- legacy suseok_ai 코드 복사
- LIVE_REAL 관련 구현

완료 조건:
- docs/live_sim_pilot_gap_report_ko.md가 생성되어 있다.
- 자동매매 pipeline의 missing link가 파일/함수/설정 단위로 정리되어 있다.
- 다음 PR에서 어디를 수정해야 하는지 명확하다.
- 기존 테스트가 깨지지 않는다.
```
