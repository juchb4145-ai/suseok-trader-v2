# No-Buy Recovery Roadmap

작성일: 2026-07-02

상태: **개발 완료** (2026-07-19 현재 `main` 기준)

이 문서의 PR-NB0~PR-NB9 범위는 모두 구현됐다. 아래의 "현재 확인된 구조적 문제"는
로드맵 작성 당시의 기준선이며, 현재 결함 목록으로 해석하지 않는다. 완료는 비운영 코드·계약과
테스트가 병합됐다는 뜻이다. FAST-0 자격, 운영 DB 정리, LIVE_SIM/LIVE_REAL 활성화 또는 주문 실행을
승인하지 않는다.

## 구현 완료 기록

| 단계 | 상태 | main 근거 | 완료된 핵심 범위 |
| --- | --- | --- | --- |
| PR-NB0 | `DONE` | `11ebcb40` | No-Buy recovery 로드맵과 후속 PR 경계 |
| PR-NB1 | `DONE` | `a11eb522` | stage funnel, role summary, inspect/rebuild 출력 |
| PR-NB2 | `DONE` | `b5835ebe` | LIVE_SIM_PILOT shadow dry-run evidence와 MARKET/LIMIT 조건 수정 |
| PR-NB3 | `DONE` | `20f90b93` | immutable `TradingCapabilities`와 profile capability matrix |
| PR-NB4 | `DONE` | `4a3621dc` | observation model의 입력 `observe_only` 보존과 검증 |
| PR-NB5 | `DONE` | `c81f5006` | 공통 trade admission evaluator와 일관된 admission trace |
| PR-NB6 | `DONE` | `6c718c86` | condition role별 promotion/not-buy 의미와 discovery 대기 상태 |
| PR-NB7 | `DONE` | `cc23017e` | momentum continuation entry route와 보수적 plan 제약 |
| PR-NB8 | `DONE` | `8d56f566` | BLOCKING/WAITING/INFO reason channel과 명시적 classifier |
| PR-NB9 | `DONE` | `ea6670b2` | profile-first 문서, deprecated flag warning과 operator 노출 |

후속 변경은 이 표의 완료 범위를 다시 여는 방식이 아니라 별도 로드맵/PR로 관리한다. 특히
과거 FAST-0 개발단계 evidence는 현재 운영 gate에서 재검증하지 않는다. 운영 승격은 현재 거래일의
tick/index/context readiness와 FAST-1 `canary_ready=true` 없이 허용하지 않는다. 자세한 경계는
`docs/fast_track_live_sim_roadmap_2026-07_ko.md`를 따른다.

이 문서는 현재 `suseok-trader-v2`에서 무매수가 구조적으로 확정되는 지점을 작은 PR로
나누어 고치기 위한 기준 문서다. 목표는 한 번에 trading pipeline 전체를 뒤엎는 것이 아니라,
각 PR이 독립적으로 검증 가능한 증거와 완료 조건을 남기면서 `condition hit -> fusion ->
candidate -> strategy -> risk -> order plan -> LIVE_SIM intent` 흐름의 생존율을 회복하는 것이다.

## 원칙

- `LIVE_REAL`은 이번 로드맵 범위가 아니다.
- 첫 PR은 동작 변경이 아니라 계측이다.
- 데드락/역논리처럼 독립적이고 재현 가능한 결함은 작은 hotfix PR로 분리할 수 있다.
- `MATCHED_OBSERVATION`, `OBSERVE_PASS`, `PLAN_READY`는 여전히 단독 주문 승인으로 해석하지 않는다.
- 매수 가능성은 Strategy/Risk/OMS 모델에 흩어진 bool이 아니라 profile capability와 admission 결과로 표현한다.
- reason code는 차단, 대기, 정보성 신호를 한 리스트에 섞지 않는다.
- 각 PR은 dashboard 문구보다 저장된 데이터와 테스트가 먼저 바뀌어야 한다.

## 작성 당시 확인된 구조적 문제

### 1. observe_only가 모델 불변식으로 고정됨

`domain/strategy/models.py`, `domain/risk/models.py`, `domain/oms/models.py`의 `__post_init__`가
입력값과 profile을 무시하고 `observe_only=True`를 강제한다. `services/config.py`에는
`TradingProfile.OBSERVE`, `TradingProfile.LIVE_SIM_PILOT`이 있지만, 실제 도메인 모델의
capability source가 되지 못한다.

결과적으로 전략의 최고 상태는 `MATCHED_OBSERVATION`, 리스크의 최고 상태는 `OBSERVE_PASS`이며,
주문 가능 admission을 표현하는 타입이 별도로 없다.

### 2. condition fusion이 모든 조건 hit에 not-buy 낙인을 붙임

`services/condition_fusion.py`는 fusion reason에 `CONDITION_NOT_BUY_SIGNAL_REASON`을 기본으로
넣고, metadata에도 `not_buy_signal=True`를 고정한다. LEADER+BREAKOUT 또는 LEADER+PULLBACK이
priority boost를 받아도 동시에 `MARKET_SENSOR_NOT_BUY_SIGNAL` 성격을 갖는다.

또한 DISCOVERY 단독은 `candidate_promotion_allowed=False`가 되고,
`services/candidate_service.py`와 `services/entry_timing/service.py`에서 관찰/제외 경로로 밀려
entry timing 평가에서 빠진다.

### 3. DRY_RUN과 LIVE_SIM 사이에 evidence 데드락이 있음

`services/oms/dry_run_service.py`는 `live_sim_allowed` 또는 `live_real_allowed`가 켜지면
`LIVE_FLAGS_ENABLED`로 dry-run intent 생성을 거부한다. 반대로 `services/live_sim/live_sim_service.py`는
기본적으로 dry-run evidence가 없으면 `DRY_RUN_EVIDENCE_MISSING`으로 LIVE_SIM intent를 거부한다.

같은 trade date의 후보 인스턴스를 기준으로 보면 LIVE_SIM을 켠 날에는 dry-run이 생성되지 않고,
dry-run이 없으니 LIVE_SIM도 생성되지 않는다.

유사한 독립 결함으로 `services/live_sim/order_plan_eligibility.py`는
`order_type != "LIMIT" or live_sim_order_plan_allow_market_order` 조건 때문에 market order 허용 flag를
켜면 LIMIT order plan까지 거부한다.

### 4. 신호 role과 entry timing 정책이 맞지 않음

후보 공급원은 LEADER/BREAKOUT 계열 모멘텀 신호인데, `services/entry_timing/engine.py`는
고가 부근이나 얕은 눌림을 `CHASE_HIGH`, `PULLBACK_TOO_SHALLOW`로 차단한다. BREAKOUT 신호가
강할수록 가격은 day high 근처에 있을 가능성이 높으므로, 현재 구조에서는 좋은 breakout 신호가
pullback 전용 엔진에서 차단된다.

### 5. reason classifier가 실상을 더 비관적으로 표시함

`services/operator/reason_classifier.py`는 substring 휴리스틱으로 reason을 분류한다.
`CANDIDATE_WITHOUT_ORDER_PLAN`처럼 단순 대기성 reason도 `ORDER_PLAN` 문자열 때문에 hard block으로
보일 수 있고, 정보성/대기성/차단성 코드가 같은 reason list에 섞인다.

## 목표 funnel

P0 이후 No-Buy Sentinel은 최소한 아래 단계별 생존 수를 하루 단위로 저장해야 한다.

```text
condition_hit
  -> fusion_promoted
  -> candidate_context_ready
  -> strategy_matched
  -> risk_pass
  -> order_plan_ready
  -> live_sim_eligible
  -> live_sim_intent_created
  -> gateway_command_queued
```

각 단계는 total count, survived count, drop count, top drop reasons, sample candidate ids를 가진다.
가능하면 role별로 `DISCOVERY`, `LEADER`, `BREAKOUT`, `PULLBACK`, `RISK_BLOCK` 생존율도 함께 저장한다.

사용 가능한 기존 테이블 축:

- `candidate_condition_fusion`
- `candidates`
- `strategy_observations_latest`
- `risk_observations_latest`
- `order_plan_drafts_latest`
- `dry_run_intents`
- `live_sim_intents`
- `gateway_commands`
- `no_buy_sentinel_snapshots`

## PR 분할 계획

### PR-NB0. Roadmap 문서 추가

목표:

- 이 문서를 추가하고 후속 PR의 기준으로 삼는다.

주요 파일:

- `docs/no_buy_recovery_roadmap_ko.md`
- 선택: `docs/roadmap.md`

Definition of Done:

- 후속 PR들이 `PR-NB*` 번호와 범위를 참조할 수 있다.
- 코드 동작 변경은 없다.

### PR-NB1. No-Buy Funnel 계측 확장

목표:

- 동작 변경 없이 무매수 funnel 생존율을 하루 단위로 기록한다.
- "reason code가 많다"가 아니라 "어느 단계에서 몇 개가 죽었는지"를 볼 수 있게 한다.

작업:

- `services/operator/no_buy_sentinel.py`에 `stage_funnel` 구조를 추가한다.
- stage별 input/output count, drop reasons, top near-miss sample을 저장한다.
- role별 생존율을 추가한다.
- snapshot JSON schema를 backward compatible하게 확장한다.
- `tools.inspect_no_buy_sentinel`, `tools.rebuild_no_buy_sentinel` 출력에 funnel 요약을 추가한다.

테스트:

- `tests/test_no_buy_sentinel.py`
- `tests/test_operator_api.py`

Definition of Done:

- 장중 데이터가 없어도 fixture 기반으로 stage funnel이 계산된다.
- `PLAN_READY=0`일 때도 이전 단계의 생존 수가 보인다.
- 기존 dashboard/API consumer가 새 필드 때문에 깨지지 않는다.

### PR-NB2. DRY_RUN/LIVE_SIM 데드락과 LIMIT 역논리 수정

목표:

- LIVE_SIM pilot에서도 dry-run evidence를 만들 수 있게 하거나, evidence 요구를 같은 profile 안에서 충족 가능한 조건으로 바꾼다.
- `live_sim_order_plan_allow_market_order`가 LIMIT order plan을 거부하지 않게 한다.

작업:

- `services/oms/dry_run_service.py`의 `LIVE_FLAGS_ENABLED` 거부 조건을 profile-aware로 변경한다.
- `services/live_sim/live_sim_service.py`의 dry-run evidence requirement를 `LIVE_SIM_PILOT`에서 충족 가능한 정책으로 조정한다.
- `services/live_sim/order_plan_eligibility.py`의 market order 조건을 `order_type == "MARKET" and not allow_market_order` 형태로 고친다.

테스트:

- `tests/test_oms_dry_run.py`
- `tests/test_live_sim_order_plan_pipeline.py`
- `tests/test_config.py`

Definition of Done:

- `TRADING_PROFILE=LIVE_SIM_PILOT`에서 dry-run shadow evidence와 LIVE_SIM intent가 같은 trade date에 이어질 수 있다.
- market order 허용 flag가 LIMIT order plan을 거부하지 않는다.
- LIVE_REAL 또는 real account/server 차단은 완화하지 않는다.

주의:

- 이 PR은 P0 이후 바로 fast-track 가능하다.
- admission 통합 전의 최소 hotfix이므로 gate 중복 자체는 다음 PR에서 다룬다.

### PR-NB3. TradingProfile capability matrix 도입

목표:

- 흩어진 `trading_mode`, `trading_allow_live_sim`, `dry_run_*`, `live_sim_*` flag를 profile capability로 해석하는 단일 진입점을 만든다.

작업:

- `services/config.py`에 profile별 capability matrix를 추가한다.
- profile이 허용하는 capability를 명시한다.

예시:

```text
OBSERVE:
  observe=true
  dry_run_shadow=false
  live_sim_intent=false
  gateway_order_command=false

LIVE_SIM_PILOT:
  observe=true
  dry_run_shadow=true
  live_sim_intent=true
  gateway_order_command=true, only simulation account/server
  live_real=false
```

주요 산출물:

- `TradingCapabilities` 또는 동등한 immutable value object
- `settings.trading_capabilities` property
- profile/flag 충돌 validation

테스트:

- `tests/test_config.py`
- LIVE_SIM 관련 safety test

Definition of Done:

- 신규 코드가 직접 `trading_allow_live_sim` 등 raw flag를 조합하지 않고 capability를 읽을 수 있다.
- 기존 env flag는 즉시 제거하지 않고 deprecation path를 문서화한다.
- `LIVE_REAL` capability는 계속 false다.

### PR-NB4. observe_only 강제값 제거와 domain state 재정의

목표:

- 모델 `__post_init__`의 무조건 `observe_only=True`를 제거하고 profile/admission이 capability를 결정하게 한다.
- 관찰 상태와 주문 admission 상태를 타입으로 분리한다.

작업:

- `domain/strategy/models.py`, `domain/risk/models.py`, `domain/oms/models.py`의 강제 setter를 제거하거나 capability 기반 검증으로 교체한다.
- Strategy/Risk observation은 계속 관찰 결과로 두되, 주문 가능성은 별도 admission 결과에서 표현한다.
- `MATCHED_OBSERVATION`과 `OBSERVE_PASS`를 주문 승인으로 승격하지 않는다.
- 필요한 경우 `AdmissionStatus`를 새로 둔다.

테스트:

- `tests/test_strategy_service.py`
- `tests/test_risk_gate_service.py`
- `tests/test_oms_dry_run.py`
- API serialization tests

Definition of Done:

- 입력 `observe_only`가 모델에서 조용히 덮어써지지 않는다.
- `OBSERVE` profile에서는 기존처럼 주문성 side effect가 없다.
- `LIVE_SIM_PILOT` profile에서는 admission service가 "모의주문 가능/불가"를 별도 결과로 낸다.

### PR-NB5. Admission service로 중복 gate 통합

목표:

- dry-run eligibility, LIVE_SIM eligibility, order plan eligibility가 candidate/strategy/risk 상태를 각자 다시 판정하는 구조를 줄인다.

작업:

- `services/admission/` 또는 기존 LIVE_SIM service 내부에 단일 admission evaluator를 추가한다.
- candidate, strategy, risk, entry timing, order plan, safety gate의 책임 경계를 정리한다.
- dry-run은 shadow accounting admission을, LIVE_SIM은 simulation order admission을 같은 evidence object에서 읽는다.

Definition of Done:

- 하나의 candidate/order_plan에 대해 "어느 gate가 막았는지"가 단일 admission trace로 나온다.
- dry-run과 LIVE_SIM reason이 서로 모순되지 않는다.
- No-Buy Sentinel이 admission trace를 그대로 보여줄 수 있다.

### PR-NB6. Condition fusion reason/channel 정리

목표:

- condition fusion의 "조건검색 센서"와 "매수 신호 아님"을 분리한다.
- DISCOVERY 단독 종목에도 제한적 승격 또는 재평가 경로를 만든다.

작업:

- `services/condition_fusion.py`에서 unconditional `CONDITION_NOT_BUY_SIGNAL_REASON`을 제거하거나 INFO 채널로 이동한다.
- `not_buy_signal=True` metadata를 role별/상태별로 계산한다.
- `DISCOVERY` 단독은 바로 order 가능 상태가 아니라 `WATCH_RETRY` 또는 `DISCOVERY_PROMOTION_PENDING`처럼 재평가 가능한 상태로 둔다.
- `services/candidate_service.py`, `services/entry_timing/service.py`의 DISCOVERY 제외 조건을 새 상태에 맞춘다.

테스트:

- condition fusion tests
- candidate service tests
- entry timing candidate query tests

Definition of Done:

- LEADER+BREAKOUT, LEADER+PULLBACK fusion은 not-buy 낙인을 기본으로 갖지 않는다.
- DISCOVERY 단독은 영구 관찰로 고정되지 않는다.
- risk block role은 계속 hard block으로 남는다.

### PR-NB7. Signal role별 entry route 분리

목표:

- BREAKOUT/LEADER 신호와 PULLBACK 신호를 같은 pullback-only entry timing engine에 태우지 않는다.

작업:

- `services/entry_timing/engine.py`에 role/setup route를 추가한다.
- BREAKOUT/LEADER route에는 `breakout_retest`, `momentum_continuation` 정책을 둔다.
- PULLBACK route에는 기존 `GOOD_PULLBACK`, `PULLBACK_RECLAIM`, `VWAP_RECLAIM` 정책을 유지한다.
- 신고가 부근 허용 시에는 사이즈 축소, tight stop, shorter TTL을 admission evidence에 명시한다.
- plan TTL과 pipeline 주기 관계를 계측하고, 필요하면 role별 TTL을 분리한다.

Definition of Done:

- BREAKOUT role이 day high 근처라는 이유만으로 무조건 `BLOCKED_CHASE`가 되지 않는다.
- PULLBACK role은 기존 pullback quality check를 유지한다.
- role별 통과율이 No-Buy Funnel에 표시된다.

### PR-NB8. Reason code 3채널 분리와 classifier 테이블화

목표:

- reason을 `BLOCKING`, `WAITING`, `INFO` 채널로 분리하고, dashboard classifier를 substring 대신 명시 테이블로 바꾼다.

작업:

- `services/operator/reason_classifier.py`에 explicit reason map을 추가한다.
- `CANDIDATE_WITHOUT_ORDER_PLAN`은 `SOFT_WAIT`로 분류한다.
- `MARKET_TICK_FRESH`, `THEME_FOLLOWER_MEMBER` 같은 정보성 reason은 INFO 채널로 보낸다.
- `RISK_BLOCKED_BY_CONDITION`처럼 실제 차단 reason은 BLOCKING 채널로 둔다.
- No-Buy Sentinel snapshot에 channel별 reason summary를 추가한다.

테스트:

- `tests/test_no_buy_sentinel.py`
- operator reason classifier 신규 테스트
- dashboard service snapshot tests

Definition of Done:

- dashboard의 primary block이 대기성 reason을 hard block으로 표시하지 않는다.
- 같은 후보에 대해 blocking/waiting/info reason이 분리되어 보인다.
- substring fallback은 unknown reason용 최후 수단으로만 남는다.

### PR-NB9. Flag/deprecated path 정리

목표:

- profile capability가 자리 잡은 뒤 중복 env flag와 문서를 정리한다.

작업:

- `TRADING_PROFILE` 중심 운영 문서로 통합한다.
- deprecated flag warning을 추가하거나 `.env.example`에 deprecated 표시를 단다.
- old observe-only 문구가 LIVE_SIM pilot capability와 충돌하지 않게 수정한다.

Definition of Done:

- 운영자가 env flag 30개 조합 대신 profile과 소수 override만 보면 된다.
- safety 문구는 `LIVE_REAL 금지`, `LIVE_SIM은 모의투자`, `AI advisory only`를 유지한다.

## 권장 진행 순서

기본 순서:

```text
PR-NB0 Roadmap
PR-NB1 No-Buy Funnel
PR-NB2 Deadlock/limit hotfix
PR-NB3 TradingProfile capability matrix
PR-NB4 observe_only/domain transition
PR-NB5 Admission service
PR-NB6 Condition fusion channels
PR-NB7 Signal-role entry routes
PR-NB8 Reason classifier channels
PR-NB9 Deprecated flag cleanup
```

운영 unblock이 급하면 `PR-NB2`는 `PR-NB1` 직후 바로 진행한다. 단, `PR-NB1` 없이 `PR-NB6` 또는
`PR-NB7`로 들어가면 어떤 gate가 실제 무매수 주범인지 다시 흐려진다.

## PR size guardrail

- 한 PR은 원칙적으로 한 게이트 또는 한 데이터 계약만 바꾼다.
- schema 확장이 있으면 reader를 먼저 backward compatible하게 만든다.
- behavior change PR은 반드시 before/after fixture를 둔다.
- dashboard 문구만 바꾸는 PR은 금지한다. 저장 데이터 또는 classifier 계약이 함께 바뀌어야 한다.
- LIVE_SIM gateway command 경로를 만지는 PR은 simulation account/server guard test를 반드시 포함한다.

## 공통 검증 명령

PR별로 관련 테스트를 좁게 돌린 뒤, 최소 smoke로 아래를 확인한다.

```powershell
python -m pytest tests/test_config.py
python -m pytest tests/test_no_buy_sentinel.py
python -m pytest tests/test_oms_dry_run.py
python -m pytest tests/test_live_sim_order_plan_pipeline.py
python -m pytest tests/test_entry_timing.py
```

전체 회귀가 필요하면 다음을 사용한다.

```powershell
python -m pytest
```

## 완료 기준

이 로드맵이 끝난 상태는 "무조건 매수한다"가 아니다. 완료 기준은 다음이다.

- 장중 조건 hit부터 LIVE_SIM intent까지 stage별 생존율이 보인다.
- LIVE_SIM pilot profile에서 dry-run shadow evidence와 simulation intent가 충돌하지 않는다.
- LEADER/BREAKOUT 신호가 pullback-only 정책 때문에 구조적으로 전멸하지 않는다.
- dashboard는 hard block, soft wait, info를 분리해 보여준다.
- `LIVE_REAL`은 여전히 미구현/금지 상태다.
