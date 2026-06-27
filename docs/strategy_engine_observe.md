# Strategy Engine Observe-Only

## 요약

Strategy Engine은 PR6 Candidate context를 읽어 deterministic setup observation을 저장한다. `StrategyObservation`은 entry plan, position size, risk approval, order request가 아니다. `MATCHED_OBSERVATION`은 setup classifier가 관찰 규칙에 맞았다는 뜻이며 매수 신호가 아니다.

## Observe-only 원칙

- `observe_only=true`가 항상 유지된다.
- `MATCHED_OBSERVATION`은 매수 신호가 아니다.
- `FORMING`과 `MATCHED_OBSERVATION`은 실행을 trigger하지 않는다.
- Risk Gate는 Strategy 결과를 read-only input으로만 읽는다.
- AI Sidecar output은 Strategy evaluation input이 아니다.
- Strategy threshold는 config/code-review 입력이지 intraday self-modifying 값이 아니다.

## 읽는 데이터

- `candidates`
- `candidate_context_latest`
- `market_ticks_latest`
- `market_minute_bars`
- `theme_latest_snapshots`
- `theme_snapshot_members`

Strategy evaluation은 Candidate state를 변경하지 않는다. `CONTEXT_READY`는 setup 관측 입력이 준비되었다는 뜻이다.

## Setup Types

| Setup | 운영자용 설명 |
| --- | --- |
| `THEME_LEADER_PULLBACK` | leading/spreading theme의 leader/co-leader/follower가 pullback range와 trade-value flow를 보이는지 관찰 |
| `VWAP_RECLAIM` | 가격이 VWAP 위/근처에 있고 거래대금 흐름이 있는지 관찰 |
| `BREAKOUT_RETEST` | 가격이 day high 또는 short-term high proxy 근처인지 관찰 |
| `THEME_FOLLOWER_EXPANSION` | follower가 theme rising ratio와 함께 확산 참여 중인지 관찰 |

이 setup 이름들은 관측 분류 이름이다. 주문 지시가 아니다.

## StrategyObservationStatus

| Status | 의미 | 주의 |
| --- | --- | --- |
| `NOT_EVALUATED` | candidate state가 평가 대상 밖 | 주문 판단 아님 |
| `DATA_WAIT` | market/bar/VWAP/theme/context 부족 | 데이터 대기 |
| `NO_SETUP` | setup rule 미충족 | 매도/취소 지시 아님 |
| `WATCH` | 일부 evidence 존재 | 관찰 지속 |
| `FORMING` | setup evidence 형성 중 | 실행 trigger 아님 |
| `MATCHED_OBSERVATION` | setup observation rule match | 매수 신호 아님 |
| `INVALID_CONTEXT` | stored context 불일치 | 데이터 점검 필요 |
| `STALE_CONTEXT` | candidate/market freshness 초과 | 오래된 관찰 |

## Setup Rule 요약

### `THEME_LEADER_PULLBACK`

- theme state가 `LEADING` 또는 `SPREADING`이어야 한다.
- role이 `LEADER_CANDIDATE`, `CO_LEADER_CANDIDATE`, `FOLLOWER_CANDIDATE` 중 하나여야 한다.
- `(day_high - price) / day_high * 100`로 pullback을 계산한다.
- configured min/max range와 trade-value flow가 맞으면 `MATCHED_OBSERVATION`이 될 수 있다.

### `VWAP_RECLAIM`

- `STRATEGY_ENGINE_REQUIRE_VWAP=true`이고 VWAP이 없으면 `DATA_WAIT`.
- `PRICE_ABOVE_VWAP`, `PRICE_BELOW_VWAP` evidence를 기록한다.
- `STRATEGY_VWAP_RECLAIM_TOLERANCE_PCT`로 near-VWAP 여부를 본다.

### `BREAKOUT_RETEST`

- `(day_high - price) / day_high * 100`를 계산한다.
- `STRATEGY_BREAKOUT_RETEST_NEAR_HIGH_PCT`로 near-high evidence를 본다.
- day high 또는 price evidence가 없으면 `DATA_WAIT`.

### `THEME_FOLLOWER_EXPANSION`

- theme state는 `LEADING` 또는 `SPREADING`.
- role은 `FOLLOWER_CANDIDATE`.
- `STRATEGY_FOLLOWER_EXPANSION_MIN_THEME_RISING_RATIO` 이상인지 본다.

## Score And Confidence

`score`는 setup 관측 강도다. `confidence`는 데이터 완성도와 관측 품질이다. 둘 다 기대수익률, 매수 확률, position sizing input, risk approval이 아니다.

## PR8 Risk / PR10 DRY_RUN 연결

Risk Gate는 latest `StrategyObservation`을 read-only로 읽는다. Strategy status가 `MATCHED_OBSERVATION`이어도 Risk Gate 결과는 주문 승인이 아니다.

PR10 DRY_RUN은 latest `MATCHED_OBSERVATION`을 eligibility input 중 하나로 읽을 수 있다. 그래도 output은 내부 `DryRunIntent` simulation record일 뿐이며 브로커 주문이 아니다.

## Storage

- `strategy_observations`
- `strategy_observations_latest`
- `strategy_setup_observations`
- `strategy_evaluation_runs`
- `strategy_evaluation_errors`

## API

- `GET /api/strategy/status`
- `GET /api/strategy/observations/latest`
- `GET /api/strategy/candidates/{candidate_instance_id}`
- `GET /api/strategy/candidates/{candidate_instance_id}/history`
- `GET /api/strategy/observations/{strategy_observation_id}/setups`
- `GET /api/strategy/runs`
- `GET /api/strategy/errors`
- `POST /api/strategy/evaluate`

`POST /api/strategy/evaluate`는 projection evaluation endpoint다. 주문 endpoint가 아니다.

## CLI

```powershell
python -m tools.evaluate_strategy --trade-date 2026-06-27
python -m tools.evaluate_strategy --candidate-instance-id CAND-2026-06-27-005930-1
python -m tools.inspect_strategy_observation `
  --candidate-instance-id CAND-2026-06-27-005930-1
```

## 운영자 체크포인트

- `MATCHED_OBSERVATION`을 매수 신호로 해석하지 않는다.
- `score`와 `confidence`를 수익 확률로 해석하지 않는다.
- Strategy result가 없으면 Candidate가 `CONTEXT_READY`인지 먼저 확인한다.
- Strategy Engine은 `OrderIntent`, `GatewayCommand`, `send_order`를 만들지 않는다.
