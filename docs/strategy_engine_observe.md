# Strategy Engine Observe-Only

PR 7 adds deterministic strategy observation over PR 6 Candidate FSM context. It classifies setup
observations and stores them for later review. It does not create an entry plan, position size,
risk approval, Gateway command, or public order request.

## Purpose

- Read `CONTEXT_READY` candidate episodes from Candidate FSM.
- Combine candidate context with Market Data projection and Theme Snapshot evidence.
- Classify setup observations deterministically.
- Store latest and historical `StrategyObservation` rows for PR 8 Risk Gate observe-only.
- Keep every result read-only and reviewable.

## Observe-Only Principles

- `observe_only` is always true in strategy observations.
- `MATCHED_OBSERVATION` is not buy readiness.
- `FORMING` and `MATCHED_OBSERVATION` do not trigger execution.
- Strategy thresholds are configuration/code-review inputs, not intraday self-modifying values.
- AI Sidecar output is not an input to Strategy evaluation.

## Candidate FSM Connection

Strategy evaluation reads:

- `candidates`
- `candidate_context_latest`
- `market_ticks_latest`
- `market_minute_bars`
- `theme_latest_snapshots`
- `theme_snapshot_members`

Candidate state is not changed by Strategy evaluation. `CONTEXT_READY` only means the context is
ready for setup observation; it is not a buy, entry, or order-ready state.

## Setup Types

- `THEME_LEADER_PULLBACK`: observes whether a leader, co-leader, or follower in a leading or
  spreading theme is in a configured pullback range with trade-value flow.
- `VWAP_RECLAIM`: observes whether price is above and near VWAP. The first implementation treats
  above-VWAP plus near-VWAP as reclaim observation because tick sequence history is not yet used.
- `BREAKOUT_RETEST`: observes whether price is near the day high or short-term high proxy.
- `THEME_FOLLOWER_EXPANSION`: observes whether a follower in a leading or spreading theme is
  participating while theme rising ratio is strong enough.

## StrategyObservationStatus

- `NOT_EVALUATED`: candidate state is outside the allowed observation states.
- `DATA_WAIT`: required market, bar, VWAP, theme, or candidate context is missing.
- `NO_SETUP`: setup rules are not satisfied.
- `WATCH`: some evidence is present, but the setup is not forming strongly.
- `FORMING`: setup evidence is developing.
- `MATCHED_OBSERVATION`: setup observation rule matched. This is not a buy signal.
- `INVALID_CONTEXT`: candidate claims readiness but required stored context is inconsistent.
- `STALE_CONTEXT`: candidate or market freshness is too old.

## Setup Rules

`THEME_LEADER_PULLBACK`:

- Requires theme state in `LEADING` or `SPREADING`.
- Requires theme role in `LEADER_CANDIDATE`, `CO_LEADER_CANDIDATE`, or `FOLLOWER_CANDIDATE`.
- Computes `(day_high - price) / day_high * 100`.
- Matches when pullback is inside configured min/max range and trade-value flow is observed.
- Returns `DATA_WAIT` when price or high evidence is missing.

`VWAP_RECLAIM`:

- Returns `DATA_WAIT` when VWAP is missing and `STRATEGY_ENGINE_REQUIRE_VWAP=true`.
- Observes `PRICE_ABOVE_VWAP` or `PRICE_BELOW_VWAP`.
- Uses `STRATEGY_VWAP_RECLAIM_TOLERANCE_PCT` for near-VWAP evidence.
- Matches when price is above VWAP, near VWAP, and trade-value flow is observed.

`BREAKOUT_RETEST`:

- Computes `(day_high - price) / day_high * 100`.
- Uses `STRATEGY_BREAKOUT_RETEST_NEAR_HIGH_PCT` for near-high evidence.
- Returns `FORMING` or `MATCHED_OBSERVATION` when near-high evidence exists.
- Returns `DATA_WAIT` when day high or price evidence is missing.

`THEME_FOLLOWER_EXPANSION`:

- Requires theme state in `LEADING` or `SPREADING`.
- Requires role `FOLLOWER_CANDIDATE`.
- Uses `STRATEGY_FOLLOWER_EXPANSION_MIN_THEME_RISING_RATIO`.
- Matches when theme rising ratio and member trade-value flow are observed.

## Score And Confidence

`score` is setup observation strength. `confidence` is data completeness and observation quality.
Neither value is an expected return, buy probability, position sizing input, or risk approval.

## Storage

PR 7 adds:

- `strategy_observations`
- `strategy_observations_latest`
- `strategy_setup_observations`
- `strategy_evaluation_runs`
- `strategy_evaluation_errors`

The historical table keeps evaluated observations. The latest table is upserted by
`candidate_instance_id`. Setup observations are stored per strategy observation. Batch runs and
candidate-level errors are recorded without aborting the whole run.

## API

- `GET /api/strategy/status`
- `GET /api/strategy/observations/latest`
- `GET /api/strategy/candidates/{candidate_instance_id}`
- `GET /api/strategy/candidates/{candidate_instance_id}/history`
- `GET /api/strategy/observations/{strategy_observation_id}/setups`
- `GET /api/strategy/runs`
- `GET /api/strategy/errors`
- `POST /api/strategy/evaluate`

`POST /api/strategy/evaluate` is a projection evaluation endpoint. It requires the local token
when `TRADING_CORE_TOKEN` is set.

## CLI

Evaluate current candidates:

```powershell
python -m tools.evaluate_strategy --trade-date 2026-06-27
```

Evaluate one candidate:

```powershell
python -m tools.evaluate_strategy --candidate-instance-id CAND-2026-06-27-005930-1
```

Inspect latest observation:

```powershell
python -m tools.inspect_strategy_observation `
  --candidate-instance-id CAND-2026-06-27-005930-1
```

Inspect setup rows by observation id:

```powershell
python -m tools.inspect_strategy_observation `
  --strategy-observation-id strategy_observation_xxx
```

## Forbidden Scope

PR 7 does not implement:

- Kiwoom OpenAPI+ runtime code;
- PyQt5 or QAxWidget imports;
- Risk Gate;
- OMS;
- OrderIntent;
- EntryPlan;
- Position or position sizing;
- send, cancel, or modify order paths;
- public order enqueue endpoint;
- GatewayCommand creation from Strategy;
- OpenAI API calls;
- AI Sidecar context builder;
- automatic buy or sell decisions from strategy observations.
