# Risk Gate Observe-Only

PR 8 adds a deterministic observe-only risk classification layer. It reads PR 7
`StrategyObservation` rows plus Candidate, Market Data, and Theme Snapshot context, then stores
read-only risk observations for PR 9 Dashboard and operator review.

## Purpose

- Convert strategy observations into deterministic risk observations.
- Record data, market, theme, candidate, strategy, chase, liquidity, duplicate, and placeholder
  risk notes.
- Preserve a latest projection by `candidate_instance_id`.
- Keep all results read-only before the later OMS and DRY_RUN work.

## Observe-Only Principles

- Risk observations always serialize with `observe_only=true`.
- `OBSERVE_PASS` is not order approval.
- `OBSERVE_CAUTION` is an operator review note.
- `OBSERVE_BLOCK` is an observed block reason, not an execution command.
- Risk Gate does not create OrderIntent, EntryPlan, PositionSizing, OMS state, GatewayCommand
  rows, public order APIs, send/cancel/modify order calls, or live flag changes.
- AI Sidecar output is not an input to Risk evaluation.
- Risk thresholds are configuration/code-review inputs and are not changed automatically intraday.

## Strategy Engine Connection

Risk evaluation reads:

- `strategy_observations_latest`
- `strategy_observations`
- `strategy_setup_observations`
- `candidates`
- `candidate_context_latest`
- `market_ticks_latest`
- `market_minute_bars`
- `theme_latest_snapshots`
- `theme_snapshot_members`

`MATCHED_OBSERVATION` remains a setup classifier result. Risk may record a pass-observed
strategy-context check for it, but that does not make it buy readiness.

## RiskObservationStatus

- `NOT_EVALUATED`: no meaningful evaluation was produced.
- `DATA_WAIT`: critical observation data is missing.
- `OBSERVE_PASS`: no block or caution was observed; not order approval.
- `OBSERVE_CAUTION`: one or more caution checks were observed.
- `OBSERVE_BLOCK`: one or more block checks were observed.
- `INVALID_CONTEXT`: core context is inconsistent or missing.
- `STALE_CONTEXT`: critical context is too old.

## RiskCategory

- `DATA_QUALITY`
- `MARKET_CONTEXT`
- `THEME_CONTEXT`
- `CANDIDATE_CONTEXT`
- `STRATEGY_CONTEXT`
- `CHASE_OVERHEAT`
- `LIQUIDITY_SPREAD`
- `DUPLICATE_COOLDOWN`
- `PORTFOLIO_PLACEHOLDER`

## Check Rules

`DATA_QUALITY` observes missing latest tick, stale tick, missing/invalid/stale readiness, missing
1m bars, and missing VWAP. Missing or stale critical data can move the overall observation to
`DATA_WAIT` or `STALE_CONTEXT`.

`MARKET_CONTEXT` is a placeholder in PR 8. It records `MARKET_CONTEXT_UNAVAILABLE` as
`NOT_EVALUATED` and does not block by itself.

`THEME_CONTEXT` observes missing theme context, weak theme states, low fresh coverage, weak rising
ratio, and missing leader evidence.

`CANDIDATE_CONTEXT` observes non-`CONTEXT_READY` states, stale/closed/data-wait candidates, and
missing active sources. It does not mutate Candidate state.

`STRATEGY_CONTEXT` observes missing, stale, forming, not-matched, invalid, and matched strategy
observations. Every result carries reminders that Risk Gate is not order approval.

`CHASE_OVERHEAT` observes high change rate, near-day-high chase risk, high VWAP extension, shallow
pullback evidence from strategy reasons, and unavailable VI data as a placeholder.

`LIQUIDITY_SPREAD` observes wide spread, weak 1m trade-value flow, low cumulative trade value, and
weak execution strength.

`DUPLICATE_COOLDOWN` observes multiple active candidates for the same code and recent risk
observations inside the configured cooldown window. It does not mutate Candidate or Strategy rows.

`PORTFOLIO_PLACEHOLDER` records `PORTFOLIO_CONTEXT_UNAVAILABLE` as `NOT_EVALUATED` because
positions, holdings, balances, and OMS are intentionally absent before PR 10.

## Storage

PR 8 adds these SQLite tables:

- `risk_observations`
- `risk_observations_latest`
- `risk_check_observations`
- `risk_evaluation_runs`
- `risk_evaluation_errors`

History rows are inserted into `risk_observations`. Latest rows are upserted by
`candidate_instance_id`. Check rows are stored per risk observation. Runs and per-target errors are
recorded so batch evaluation can continue when one candidate fails.

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

`POST /api/risk/evaluate` is an observation projection endpoint. It requires the local token when
`TRADING_CORE_TOKEN` is set and returns `order_routing_enabled=false`.

## CLI

Evaluate current matched strategy observations:

```powershell
python -m tools.evaluate_risk
```

Evaluate one candidate:

```powershell
python -m tools.evaluate_risk --candidate-instance-id CAND-2026-06-27-005930-1
```

Evaluate one strategy observation:

```powershell
python -m tools.evaluate_risk --strategy-observation-id strategy_observation_xxx
```

Inspect latest risk observation:

```powershell
python -m tools.inspect_risk_observation `
  --candidate-instance-id CAND-2026-06-27-005930-1
```

Inspect by risk observation id:

```powershell
python -m tools.inspect_risk_observation `
  --risk-observation-id risk_observation_xxx
```

## Forbidden Scope

PR 8 does not implement:

- Kiwoom OpenAPI+ runtime code;
- PyQt5 or QAxWidget imports;
- OMS;
- OrderIntent;
- EntryPlan;
- PositionSizing;
- Position or Portfolio services;
- send, cancel, or modify order paths;
- public order enqueue endpoint;
- GatewayCommand creation from Risk;
- OpenAI API calls;
- AI Sidecar context builder;
- automatic buy or sell decisions from risk observations;
- LIVE_SIM or LIVE_REAL flag changes from risk observations.
