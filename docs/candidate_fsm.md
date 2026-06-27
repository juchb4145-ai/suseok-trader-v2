# Candidate FSM

PR 6 adds an observe-only Candidate FSM. A Candidate is an observation episode, not a buy
candidate. PR 7 Strategy Engine reads `CONTEXT_READY` candidate context as a read-only input and
stores strategy observations without touching Gateway commands, OMS, Risk, or order APIs.

## Purpose

- Convert condition signals, theme snapshots, and market observations into candidate sources.
- Create deterministic `candidate_instance_id` episodes.
- Track lifecycle state through a deterministic FSM.
- Store source events, source latest rows, state transitions, context latest rows, and projection
  errors.
- Expose read-only Candidate context for Strategy observation evaluation.

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

Condition sources are read from `market_condition_signals`. Theme sources are read from
`theme_latest_snapshots` joined to `theme_snapshot_members`, filtered by
`CANDIDATE_THEME_SOURCE_STATES` and `CANDIDATE_THEME_MEMBER_ROLES`.

## States

- `DETECTED`: a source first observed the episode.
- `HYDRATING`: latest tick, bar, readiness, and theme/source context are being gathered.
- `WATCHING`: minimum observation data exists, before strategy setup evaluation.
- `CONTEXT_READY`: read-only context is ready for PR 7 Strategy evaluation.
- `DATA_WAIT`: required observation data is missing.
- `BLOCKED_OBSERVATION`: observation should be excluded or held for a clear reason.
- `STALE`: source or tick freshness exceeded configured stale thresholds.
- `COOLDOWN`: reserved for episode churn control.
- `CLOSED`: source exit, no active source, TTL expiry, or theme rotation ended the episode.

`CONTEXT_READY` is not buy readiness. It only means the observation context can be read by PR 7
Strategy Engine. PR 6 has no setup validation, entry readiness, score, risk pass/fail, order
intent, or order command.

## Identity And Generation

The active episode key is `(trade_date, code)`. If no active episode exists, the service creates:

```text
CAND-{trade_date}-{code}-{generation}
```

Generation starts at `1` per `trade_date + code` and increments after a prior episode is
`CLOSED`. If the same code is detected while an episode is active, the source is merged into the
existing candidate.

## Transition Rules

- New source with no active candidate creates `DETECTED`, then moves to `HYDRATING`.
- Duplicate source events are ignored by `source_event_id`.
- `HYDRATING -> DATA_WAIT` when latest tick, market readiness, required bar, or required theme
  context is missing.
- `HYDRATING -> WATCHING` when latest tick, non-missing readiness, and source attribution exist.
- `DATA_WAIT -> WATCHING` when missing data recovers.
- `WATCHING -> CONTEXT_READY` when readiness is not missing/stale/invalid, 1m bar exists when
  required, VWAP exists when required, and source-specific context is present.
- Active states move to `STALE` when source or tick age exceeds candidate stale settings.
- Active states move to `CLOSED` when active source count is zero, TTL expires, condition exit is
  the only remaining source condition, an explicit close is requested internally, or a theme is
  rotated out.

Repeated refreshes that do not change state do not create duplicate transition rows.

## Storage

PR 6 adds these SQLite tables:

- `candidates`
- `candidate_source_events`
- `candidate_sources_latest`
- `candidate_state_transitions`
- `candidate_context_latest`
- `candidate_projection_errors`

`candidate_state_transitions` is the authoritative Candidate event log for PR 6. It is separate
from Gateway transport events; Candidate FSM does not write `gateway_events` or enqueue
`gateway_commands`.

## Context Refresh

`refresh_candidate_context()` reads:

- latest tick and readiness from Market Data Service;
- 60/180/300 second bar presence and VWAP readiness;
- condition attribution from active condition sources;
- theme snapshot/member rows for active theme sources;
- active and total source counts.

The resulting read-only context is stored in `candidate_context_latest` as `theme_context_json`,
`market_context_json`, `source_context_json`, and `readiness_json`.

## PR 7 Strategy Connection

Strategy Engine observe-only reads candidate rows and `candidate_context_latest` when evaluating
candidate setup observations. It also reads Market Data projection rows and Theme Snapshot rows as
read-only evidence. Strategy evaluation does not mutate Candidate state, does not convert
`CONTEXT_READY` into buy readiness, and does not create Gateway commands or order API calls.

When a Strategy setup reaches `MATCHED_OBSERVATION`, the Candidate remains an observation episode.
Risk Gate and OMS behavior are intentionally absent before later PRs.

## API

- `GET /api/candidates/status`
- `GET /api/candidates`
- `GET /api/candidates/{candidate_instance_id}`
- `GET /api/candidates/{candidate_instance_id}/sources`
- `GET /api/candidates/{candidate_instance_id}/transitions`
- `GET /api/candidates/by-code/{code}`
- `GET /api/candidates/projection-errors`
- `POST /api/candidates/rebuild`

`POST /api/candidates/rebuild` is a projection rebuild endpoint. It uses the local token dependency
when `TRADING_CORE_TOKEN` is configured and does not create manual watch candidates.

## Rebuild

CLI:

```powershell
python -m tools.rebuild_candidates
```

HTTP:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/candidates/rebuild `
  -Method Post `
  -Headers @{"X-Core-Token" = $env:TRADING_CORE_TOKEN}
```

Inspect one episode:

```powershell
python -m tools.inspect_candidate --candidate-instance-id CAND-2026-06-27-005930-1 `
  --include-context --include-sources --include-transitions
```

## Forbidden Scope

PR 6 does not implement:

- Kiwoom OpenAPI+ runtime code;
- PyQt5 or QAxWidget imports;
- Strategy Engine;
- Risk Gate;
- OMS;
- OrderIntent;
- EntryPlan;
- Position or position sizing;
- send, cancel, or modify order paths;
- public order enqueue endpoint;
- GatewayCommand creation from Candidate FSM;
- OpenAI API calls;
- AI Sidecar context builder;
- automatic buy/sell decisions from candidates.

PR 7 keeps this boundary: Strategy observations are stored separately in strategy projection
tables and never write Candidate states such as buy-ready or order-ready.
