# Theme Service

PR 5 adds a read-only theme observation layer. Theme membership and theme snapshots are derived
context for later observation workflows. They do not produce candidates, strategy decisions, risk
decisions, order intents, broker commands, or order API calls.

## Purpose

- Store and query source-typed theme membership.
- Aggregate Market Data Service projections at theme level.
- Summarize coverage, rising ratio, trade-value flow, VWAP position, and member roles.
- Provide PR 6 Candidate FSM with observation context only.

Manual/file-based seed data is bootstrap/testing only. It exists to verify the membership import
contract and snapshot rebuild path. Production membership is expected to come later from legacy
exports, external reference importers, or condition-derived sources; PR 5 does not implement those
collectors and does not scrape external sites.

## Membership Schema

`themes` stores one row per theme:

- `theme_id`, `theme_name`
- `source_type`, `source_name`
- `active`
- `metadata_json`
- `created_at`, `updated_at`

`theme_members` stores one row per `(theme_id, code)`:

- `theme_id`, `code`, `name`
- `source_type`, `source_name`
- `active`
- `weight`
- `metadata_json`
- `created_at`, `updated_at`

`source_type` is required and supports:

- `MANUAL`
- `IMPORTED`
- `NAVER_REFERENCE`
- `CONDITION_DERIVED`
- `MOCK`

## Import Payload

```json
{
  "source_type": "MANUAL",
  "source_name": "local_seed",
  "themes": [
    {
      "theme_id": "semiconductor",
      "theme_name": "반도체",
      "members": [
        {"code": "005930", "name": "삼성전자"},
        {"code": "000660", "name": "SK하이닉스"}
      ]
    }
  ]
}
```

Import behavior:

- stock codes are normalized through the broker contract validator;
- duplicate imports are idempotent upserts;
- `replace=false` keeps existing members;
- `replace=true` only inactivates members in the same theme/source scope before upserting the new
  payload;
- import batches are recorded in `theme_import_batches`;
- invalid imports record an error batch when the source and theme list can be parsed.

## Snapshot Tables

- `theme_snapshots`: one persisted theme-level summary per `snapshot_id`.
- `theme_snapshot_members`: per-member observation rows for each snapshot.
- `theme_latest_snapshots`: latest snapshot pointer and summary per theme.
- `theme_projection_errors`: member/theme projection errors that did not block other members.

Snapshots are rebuildable derived projections. Market Data Service and Event Store remain the
source of truth.

## Snapshot Calculation

For active members, the service reads:

- `market_ticks_latest`
- latest `market_minute_bars` for 60, 180, and 300 seconds
- `get_market_data_readiness()`
- `market_condition_latest` as auxiliary observation metadata

Member fields include latest price, change rate, cumulative trade value, 1/3/5 minute
trade-value deltas, 1 minute volume delta, execution strength, VWAP, `above_vwap`, readiness,
tick age, event timestamp, and member role.

Theme fields include:

- active/observed/fresh member counts;
- fresh coverage ratio;
- rising count and rising ratio;
- average and max change rate;
- total cumulative trade value;
- 1/3/5 minute trade-value delta sums;
- leading code/name;
- co-leader and follower code lists;
- state, quality, and reason codes.

## Member Score Formula

PR 5 uses a deterministic observation score:

```text
score =
  change_rate * 100
  + trade_value_delta_1m / 1,000,000
  + trade_value_delta_3m / 3,000,000
  + cumulative_trade_value / 1,000,000,000
  + execution_strength / 100
```

If the member is observed but not fresh, the score is multiplied by `0.5`. Missing members score
below observed members. The score is only for ranking the snapshot's observation roles.

Role assignment:

- top fresh observed member: `LEADER_CANDIDATE`;
- fresh observed members at or above `THEME_CO_LEADER_SCORE_RATIO` of leader score:
  `CO_LEADER_CANDIDATE`;
- other fresh rising members: `FOLLOWER_CANDIDATE`;
- fresh observed non-rising members: `LAGGARD`;
- stale/degraded observed members: `STALE`;
- missing members: `UNKNOWN`.

## ThemeState Rules

Initial deterministic rules:

- active members below `THEME_MIN_ACTIVE_MEMBERS`:
  `DATA_WAIT`, `MISSING_MEMBERSHIP`, `INSUFFICIENT_MEMBERSHIP`
- no observed members:
  `DATA_WAIT`, `DATA_WAIT`, `NO_OBSERVED_MEMBERS`
- fresh coverage below `THEME_MIN_FRESH_COVERAGE_RATIO`:
  `DATA_WAIT`, `PARTIAL`, `LOW_FRESH_COVERAGE`
- rising ratio at or above `THEME_LEADING_RISING_RATIO`, leader exists, leader change rate at or
  above `THEME_LEADER_MIN_CHANGE_RATE`, leader 1 minute trade-value delta at or above
  `THEME_LEADER_MIN_TRADE_VALUE_DELTA_1M`, and total trade value at or above
  `THEME_MIN_TOTAL_TRADE_VALUE`:
  `LEADING`, `FRESH`
- rising ratio at or above `THEME_SPREADING_RISING_RATIO`:
  `SPREADING`, `FRESH`
- otherwise, with observations:
  `WATCH`, `FRESH`

`FADING` and `ROTATED_OUT` are enum values reserved for later hysteresis/time-based rules.

## API

- `GET /api/themes/status`
- `GET /api/themes?active_only=true`
- `GET /api/themes/{theme_id}?include_members=true`
- `GET /api/themes/{theme_id}/members`
- `GET /api/themes/by-code/{code}`
- `GET /api/themes/snapshots/latest?limit=100&state=LEADING`
- `GET /api/themes/{theme_id}/snapshot/latest?include_members=true`
- `GET /api/themes/{theme_id}/snapshots?limit=100`
- `GET /api/themes/projection-errors?limit=100`
- `POST /api/themes/import?replace=false`
- `POST /api/themes/snapshots/rebuild?theme_id=semiconductor`

POST endpoints are import/rebuild only and use the local token dependency when
`TRADING_CORE_TOKEN` is set. There are no theme endpoints for buying, selling, candidate creation,
strategy execution, risk execution, OMS behavior, or order enqueueing.

## Rebuild Procedure

Import sample membership:

```powershell
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
```

Rebuild every active theme:

```powershell
python -m tools.rebuild_theme_snapshots
```

Rebuild one theme:

```powershell
python -m tools.rebuild_theme_snapshots --theme-id semiconductor
```

HTTP rebuild:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/themes/snapshots/rebuild?theme_id=semiconductor" `
  -Method Post `
  -Headers @{"X-Core-Token" = $env:TRADING_CORE_TOKEN}
```

## Market Data Connection

The Theme Service reads Market Data Service projection tables. It does not ingest Gateway events
directly and does not mutate market data. A typical local verification flow is:

```powershell
python -m apps.mock_gateway --core-url http://127.0.0.1:8000 --once
python -m tools.import_theme_memberships --file data/themes/sample_themes.json
python -m tools.rebuild_theme_snapshots --theme-id semiconductor
Invoke-RestMethod http://127.0.0.1:8000/api/themes/snapshots/latest
```

If only one member has a fresh tick, snapshots still persist with partial coverage/state according
to the deterministic rules.

## Forbidden Scope

PR 5 does not implement:

- external web crawling or reference scraping;
- broker runtime integration;
- ActiveX or UI automation imports;
- Candidate FSM;
- strategy engine;
- risk gate;
- OMS;
- order intent;
- send, cancel, or modify order paths;
- public order enqueue endpoint;
- OpenAI API calls;
- AI Sidecar context builder;
- automatic trading decisions from theme snapshots.
