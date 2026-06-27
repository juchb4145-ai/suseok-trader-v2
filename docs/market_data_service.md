# Market Data Service

PR 4 adds a read-only market data projection over accepted Gateway observations. The Event Store
is the source of truth. Market data tables are derived state and can be rebuilt from
`gateway_events` at any time.

## Purpose

- Normalize accepted `price_tick`, `condition_event`, and `tr_response` Gateway events.
- Keep latest tick, tick samples, 1/3/5 minute bars, VWAP, condition signals, TR snapshots, and
  projection errors in SQLite.
- Provide stable read APIs for Theme Snapshot, Candidate FSM, future Strategy, and Dashboard work.
- Avoid strategy, risk, OMS, order intent, and broker order side effects.

## Projection Tables

- `market_ticks_latest`: one latest normalized tick per stock code.
- `market_tick_samples`: append-only per-event tick sample rows.
- `market_minute_bars`: OHLCV-style bars keyed by `(code, interval_sec, bucket_start)`.
- `market_condition_signals`: append-only condition enter/exit observations.
- `market_condition_latest`: latest condition state per `(condition_id, code)`.
- `market_tr_snapshots`: append-only TR response rows.
- `market_projection_errors`: projection failures that did not block event ingest.

## Price Tick Handling

`process_gateway_event()` sends `price_tick` events to `BrokerPriceTick.from_dict()` first. The
broker-neutral contract validates code, numeric fields, spread, day range, and timestamps.

For accepted ticks the service:

- upserts `market_ticks_latest`;
- inserts one `market_tick_samples` row using `event_id` as the dedupe key;
- computes `volume_delta` and `trade_value_delta` against the previous latest tick;
- clamps negative deltas to zero for reset-like cumulative values;
- updates configured minute bars, defaulting to `60,180,300` seconds;
- stores quality as `FRESH`, `DEGRADED`, or `INVALID` according to tick shape.

## Condition Event Handling

`condition_event` payloads are validated by `BrokerConditionEvent.from_dict()`. The service stores
the raw signal in `market_condition_signals`, then upserts `market_condition_latest` for the same
condition/code pair. `ENTER` and `EXIT` actions are preserved exactly as observations. Metadata is
stored as canonical JSON.

## TR Response Handling

`tr_response` payloads are validated by `BrokerTrResponse.from_dict()`. Each row is stored in
`market_tr_snapshots`. If a row contains `code`, `stock_code`, or `종목코드`, the value is normalized
to a 6-digit domestic stock code when valid.

TR rows never overwrite latest tick data. They are market observations only.

## Minute Bars And VWAP

Minute buckets are calculated from the tick trade time. For each configured interval, the service
keeps:

- `open`: first price in the bucket;
- `high`: max price in the bucket;
- `low`: min price in the bucket;
- `close`: latest price in the bucket;
- `volume_delta` and `trade_value_delta`: sum of tick deltas in the bucket;
- `tick_count`: number of projected ticks in the bucket;
- `vwap`: cumulative trade value divided by cumulative volume when possible, otherwise bar
  trade-value delta divided by bar volume delta.

## Freshness And Readiness

Readiness is calculated dynamically by `get_market_data_readiness()`:

- no latest tick: `MISSING` with `TICK_MISSING`;
- latest tick age within `MARKET_DATA_TICK_STALE_SEC`: `FRESH`;
- age past stale threshold but within `MARKET_DATA_DEGRADED_TICK_STALE_SEC`: `STALE`;
- age past degraded threshold: `DEGRADED`;
- invalid tick shape: `INVALID`.

Readiness also reports whether 1m, 3m, and 5m bars exist, whether any VWAP is available, and
reason codes such as `BAR_MISSING`, `BAR_MISSING_60`, and `VWAP_MISSING`.

## Theme Snapshot Connection

PR 5 Theme Service reads Market Data Service projections as a downstream derived projection. It
aggregates latest ticks, minute bars, readiness, VWAP, and latest condition observations by theme
membership. Market Data Service remains read-only and does not know about theme state, candidate
state, strategy decisions, risk decisions, or order routing.

Theme snapshots are rebuilt separately through:

```powershell
python -m tools.rebuild_theme_snapshots
```

## Candidate FSM Connection

PR 6 Candidate FSM reads Market Data Service projections as read-only context. It uses
`market_condition_signals` for condition source events, `market_condition_latest` for condition
attribution, `market_ticks_latest` for latest tick age, `market_minute_bars` for 1/3/5 minute bar
and VWAP readiness, and `get_market_data_readiness()` for status and reason codes.

Market Data Service still does not create candidate state itself, does not enqueue commands, and
does not make strategy, risk, or order decisions.

## Rebuild

Use the CLI to replay accepted market events:

```powershell
python -m tools.rebuild_market_data_projection
```

This default mode is non-destructive and skips already projected events. To clear projection
tables first:

```powershell
python -m tools.rebuild_market_data_projection --clear-projection
```

The underlying service requires an explicit clear guard before deleting projection rows.

## Read-Only API

- `GET /api/market-data/status`
- `GET /api/market-data/ticks/latest`
- `GET /api/market-data/ticks/{code}`
- `GET /api/market-data/bars/{code}`
- `GET /api/market-data/readiness/{code}`
- `GET /api/market-data/conditions/recent`
- `GET /api/market-data/tr-snapshots/recent`
- `GET /api/market-data/projection-errors`

There are no POST, PUT, DELETE, order, strategy, risk, or OMS endpoints in Market Data Service.

## Forbidden Scope

PR 4 does not implement:

- Kiwoom OpenAPI+ runtime code;
- PyQt5 or QAxWidget imports;
- strategy engine;
- risk gate;
- OMS;
- order intent;
- candidate FSM;
- send, cancel, or modify order paths;
- OpenAI API calls;
- AI Sidecar context builder.
