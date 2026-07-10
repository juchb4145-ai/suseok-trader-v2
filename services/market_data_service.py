from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.market import BrokerPriceTick
from domain.broker.tr import BrokerTrResponse
from domain.broker.utils import (
    datetime_to_wire,
    normalize_payload,
    parse_timestamp,
    utc_now,
    validate_stock_code,
)
from domain.market.bars import bucket_start_for, calculate_vwap
from domain.market.models import MarketDataQualityStatus
from domain.market.quality import assess_price_tick_quality, freshness_status, tick_age_seconds
from storage.gateway_command_store import canonical_json
from storage.projection_watermarks import (
    ProjectionWatermark,
    advance_projection_watermark,
    get_projection_watermark,
    record_projection_event_result,
    reset_projection_watermark,
)

from services.candidate_quote_refresh import (
    candidate_quote_refresh_tick_payloads_from_tr_response,
)
from services.config import Settings, load_settings

MARKET_DATA_PROJECTION_NAME = "market_data"
MARKET_DATA_EVENT_TYPES: frozenset[str] = frozenset(
    {"price_tick", "condition_event", "tr_response"}
)
MARKET_PROJECTION_TABLES: tuple[str, ...] = (
    "market_ticks_latest",
    "market_tick_samples",
    "market_minute_bars",
    "market_premarket_snapshots",
    "market_cross_exchange_observations",
    "market_condition_signals",
    "market_condition_latest",
    "market_tr_snapshots",
    "market_projection_errors",
)
MARKET_DATA_EXCHANGES: frozenset[str] = frozenset({"KRX", "NXT"})
MARKET_DATA_EXCHANGE_FILTERS: frozenset[str] = frozenset({"KRX", "NXT", "ALL"})
MARKET_DATA_SESSIONS: frozenset[str] = frozenset(
    {"PREMARKET_NXT", "REGULAR", "AFTERMARKET_NXT", "OFF_HOURS"}
)
INVALID_PRICE_TICK_REASON_CODES: frozenset[str] = frozenset({"PRICE_MISSING"})
QUOTE_ONLY_REAL_TYPES: frozenset[str] = frozenset({"주식우선호가"})


@dataclass(frozen=True, kw_only=True)
class MarketDataProcessResult:
    event_id: str
    event_type: str
    status: str
    applied_count: int = 0
    ignored_count: int = 0
    error_count: int = 0
    error_message: str | None = None


@dataclass(frozen=True, kw_only=True)
class MarketDataRebuildResult:
    processed_count: int
    applied_count: int
    ignored_count: int
    error_count: int
    mode: str = "full"
    from_event_rowid: int = 0
    last_event_rowid: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "processed_count": self.processed_count,
            "applied_count": self.applied_count,
            "ignored_count": self.ignored_count,
            "error_count": self.error_count,
            "from_event_rowid": self.from_event_rowid,
            "last_event_rowid": self.last_event_rowid,
        }


def normalize_market_data_exchange_filter(value: object = "KRX") -> str:
    text = str(value or "KRX").strip().upper()
    aliases = {
        "": "KRX",
        "K": "KRX",
        "KR": "KRX",
        "KRX": "KRX",
        "N": "NXT",
        "NX": "NXT",
        "NXT": "NXT",
        "A": "ALL",
        "AL": "ALL",
        "ALL": "ALL",
        "SOR": "ALL",
        "INTEGRATED": "ALL",
    }
    exchange = aliases.get(text)
    if exchange not in MARKET_DATA_EXCHANGE_FILTERS:
        raise ValueError(f"unsupported market data exchange: {value}")
    return exchange


def normalize_market_data_exchange(value: object = "KRX") -> str:
    exchange = normalize_market_data_exchange_filter(value)
    if exchange == "ALL":
        return "KRX"
    return exchange


def market_session_for_tick(trade_time: datetime, exchange: object = "KRX") -> str:
    normalized_exchange = normalize_market_data_exchange(exchange)
    local_time = parse_timestamp(trade_time, "trade_time").astimezone(_seoul_timezone()).time()
    if time(9, 0) <= local_time < time(15, 30):
        return "REGULAR"
    if normalized_exchange == "NXT" and time(8, 0) <= local_time < time(8, 50):
        return "PREMARKET_NXT"
    if normalized_exchange == "NXT" and time(15, 30) <= local_time < time(20, 0):
        return "AFTERMARKET_NXT"
    return "OFF_HOURS"


def process_gateway_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> MarketDataProcessResult:
    resolved_settings = settings or load_settings()
    event_type = event.event_type.strip().lower()
    if not resolved_settings.market_data_enabled:
        return MarketDataProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="DISABLED",
            ignored_count=1,
        )
    if event_type not in MARKET_DATA_EVENT_TYPES:
        return MarketDataProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="IGNORED",
            ignored_count=1,
        )
    if _projection_exists(connection, event_type, event.event_id):
        return _with_market_data_watermark(
            connection,
            event,
            MarketDataProcessResult(
                event_id=event.event_id,
                event_type=event_type,
                status="DUPLICATE",
                ignored_count=1,
            ),
            commit=True,
        )
    try:
        connection.execute("BEGIN IMMEDIATE")
        if event_type == "price_tick":
            applied_count = _process_price_tick(connection, event, resolved_settings)
        elif event_type == "condition_event":
            applied_count = _process_condition_event(connection, event)
        else:
            applied_count = _process_tr_response(connection, event, resolved_settings)
        projection_error = _projection_error_message(connection, event.event_id)
        if projection_error is None:
            record_projection_event_result(
                connection,
                projection_name=MARKET_DATA_PROJECTION_NAME,
                event_id=event.event_id,
                status="SUCCESS",
                outcome="APPLIED" if applied_count else "IGNORED",
                metadata={"event_type": event_type},
                commit=False,
            )
            _advance_market_data_watermark_for_event(connection, event, commit=False)
        else:
            record_projection_event_result(
                connection,
                projection_name=MARKET_DATA_PROJECTION_NAME,
                event_id=event.event_id,
                status="ERROR",
                outcome="INLINE_PROJECTION_ERROR",
                error_message=projection_error,
                metadata={"event_type": event_type},
                commit=False,
            )
        connection.commit()
    except Exception as exc:
        connection.rollback()
        _record_projection_error(connection, event, error_message=str(exc))
        record_projection_event_result(
            connection,
            projection_name=MARKET_DATA_PROJECTION_NAME,
            event_id=event.event_id,
            status="ERROR",
            outcome="INLINE_PROJECTION_EXCEPTION",
            error_message=str(exc),
            metadata={"event_type": event_type},
            commit=False,
        )
        connection.commit()
        return MarketDataProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="ERROR",
            error_count=1,
            error_message=str(exc),
        )

    if event_type == "price_tick" and applied_count == 0:
        return MarketDataProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="IGNORED",
            ignored_count=1,
        )

    return MarketDataProcessResult(
        event_id=event.event_id,
        event_type=event_type,
        status="APPLIED",
        applied_count=applied_count,
    )


def get_latest_tick(
    connection: sqlite3.Connection,
    code: str,
    *,
    exchange: object = "KRX",
) -> dict[str, Any] | None:
    normalized_code = validate_stock_code(code)
    normalized_exchange = normalize_market_data_exchange(exchange)
    row = connection.execute(
        """
        SELECT *
        FROM market_ticks_latest
        WHERE code = ? AND exchange = ?
        """,
        (normalized_code, normalized_exchange),
    ).fetchone()
    if row is None:
        return None
    return _latest_tick_row_to_dict(row)


def list_latest_ticks_for_code(
    connection: sqlite3.Connection,
    code: str,
    *,
    exchange: object = "KRX",
) -> list[dict[str, Any]]:
    normalized_code = validate_stock_code(code)
    normalized_exchange = normalize_market_data_exchange_filter(exchange)
    if normalized_exchange == "ALL":
        rows = connection.execute(
            """
            SELECT *
            FROM market_ticks_latest
            WHERE code = ?
            ORDER BY exchange ASC
            """,
            (normalized_code,),
        ).fetchall()
    else:
        rows = connection.execute(
            """
            SELECT *
            FROM market_ticks_latest
            WHERE code = ? AND exchange = ?
            ORDER BY exchange ASC
            """,
            (normalized_code, normalized_exchange),
        ).fetchall()
    return [_latest_tick_row_to_dict(row) for row in rows]


def list_latest_ticks(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
    exchange: object = "KRX",
) -> list[dict[str, Any]]:
    bounded_limit = _bounded_limit(limit)
    normalized_exchange = normalize_market_data_exchange_filter(exchange)
    if normalized_exchange == "ALL":
        rows = connection.execute(
            """
            SELECT *
            FROM market_ticks_latest
            ORDER BY updated_at DESC, code ASC, exchange ASC
            LIMIT ?
            """,
            (bounded_limit,),
        ).fetchall()
    else:
        rows = connection.execute(
            """
            SELECT *
            FROM market_ticks_latest
            WHERE exchange = ?
            ORDER BY updated_at DESC, code ASC
            LIMIT ?
            """,
            (normalized_exchange, bounded_limit),
        ).fetchall()
    return [_latest_tick_row_to_dict(row) for row in rows]


def list_bars(
    connection: sqlite3.Connection,
    code: str,
    *,
    exchange: object = "KRX",
    interval_sec: int = 60,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_code = validate_stock_code(code)
    normalized_exchange = normalize_market_data_exchange_filter(exchange)
    bounded_limit = _bounded_limit(limit)
    if normalized_exchange == "ALL":
        rows = connection.execute(
            """
            SELECT *
            FROM market_minute_bars
            WHERE code = ? AND interval_sec = ?
            ORDER BY bucket_start DESC, exchange ASC, session ASC
            LIMIT ?
            """,
            (normalized_code, int(interval_sec), bounded_limit),
        ).fetchall()
    else:
        rows = connection.execute(
            """
            SELECT *
            FROM market_minute_bars
            WHERE code = ? AND exchange = ? AND interval_sec = ?
            ORDER BY bucket_start DESC, session ASC
            LIMIT ?
            """,
            (normalized_code, normalized_exchange, int(interval_sec), bounded_limit),
        ).fetchall()
    return [_bar_row_to_dict(row) for row in rows]


def list_premarket_snapshots(
    connection: sqlite3.Connection,
    trade_date: str,
    *,
    limit: int = 500,
) -> list[dict[str, Any]]:
    normalized_trade_date = _normalize_trade_date(trade_date)
    rows = connection.execute(
        """
        SELECT *
        FROM market_premarket_snapshots
        WHERE trade_date = ?
        ORDER BY
            CASE WHEN premarket_gap_pct IS NULL THEN 1 ELSE 0 END,
            premarket_gap_pct DESC,
            tick_count DESC,
            code ASC
        LIMIT ?
        """,
        (normalized_trade_date, _bounded_limit(limit)),
    ).fetchall()
    return [_premarket_snapshot_row_to_dict(row) for row in rows]


def get_premarket_snapshot(
    connection: sqlite3.Connection,
    trade_date: str,
    code: str,
) -> dict[str, Any] | None:
    normalized_trade_date = _normalize_trade_date(trade_date)
    normalized_code = validate_stock_code(code)
    row = connection.execute(
        """
        SELECT *
        FROM market_premarket_snapshots
        WHERE trade_date = ? AND code = ?
        """,
        (normalized_trade_date, normalized_code),
    ).fetchone()
    return None if row is None else _premarket_snapshot_row_to_dict(row)


def list_cross_exchange_observations(
    connection: sqlite3.Connection,
    code: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_code = validate_stock_code(code)
    rows = connection.execute(
        """
        SELECT *
        FROM market_cross_exchange_observations
        WHERE code = ?
        ORDER BY bucket_start DESC
        LIMIT ?
        """,
        (normalized_code, _bounded_limit(limit)),
    ).fetchall()
    return [_cross_exchange_observation_row_to_dict(row) for row in rows]


def get_latest_cross_exchange_observation(
    connection: sqlite3.Connection,
    code: str,
) -> dict[str, Any] | None:
    normalized_code = validate_stock_code(code)
    row = connection.execute(
        """
        SELECT *
        FROM market_cross_exchange_observations
        WHERE code = ?
        ORDER BY bucket_start DESC
        LIMIT 1
        """,
        (normalized_code,),
    ).fetchone()
    return None if row is None else _cross_exchange_observation_row_to_dict(row)


def list_recent_cross_exchange_observations(
    connection: sqlite3.Connection,
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_cross_exchange_observations
        ORDER BY updated_at DESC, bucket_start DESC, code ASC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_cross_exchange_observation_row_to_dict(row) for row in rows]


def get_market_data_readiness(
    connection: sqlite3.Connection,
    code: str,
    *,
    exchange: object = "KRX",
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    normalized_code = validate_stock_code(code)
    normalized_exchange = normalize_market_data_exchange(exchange)
    latest = get_latest_tick(connection, normalized_code, exchange=normalized_exchange)
    reason_codes: list[str] = []
    tick_age_sec: float | None = None
    quality_status = MarketDataQualityStatus.MISSING

    if latest is None:
        reason_codes.append("TICK_MISSING")
    else:
        tick_age_sec = tick_age_seconds(latest["event_ts"])
        stored_status = MarketDataQualityStatus(latest["quality_status"])
        quality_status = freshness_status(
            latest["event_ts"],
            stale_sec=resolved_settings.market_data_tick_stale_sec,
            degraded_sec=resolved_settings.market_data_degraded_tick_stale_sec,
            base_status=stored_status,
        )
        if quality_status is MarketDataQualityStatus.STALE:
            reason_codes.append("TICK_STALE")
        elif quality_status is MarketDataQualityStatus.DEGRADED:
            reason_codes.append("TICK_DEGRADED")
        elif quality_status is MarketDataQualityStatus.INVALID:
            reason_codes.append("TICK_INVALID")

    bar_presence = _bar_presence(connection, normalized_code, exchange=normalized_exchange)
    has_1m_bar = bar_presence.get(60, False)
    has_3m_bar = bar_presence.get(180, False)
    has_5m_bar = bar_presence.get(300, False)

    for interval_sec in resolved_settings.market_data_bar_intervals_sec:
        if not bar_presence.get(interval_sec, False):
            if "BAR_MISSING" not in reason_codes:
                reason_codes.append("BAR_MISSING")
            reason_codes.append(f"BAR_MISSING_{interval_sec}")

    vwap_ready = _has_vwap(connection, normalized_code, exchange=normalized_exchange)
    if not vwap_ready:
        reason_codes.append("VWAP_MISSING")

    return {
        "code": normalized_code,
        "exchange": normalized_exchange,
        "quality_status": quality_status.value,
        "has_latest_tick": latest is not None,
        "tick_age_sec": tick_age_sec,
        "has_1m_bar": has_1m_bar,
        "has_3m_bar": has_3m_bar,
        "has_5m_bar": has_5m_bar,
        "vwap_ready": vwap_ready,
        "reason_codes": reason_codes,
    }


def list_recent_condition_signals(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_condition_signals
        ORDER BY event_ts DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_condition_signal_row_to_dict(row) for row in rows]


def list_recent_tr_snapshots(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_tr_snapshots
        ORDER BY event_ts DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_tr_snapshot_row_to_dict(row) for row in rows]


def list_projection_errors(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_projection_errors
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_projection_error_row_to_dict(row) for row in rows]


def get_market_data_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    freshness_counts = _latest_tick_freshness_counts(connection, settings=resolved_settings)
    recent_window_sec = max(int(resolved_settings.market_data_tick_stale_sec), 60)
    watermark = get_market_data_projection_watermark(connection)
    return {
        "enabled": resolved_settings.market_data_enabled,
        "latest_tick_count": _count_rows(connection, "market_ticks_latest"),
        "latest_tick_freshness_counts": freshness_counts,
        "fresh_tick_count": freshness_counts.get(MarketDataQualityStatus.FRESH.value, 0),
        "stale_tick_count": (
            freshness_counts.get(MarketDataQualityStatus.STALE.value, 0)
            + freshness_counts.get(MarketDataQualityStatus.DEGRADED.value, 0)
        ),
        "sample_count": _count_rows(connection, "market_tick_samples"),
        "bar_count": _count_rows(connection, "market_minute_bars"),
        "premarket_snapshot_count": _count_rows(connection, "market_premarket_snapshots"),
        "premarket_snapshot_enabled": resolved_settings.market_data_premarket_snapshot_enabled,
        "cross_exchange_observation_count": _count_rows(
            connection,
            "market_cross_exchange_observations",
        ),
        "condition_signal_count": _count_rows(connection, "market_condition_signals"),
        "tr_snapshot_count": _count_rows(connection, "market_tr_snapshots"),
        "projection_error_count": _count_rows(connection, "market_projection_errors"),
        "recent_projection_error_count": _count_recent_projection_errors(
            connection,
            within_sec=recent_window_sec,
        ),
        "projection_error_recent_window_sec": recent_window_sec,
        "latest_projection_error_at": _latest_projection_error_at(connection),
        "tick_stale_sec": resolved_settings.market_data_tick_stale_sec,
        "bar_intervals_sec": list(resolved_settings.market_data_bar_intervals_sec),
        "max_recent_ticks": resolved_settings.market_data_max_recent_ticks,
        "projection_watermark": watermark.to_dict(),
    }


def get_market_data_projection_watermark(
    connection: sqlite3.Connection,
) -> ProjectionWatermark:
    return get_projection_watermark(connection, MARKET_DATA_PROJECTION_NAME)


def clear_market_data_projection(connection: sqlite3.Connection) -> None:
    try:
        connection.execute("BEGIN IMMEDIATE")
        for table_name in MARKET_PROJECTION_TABLES:
            connection.execute(f"DELETE FROM {table_name}")
        reset_projection_watermark(
            connection,
            MARKET_DATA_PROJECTION_NAME,
            commit=False,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def rebuild_market_data_projection(
    connection: sqlite3.Connection,
    *,
    clear_projection: bool = False,
    require_clear: bool = False,
    incremental: bool = False,
    limit: int | None = None,
    settings: Settings | None = None,
) -> MarketDataRebuildResult:
    if clear_projection and not require_clear:
        raise ValueError("clear_projection requires require_clear=True")
    if clear_projection and incremental:
        raise ValueError("incremental rebuild cannot clear projection")
    if clear_projection:
        clear_market_data_projection(connection)

    starting_watermark = get_market_data_projection_watermark(connection)
    from_event_rowid = starting_watermark.last_event_rowid if incremental else 0
    rows = _list_replayable_gateway_events(
        connection,
        limit=limit,
        after_event_rowid=from_event_rowid,
    )
    processed_count = applied_count = ignored_count = error_count = 0
    for row in rows:
        processed_count += 1
        event = GatewayEvent(
            event_id=row["event_id"],
            event_type=row["event_type"],
            source=row["source"],
            command_id=row["command_id"],
            idempotency_key=row["idempotency_key"],
            ts=parse_timestamp(row["event_ts"], "event_ts"),
            payload=json.loads(row["payload_json"]),
        )
        result = process_gateway_event(connection, event, settings=settings)
        applied_count += result.applied_count
        ignored_count += result.ignored_count
        error_count += result.error_count

    ending_watermark = get_market_data_projection_watermark(connection)
    return MarketDataRebuildResult(
        processed_count=processed_count,
        applied_count=applied_count,
        ignored_count=ignored_count,
        error_count=error_count,
        mode="incremental" if incremental else "full",
        from_event_rowid=from_event_rowid,
        last_event_rowid=ending_watermark.last_event_rowid,
    )


def _process_price_tick(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    settings: Settings,
) -> int:
    if _price_tick_payload_real_type(event.payload) in QUOTE_ONLY_REAL_TYPES:
        return 0
    tick = BrokerPriceTick.from_dict(event.payload)
    exchange = _price_tick_payload_exchange(event.payload)
    session = market_session_for_tick(tick.trade_time, exchange)
    reason_codes = _price_tick_payload_reason_codes(event.payload)
    quality_status = assess_price_tick_quality(tick)
    invalid_reasons = sorted(
        set(reason_codes).intersection(INVALID_PRICE_TICK_REASON_CODES)
    )
    if invalid_reasons or quality_status is MarketDataQualityStatus.INVALID:
        _record_projection_error(
            connection,
            event,
            error_message=f"INVALID_PRICE_TICK:{','.join(invalid_reasons) or quality_status.value}",
        )
        return 0

    event_ts, received_at = _event_store_times(connection, event)
    older_than_latest = _is_older_than_latest_price_tick(connection, event)
    previous = connection.execute(
        """
        SELECT cumulative_volume, cumulative_trade_value
        FROM market_ticks_latest
        WHERE code = ? AND exchange = ?
        """,
        (tick.code, exchange),
    ).fetchone()
    if previous is None:
        volume_delta = tick.volume
        trade_value_delta = float(tick.trade_value)
    else:
        volume_delta = max(tick.volume - int(previous["cumulative_volume"]), 0)
        previous_trade_value = float(previous["cumulative_trade_value"])
        trade_value_delta = max(float(tick.trade_value) - previous_trade_value, 0.0)

    connection.execute(
        """
        INSERT INTO market_tick_samples (
            event_id,
            code,
            exchange,
            session,
            price,
            cumulative_volume,
            cumulative_trade_value,
            volume_delta,
            trade_value_delta,
            execution_strength,
            event_ts,
            received_at,
            source,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            tick.code,
            exchange,
            session,
            tick.price,
            tick.volume,
            float(tick.trade_value),
            volume_delta,
            trade_value_delta,
            tick.execution_strength,
            event_ts,
            received_at,
            event.source,
            _price_tick_metadata_json(event.payload),
        ),
    )
    if older_than_latest:
        return 1

    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO market_ticks_latest (
            code,
            exchange,
            session,
            name,
            price,
            change_rate,
            cumulative_volume,
            cumulative_trade_value,
            execution_strength,
            best_bid,
            best_ask,
            spread_ticks,
            day_high,
            day_low,
            trade_time,
            event_ts,
            received_at,
            source,
            event_id,
            quality_status,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code, exchange) DO UPDATE SET
            session = excluded.session,
            name = excluded.name,
            price = excluded.price,
            change_rate = excluded.change_rate,
            cumulative_volume = excluded.cumulative_volume,
            cumulative_trade_value = excluded.cumulative_trade_value,
            execution_strength = excluded.execution_strength,
            best_bid = excluded.best_bid,
            best_ask = excluded.best_ask,
            spread_ticks = excluded.spread_ticks,
            day_high = excluded.day_high,
            day_low = excluded.day_low,
            trade_time = excluded.trade_time,
            event_ts = excluded.event_ts,
            received_at = excluded.received_at,
            source = excluded.source,
            event_id = excluded.event_id,
            quality_status = excluded.quality_status,
            updated_at = excluded.updated_at
        """,
        (
            tick.code,
            exchange,
            session,
            tick.name,
            tick.price,
            tick.change_rate,
            tick.volume,
            float(tick.trade_value),
            tick.execution_strength,
            tick.best_bid,
            tick.best_ask,
            tick.spread_ticks,
            tick.day_high,
            tick.day_low,
            datetime_to_wire(tick.trade_time),
            event_ts,
            received_at,
            event.source,
            event.event_id,
            quality_status.value,
            now,
        ),
    )
    for interval_sec in settings.market_data_bar_intervals_sec:
        _upsert_minute_bar(
            connection,
            tick=tick,
            exchange=exchange,
            session=session,
            interval_sec=interval_sec,
            volume_delta=volume_delta,
            trade_value_delta=trade_value_delta,
        )
    premarket_count = _upsert_premarket_snapshot_if_needed(
        connection,
        tick=tick,
        exchange=exchange,
        session=session,
        event=event,
        volume_delta=volume_delta,
        trade_value_delta=trade_value_delta,
        settings=settings,
    )
    cross_exchange_count = _upsert_cross_exchange_observation_if_needed(
        connection,
        tick=tick,
        exchange=exchange,
        session=session,
    )
    return (
        1
        + len(settings.market_data_bar_intervals_sec)
        + premarket_count
        + cross_exchange_count
    )


def _process_condition_event(connection: sqlite3.Connection, event: GatewayEvent) -> int:
    condition = BrokerConditionEvent.from_dict(event.payload)
    event_ts, received_at = _event_store_times(connection, event)
    metadata_json = canonical_json(condition.metadata)
    connection.execute(
        """
        INSERT INTO market_condition_signals (
            event_id,
            condition_id,
            condition_name,
            code,
            name,
            action,
            price,
            event_ts,
            received_at,
            source,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            condition.condition_id,
            condition.condition_name,
            condition.code,
            condition.name,
            condition.action.value,
            condition.price,
            event_ts,
            received_at,
            event.source,
            metadata_json,
        ),
    )
    connection.execute(
        """
        INSERT INTO market_condition_latest (
            condition_id,
            code,
            condition_name,
            name,
            action,
            price,
            event_ts,
            received_at,
            source,
            event_id,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(condition_id, code) DO UPDATE SET
            condition_name = excluded.condition_name,
            name = excluded.name,
            action = excluded.action,
            price = excluded.price,
            event_ts = excluded.event_ts,
            received_at = excluded.received_at,
            source = excluded.source,
            event_id = excluded.event_id,
            metadata_json = excluded.metadata_json
        """,
        (
            condition.condition_id,
            condition.code,
            condition.condition_name,
            condition.name,
            condition.action.value,
            condition.price,
            event_ts,
            received_at,
            event.source,
            event.event_id,
            metadata_json,
        ),
    )
    return 2


def _process_tr_response(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    settings: Settings,
) -> int:
    response = BrokerTrResponse.from_dict(event.payload)
    event_ts, received_at = _event_store_times(connection, event)
    inserted_count = 0
    for row in response.rows:
        row_payload = normalize_payload(row)
        connection.execute(
            """
            INSERT INTO market_tr_snapshots (
                event_id,
                request_id,
                tr_code,
                request_name,
                code,
                row_json,
                event_ts,
                received_at,
                source
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                response.request_id,
                response.tr_code,
                response.request_name,
                _extract_row_code(row_payload),
                canonical_json(row_payload),
                event_ts,
                received_at,
                event.source,
            ),
        )
        inserted_count += 1
    for row_index, tick_payload in _candidate_quote_refresh_tick_payloads_with_row_index(
        event.payload,
        event_ts=event_ts,
    ):
        tick = BrokerPriceTick.from_dict(tick_payload)
        exchange = _price_tick_payload_exchange(tick_payload)
        child_event_id = _synthetic_price_tick_event_id(
            parent_event_id=event.event_id,
            row_index=row_index,
            code=tick.code,
            exchange=exchange,
        )
        if _projection_exists(connection, "price_tick", child_event_id):
            continue
        synthetic_payload = _with_synthetic_price_tick_metadata(
            tick_payload,
            parent_event=event,
            parent_response=response,
            row_index=row_index,
        )
        tick_event = GatewayEvent(
            event_id=child_event_id,
            event_type="price_tick",
            source=event.source,
            command_id=event.command_id,
            idempotency_key=event.idempotency_key,
            ts=parse_timestamp(event_ts, "event_ts"),
            payload=synthetic_payload,
        )
        inserted_count += _process_price_tick(connection, tick_event, settings)
    return inserted_count


def _candidate_quote_refresh_tick_payloads_with_row_index(
    payload: Mapping[str, Any],
    *,
    event_ts: str,
) -> list[tuple[int, dict[str, Any]]]:
    rows = payload.get("rows")
    if isinstance(rows, list):
        source_rows = rows
    else:
        row = payload.get("row")
        source_rows = [row] if isinstance(row, Mapping) else []

    tick_payloads: list[tuple[int, dict[str, Any]]] = []
    for row_index, row in enumerate(source_rows):
        if not isinstance(row, Mapping):
            continue
        single_row_payload = dict(payload)
        single_row_payload["rows"] = [row]
        single_row_payload.pop("row", None)
        for tick_payload in candidate_quote_refresh_tick_payloads_from_tr_response(
            single_row_payload,
            event_ts=event_ts,
        ):
            tick_payloads.append((row_index, tick_payload))
    return tick_payloads


def _synthetic_price_tick_event_id(
    *,
    parent_event_id: str,
    row_index: int,
    code: str,
    exchange: str,
) -> str:
    return (
        f"{parent_event_id}:synthetic_price_tick:"
        f"{int(row_index)}:{validate_stock_code(code)}:{normalize_market_data_exchange(exchange)}"
    )


def _with_synthetic_price_tick_metadata(
    payload: Mapping[str, Any],
    *,
    parent_event: GatewayEvent,
    parent_response: BrokerTrResponse,
    row_index: int,
) -> dict[str, Any]:
    enriched = dict(payload)
    metadata = payload.get("metadata")
    enriched_metadata = dict(metadata) if isinstance(metadata, Mapping) else {}
    enriched_metadata.update(
        {
            "parent_event_id": parent_event.event_id,
            "parent_command_id": parent_event.command_id,
            "parent_tr_code": parent_response.tr_code,
            "parent_request_name": parent_response.request_name,
            "synthetic_event": True,
            "row_index": int(row_index),
        }
    )
    enriched["metadata"] = enriched_metadata
    return enriched


def _upsert_minute_bar(
    connection: sqlite3.Connection,
    *,
    tick: BrokerPriceTick,
    exchange: str,
    session: str,
    interval_sec: int,
    volume_delta: int,
    trade_value_delta: float,
) -> None:
    bucket_start = datetime_to_wire(bucket_start_for(tick.trade_time, interval_sec))
    existing = connection.execute(
        """
        SELECT *
        FROM market_minute_bars
        WHERE code = ? AND exchange = ? AND session = ? AND interval_sec = ? AND bucket_start = ?
        """,
        (tick.code, exchange, session, interval_sec, bucket_start),
    ).fetchone()
    if existing is None:
        bar_open = tick.price
        high = tick.price
        low = tick.price
        total_volume_delta = volume_delta
        total_trade_value_delta = trade_value_delta
        tick_count = 1
    else:
        bar_open = int(existing["open"])
        high = max(int(existing["high"]), tick.price)
        low = min(int(existing["low"]), tick.price)
        total_volume_delta = int(existing["volume_delta"]) + volume_delta
        total_trade_value_delta = float(existing["trade_value_delta"]) + trade_value_delta
        tick_count = int(existing["tick_count"]) + 1

    vwap = calculate_vwap(
        cumulative_trade_value=float(tick.trade_value),
        cumulative_volume=tick.volume,
        bar_trade_value_delta=total_trade_value_delta,
        bar_volume_delta=total_volume_delta,
    )
    connection.execute(
        """
        INSERT INTO market_minute_bars (
            code,
            exchange,
            session,
            interval_sec,
            bucket_start,
            open,
            high,
            low,
            close,
            volume_delta,
            trade_value_delta,
            tick_count,
            vwap,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code, exchange, session, interval_sec, bucket_start) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume_delta = excluded.volume_delta,
            trade_value_delta = excluded.trade_value_delta,
            tick_count = excluded.tick_count,
            vwap = excluded.vwap,
            updated_at = excluded.updated_at
        """,
        (
            tick.code,
            exchange,
            session,
            interval_sec,
            bucket_start,
            bar_open,
            high,
            low,
            tick.price,
            total_volume_delta,
            total_trade_value_delta,
            tick_count,
            vwap,
            datetime_to_wire(utc_now()),
        ),
    )


def _upsert_premarket_snapshot_if_needed(
    connection: sqlite3.Connection,
    *,
    tick: BrokerPriceTick,
    exchange: str,
    session: str,
    event: GatewayEvent,
    volume_delta: int,
    trade_value_delta: float,
    settings: Settings,
) -> int:
    if not settings.market_data_premarket_snapshot_enabled:
        return 0
    if exchange != "NXT" or session != "PREMARKET_NXT":
        return 0

    trade_date = _trade_date_for_timestamp(tick.trade_time)
    prev_close = _previous_krx_close(connection, tick.code, trade_date)
    gap_pct = _gap_pct(tick.price, prev_close)
    trade_time = datetime_to_wire(tick.trade_time)
    now = datetime_to_wire(utc_now())
    metadata = {
        "observe_only": True,
        "not_order_signal": True,
        "no_order_side_effects": True,
        "source": "NXT_PREMARKET_TICK",
        "session": session,
        "exchange": exchange,
        "prev_krx_close_available": prev_close is not None,
        "premarket_observation_is_not_buy_signal": True,
    }
    existing = connection.execute(
        """
        SELECT volume, trade_value, tick_count, first_price, first_trade_time, first_event_id
        FROM market_premarket_snapshots
        WHERE trade_date = ? AND code = ?
        """,
        (trade_date, tick.code),
    ).fetchone()
    if existing is None:
        first_price = tick.price
        first_trade_time = trade_time
        first_event_id = event.event_id
        volume = max(int(volume_delta), 0)
        trade_value = max(float(trade_value_delta), 0.0)
        tick_count = 1
    else:
        first_price = int(existing["first_price"])
        first_trade_time = str(existing["first_trade_time"])
        first_event_id = str(existing["first_event_id"])
        volume = int(existing["volume"]) + max(int(volume_delta), 0)
        trade_value = float(existing["trade_value"]) + max(float(trade_value_delta), 0.0)
        tick_count = int(existing["tick_count"]) + 1

    connection.execute(
        """
        INSERT INTO market_premarket_snapshots (
            trade_date,
            code,
            name,
            first_price,
            first_trade_time,
            first_event_id,
            last_price,
            last_trade_time,
            last_event_id,
            prev_krx_close,
            premarket_gap_pct,
            volume,
            trade_value,
            tick_count,
            updated_at,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(trade_date, code) DO UPDATE SET
            name = excluded.name,
            last_price = excluded.last_price,
            last_trade_time = excluded.last_trade_time,
            last_event_id = excluded.last_event_id,
            prev_krx_close = excluded.prev_krx_close,
            premarket_gap_pct = excluded.premarket_gap_pct,
            volume = excluded.volume,
            trade_value = excluded.trade_value,
            tick_count = excluded.tick_count,
            updated_at = excluded.updated_at,
            metadata_json = excluded.metadata_json
        """,
        (
            trade_date,
            tick.code,
            tick.name,
            first_price,
            first_trade_time,
            first_event_id,
            tick.price,
            trade_time,
            event.event_id,
            prev_close,
            gap_pct,
            volume,
            trade_value,
            tick_count,
            now,
            canonical_json(metadata),
        ),
    )
    return 1


def _upsert_cross_exchange_observation_if_needed(
    connection: sqlite3.Connection,
    *,
    tick: BrokerPriceTick,
    exchange: str,
    session: str,
) -> int:
    if exchange not in {"KRX", "NXT"} or session != "REGULAR":
        return 0

    bucket_start = datetime_to_wire(bucket_start_for(tick.trade_time, 60))
    rows = connection.execute(
        """
        SELECT exchange, close, volume_delta, tick_count
        FROM market_minute_bars
        WHERE code = ?
            AND session = 'REGULAR'
            AND interval_sec = 60
            AND bucket_start = ?
            AND exchange IN ('KRX', 'NXT')
        """,
        (tick.code, bucket_start),
    ).fetchall()
    if not rows:
        return 0
    by_exchange = {str(row["exchange"]): row for row in rows}
    krx = by_exchange.get("KRX")
    nxt = by_exchange.get("NXT")
    krx_last_price = int(krx["close"]) if krx is not None else None
    nxt_last_price = int(nxt["close"]) if nxt is not None else None
    krx_volume = int(krx["volume_delta"]) if krx is not None else 0
    nxt_volume = int(nxt["volume_delta"]) if nxt is not None else 0
    krx_tick_count = int(krx["tick_count"]) if krx is not None else 0
    nxt_tick_count = int(nxt["tick_count"]) if nxt is not None else 0
    total_volume = krx_volume + nxt_volume
    total_tick_count = krx_tick_count + nxt_tick_count
    metadata = {
        "observe_only": True,
        "not_order_signal": True,
        "no_order_side_effects": True,
        "source": "KRX_NXT_REGULAR_MINUTE",
        "session": session,
        "bucket_interval_sec": 60,
        "both_markets_present": krx is not None and nxt is not None,
    }
    connection.execute(
        """
        INSERT INTO market_cross_exchange_observations (
            code,
            bucket_start,
            krx_last_price,
            nxt_last_price,
            divergence_bp,
            krx_volume,
            nxt_volume,
            krx_volume_share,
            nxt_volume_share,
            krx_tick_count,
            nxt_tick_count,
            total_tick_count,
            updated_at,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code, bucket_start) DO UPDATE SET
            krx_last_price = excluded.krx_last_price,
            nxt_last_price = excluded.nxt_last_price,
            divergence_bp = excluded.divergence_bp,
            krx_volume = excluded.krx_volume,
            nxt_volume = excluded.nxt_volume,
            krx_volume_share = excluded.krx_volume_share,
            nxt_volume_share = excluded.nxt_volume_share,
            krx_tick_count = excluded.krx_tick_count,
            nxt_tick_count = excluded.nxt_tick_count,
            total_tick_count = excluded.total_tick_count,
            updated_at = excluded.updated_at,
            metadata_json = excluded.metadata_json
        """,
        (
            tick.code,
            bucket_start,
            krx_last_price,
            nxt_last_price,
            _cross_exchange_divergence_bp(krx_last_price, nxt_last_price),
            krx_volume,
            nxt_volume,
            _share(krx_volume, total_volume),
            _share(nxt_volume, total_volume),
            krx_tick_count,
            nxt_tick_count,
            total_tick_count,
            datetime_to_wire(utc_now()),
            canonical_json(metadata),
        ),
    )
    return 1


def _previous_krx_close(
    connection: sqlite3.Connection,
    code: str,
    trade_date: str,
) -> int | None:
    local_start = datetime.combine(
        date.fromisoformat(_normalize_trade_date(trade_date)),
        time.min,
        tzinfo=_seoul_timezone(),
    )
    start_wire = datetime_to_wire(local_start)
    row = connection.execute(
        """
        SELECT close
        FROM market_minute_bars
        WHERE code = ?
            AND exchange = 'KRX'
            AND session = 'REGULAR'
            AND bucket_start < ?
        ORDER BY bucket_start DESC
        LIMIT 1
        """,
        (validate_stock_code(code), start_wire),
    ).fetchone()
    if row is None:
        return None
    return int(row["close"])


def _projection_exists(
    connection: sqlite3.Connection,
    event_type: str,
    event_id: str,
) -> bool:
    if event_type == "price_tick":
        table_name = "market_tick_samples"
    elif event_type == "condition_event":
        table_name = "market_condition_signals"
    elif event_type == "tr_response":
        table_name = "market_tr_snapshots"
    else:
        return False
    row = connection.execute(
        f"SELECT 1 FROM {table_name} WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone()
    if row is not None:
        return True
    if event_type == "price_tick":
        row = connection.execute(
            """
            SELECT 1
            FROM market_projection_errors
            WHERE event_id = ?
            LIMIT 1
            """,
            (event_id,),
        ).fetchone()
        return row is not None
    return False


def _event_store_times(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> tuple[str, str]:
    row = connection.execute(
        """
        SELECT event_ts, received_at
        FROM gateway_events
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    if row is not None:
        return row["event_ts"], row["received_at"]
    metadata = event.payload.get("metadata")
    parent_event_id = (
        metadata.get("parent_event_id") if isinstance(metadata, Mapping) else None
    )
    if parent_event_id:
        parent_row = connection.execute(
            """
            SELECT event_ts, received_at
            FROM gateway_events
            WHERE event_id = ?
            """,
            (str(parent_event_id),),
        ).fetchone()
        if parent_row is not None:
            return parent_row["event_ts"], parent_row["received_at"]
    return datetime_to_wire(event.ts), datetime_to_wire(utc_now())


def _is_older_than_latest_price_tick(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> bool:
    if _price_tick_payload_real_type(event.payload) in QUOTE_ONLY_REAL_TYPES:
        return False
    try:
        code = validate_stock_code(event.payload.get("code"))
        exchange = _price_tick_payload_exchange(event.payload)
    except ValueError:
        return False
    row = connection.execute(
        """
        SELECT event_ts
        FROM market_ticks_latest
        WHERE code = ? AND exchange = ?
        """,
        (code, exchange),
    ).fetchone()
    if row is None:
        return False
    incoming_event_ts, _ = _event_store_times(connection, event)
    return _timestamp_is_before(incoming_event_ts, row["event_ts"])


def _timestamp_is_before(incoming: str, current: str) -> bool:
    try:
        return parse_timestamp(incoming, "incoming_event_ts") < parse_timestamp(
            current,
            "current_event_ts",
        )
    except ValueError:
        return str(incoming) < str(current)


def _record_projection_error(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    error_message: str,
) -> None:
    connection.execute(
        """
        INSERT INTO market_projection_errors (
            event_id,
            event_type,
            code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            event.event_type.strip().lower(),
            _payload_code(event.payload),
            error_message,
            canonical_json(event.payload),
        ),
    )


def _projection_error_message(
    connection: sqlite3.Connection,
    event_id: str,
) -> str | None:
    row = connection.execute(
        """
        SELECT error_message
        FROM market_projection_errors
        WHERE event_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else str(row["error_message"])


def _payload_code(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("code")
    if value is None:
        return None
    try:
        return validate_stock_code(value)
    except ValueError:
        return str(value)


def _extract_row_code(row: Mapping[str, Any]) -> str | None:
    for key in ("code", "stock_code", "종목코드"):
        value = row.get(key)
        if value is None:
            continue
        try:
            return validate_stock_code(value)
        except ValueError:
            return None
    return None


def _price_tick_payload_reason_codes(payload: Mapping[str, Any]) -> list[str]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return []
    raw_codes = metadata.get("reason_codes")
    if not isinstance(raw_codes, list | tuple | set):
        return []
    return [str(code).strip().upper() for code in raw_codes if str(code).strip()]


def _price_tick_payload_real_type(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return ""
    return str(metadata.get("real_type") or "").strip()


def _price_tick_metadata_json(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return canonical_json({})
    return canonical_json(metadata)


def _price_tick_payload_exchange(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping) and metadata.get("exchange") is not None:
        return normalize_market_data_exchange(metadata.get("exchange"))
    if payload.get("exchange") is not None:
        return normalize_market_data_exchange(payload.get("exchange"))
    return "KRX"


def _latest_tick_freshness_counts(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, int]:
    counts = {status.value: 0 for status in MarketDataQualityStatus}
    rows = connection.execute(
        """
        SELECT event_ts, quality_status
        FROM market_ticks_latest
        """
    ).fetchall()
    for row in rows:
        try:
            base_status = MarketDataQualityStatus(str(row["quality_status"]))
        except ValueError:
            base_status = MarketDataQualityStatus.INVALID
        status = freshness_status(
            row["event_ts"],
            stale_sec=settings.market_data_tick_stale_sec,
            degraded_sec=settings.market_data_degraded_tick_stale_sec,
            base_status=base_status,
        )
        counts[status.value] += 1
    return counts


def _count_recent_projection_errors(
    connection: sqlite3.Connection,
    *,
    within_sec: int,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_projection_errors
        WHERE created_at >= datetime('now', ?)
        """,
        (f"-{max(int(within_sec), 1)} seconds",),
    ).fetchone()
    return int(row["count"] if row else 0)


def _latest_projection_error_at(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        """
        SELECT created_at
        FROM market_projection_errors
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else str(row["created_at"])


def _list_replayable_gateway_events(
    connection: sqlite3.Connection,
    *,
    limit: int | None,
    after_event_rowid: int = 0,
) -> list[sqlite3.Row]:
    limit_sql = "" if limit is None else "LIMIT ?"
    params: tuple[int, ...]
    if limit is None:
        params = (max(int(after_event_rowid), 0),)
    else:
        params = (max(int(after_event_rowid), 0), max(int(limit), 1))
    return connection.execute(
        f"""
        SELECT
            rowid AS event_rowid,
            event_id,
            event_type,
            source,
            command_id,
            idempotency_key,
            event_ts,
            received_at,
            payload_json
        FROM gateway_events
        WHERE status = 'ACCEPTED'
            AND event_type IN ('price_tick', 'condition_event', 'tr_response')
            AND rowid > ?
        ORDER BY rowid ASC
        {limit_sql}
        """,
        params,
    ).fetchall()


def _with_market_data_watermark(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    result: MarketDataProcessResult,
    *,
    commit: bool,
) -> MarketDataProcessResult:
    record_projection_event_result(
        connection,
        projection_name=MARKET_DATA_PROJECTION_NAME,
        event_id=event.event_id,
        status="SUCCESS",
        outcome=result.status,
        metadata={"event_type": event.event_type.strip().lower()},
        commit=False,
    )
    _advance_market_data_watermark_for_event(connection, event, commit=commit)
    return result


def _advance_market_data_watermark_for_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    commit: bool,
) -> ProjectionWatermark | None:
    event_type = event.event_type.strip().lower()
    if event_type not in MARKET_DATA_EVENT_TYPES:
        return None
    row = connection.execute(
        """
        SELECT rowid AS event_rowid, received_at
        FROM gateway_events
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    if row is None:
        return None
    return advance_projection_watermark(
        connection,
        MARKET_DATA_PROJECTION_NAME,
        last_event_rowid=int(row["event_rowid"]),
        last_event_id=event.event_id,
        last_event_received_at=row["received_at"],
        metadata={"event_type": event_type},
        commit=commit,
    )


def _latest_tick_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["tick_age_sec"] = tick_age_seconds(row["event_ts"])
    return data


def _bar_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return _row_to_dict(row)


def _premarket_snapshot_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _cross_exchange_observation_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _condition_signal_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["metadata"] = json.loads(data.pop("metadata_json"))
    return data


def _tr_snapshot_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["row"] = json.loads(data.pop("row_json"))
    return data


def _projection_error_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["payload"] = json.loads(data.pop("payload_json"))
    return data


def _bar_presence(
    connection: sqlite3.Connection,
    code: str,
    *,
    exchange: object = "KRX",
) -> dict[int, bool]:
    normalized_exchange = normalize_market_data_exchange(exchange)
    rows = connection.execute(
        """
        SELECT interval_sec, COUNT(*) AS count
        FROM market_minute_bars
        WHERE code = ? AND exchange = ?
        GROUP BY interval_sec
        """,
        (code, normalized_exchange),
    ).fetchall()
    return {int(row["interval_sec"]): int(row["count"]) > 0 for row in rows}


def _has_vwap(
    connection: sqlite3.Connection,
    code: str,
    *,
    exchange: object = "KRX",
) -> bool:
    normalized_exchange = normalize_market_data_exchange(exchange)
    row = connection.execute(
        """
        SELECT 1
        FROM market_minute_bars
        WHERE code = ? AND exchange = ? AND vwap IS NOT NULL
        LIMIT 1
        """,
        (code, normalized_exchange),
    ).fetchone()
    return row is not None


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _normalize_trade_date(value: object) -> str:
    text = str(value or "").strip()
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError("trade_date must be YYYY-MM-DD") from exc


def _trade_date_for_timestamp(value: object) -> str:
    return parse_timestamp(value, "timestamp").astimezone(_seoul_timezone()).date().isoformat()


def _gap_pct(price: int, prev_close: int | None) -> float | None:
    if prev_close is None or prev_close <= 0:
        return None
    return (float(price) - float(prev_close)) / float(prev_close) * 100.0


def _cross_exchange_divergence_bp(
    krx_last_price: int | None,
    nxt_last_price: int | None,
) -> float | None:
    if krx_last_price is None or nxt_last_price is None or krx_last_price <= 0:
        return None
    return (float(nxt_last_price) - float(krx_last_price)) / float(krx_last_price) * 10000.0


def _share(value: int, total: int) -> float | None:
    if total <= 0:
        return None
    return float(value) / float(total)


def _seoul_timezone():
    try:
        return ZoneInfo("Asia/Seoul")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=9), name="Asia/Seoul")


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
