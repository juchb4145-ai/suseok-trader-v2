from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

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

from services.config import Settings, load_settings

MARKET_DATA_EVENT_TYPES: frozenset[str] = frozenset(
    {"price_tick", "condition_event", "tr_response"}
)
MARKET_PROJECTION_TABLES: tuple[str, ...] = (
    "market_ticks_latest",
    "market_tick_samples",
    "market_minute_bars",
    "market_condition_signals",
    "market_condition_latest",
    "market_tr_snapshots",
    "market_projection_errors",
)
INVALID_PRICE_TICK_REASON_CODES: frozenset[str] = frozenset({"PRICE_MISSING"})


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

    def to_dict(self) -> dict[str, int]:
        return {
            "processed_count": self.processed_count,
            "applied_count": self.applied_count,
            "ignored_count": self.ignored_count,
            "error_count": self.error_count,
        }


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
        return MarketDataProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="DUPLICATE",
            ignored_count=1,
        )

    try:
        connection.execute("BEGIN IMMEDIATE")
        if event_type == "price_tick":
            applied_count = _process_price_tick(connection, event, resolved_settings)
        elif event_type == "condition_event":
            applied_count = _process_condition_event(connection, event)
        else:
            applied_count = _process_tr_response(connection, event)
        connection.commit()
    except Exception as exc:
        connection.rollback()
        _record_projection_error(connection, event, error_message=str(exc))
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


def get_latest_tick(connection: sqlite3.Connection, code: str) -> dict[str, Any] | None:
    normalized_code = validate_stock_code(code)
    row = connection.execute(
        """
        SELECT *
        FROM market_ticks_latest
        WHERE code = ?
        """,
        (normalized_code,),
    ).fetchone()
    if row is None:
        return None
    return _latest_tick_row_to_dict(row)


def list_latest_ticks(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    bounded_limit = _bounded_limit(limit)
    rows = connection.execute(
        """
        SELECT *
        FROM market_ticks_latest
        ORDER BY updated_at DESC, code ASC
        LIMIT ?
        """,
        (bounded_limit,),
    ).fetchall()
    return [_latest_tick_row_to_dict(row) for row in rows]


def list_bars(
    connection: sqlite3.Connection,
    code: str,
    *,
    interval_sec: int = 60,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_code = validate_stock_code(code)
    bounded_limit = _bounded_limit(limit)
    rows = connection.execute(
        """
        SELECT *
        FROM market_minute_bars
        WHERE code = ? AND interval_sec = ?
        ORDER BY bucket_start DESC
        LIMIT ?
        """,
        (normalized_code, int(interval_sec), bounded_limit),
    ).fetchall()
    return [_bar_row_to_dict(row) for row in rows]


def get_market_data_readiness(
    connection: sqlite3.Connection,
    code: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    normalized_code = validate_stock_code(code)
    latest = get_latest_tick(connection, normalized_code)
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

    bar_presence = _bar_presence(connection, normalized_code)
    has_1m_bar = bar_presence.get(60, False)
    has_3m_bar = bar_presence.get(180, False)
    has_5m_bar = bar_presence.get(300, False)

    for interval_sec in resolved_settings.market_data_bar_intervals_sec:
        if not bar_presence.get(interval_sec, False):
            if "BAR_MISSING" not in reason_codes:
                reason_codes.append("BAR_MISSING")
            reason_codes.append(f"BAR_MISSING_{interval_sec}")

    vwap_ready = _has_vwap(connection, normalized_code)
    if not vwap_ready:
        reason_codes.append("VWAP_MISSING")

    return {
        "code": normalized_code,
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
    return {
        "enabled": resolved_settings.market_data_enabled,
        "latest_tick_count": _count_rows(connection, "market_ticks_latest"),
        "sample_count": _count_rows(connection, "market_tick_samples"),
        "bar_count": _count_rows(connection, "market_minute_bars"),
        "condition_signal_count": _count_rows(connection, "market_condition_signals"),
        "tr_snapshot_count": _count_rows(connection, "market_tr_snapshots"),
        "projection_error_count": _count_rows(connection, "market_projection_errors"),
        "tick_stale_sec": resolved_settings.market_data_tick_stale_sec,
        "bar_intervals_sec": list(resolved_settings.market_data_bar_intervals_sec),
        "max_recent_ticks": resolved_settings.market_data_max_recent_ticks,
    }


def clear_market_data_projection(connection: sqlite3.Connection) -> None:
    try:
        connection.execute("BEGIN IMMEDIATE")
        for table_name in MARKET_PROJECTION_TABLES:
            connection.execute(f"DELETE FROM {table_name}")
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def rebuild_market_data_projection(
    connection: sqlite3.Connection,
    *,
    clear_projection: bool = False,
    require_clear: bool = False,
    limit: int | None = None,
    settings: Settings | None = None,
) -> MarketDataRebuildResult:
    if clear_projection and not require_clear:
        raise ValueError("clear_projection requires require_clear=True")
    if clear_projection:
        clear_market_data_projection(connection)

    rows = _list_replayable_gateway_events(connection, limit=limit)
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

    return MarketDataRebuildResult(
        processed_count=processed_count,
        applied_count=applied_count,
        ignored_count=ignored_count,
        error_count=error_count,
    )


def _process_price_tick(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    settings: Settings,
) -> int:
    tick = BrokerPriceTick.from_dict(event.payload)
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
    previous = connection.execute(
        """
        SELECT cumulative_volume, cumulative_trade_value
        FROM market_ticks_latest
        WHERE code = ?
        """,
        (tick.code,),
    ).fetchone()
    if previous is None:
        volume_delta = tick.volume
        trade_value_delta = float(tick.trade_value)
    else:
        volume_delta = max(tick.volume - int(previous["cumulative_volume"]), 0)
        previous_trade_value = float(previous["cumulative_trade_value"])
        trade_value_delta = max(float(tick.trade_value) - previous_trade_value, 0.0)

    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO market_ticks_latest (
            code,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
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
    connection.execute(
        """
        INSERT INTO market_tick_samples (
            event_id,
            code,
            price,
            cumulative_volume,
            cumulative_trade_value,
            volume_delta,
            trade_value_delta,
            execution_strength,
            event_ts,
            received_at,
            source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            tick.code,
            tick.price,
            tick.volume,
            float(tick.trade_value),
            volume_delta,
            trade_value_delta,
            tick.execution_strength,
            event_ts,
            received_at,
            event.source,
        ),
    )
    for interval_sec in settings.market_data_bar_intervals_sec:
        _upsert_minute_bar(
            connection,
            tick=tick,
            interval_sec=interval_sec,
            volume_delta=volume_delta,
            trade_value_delta=trade_value_delta,
        )
    return 1 + len(settings.market_data_bar_intervals_sec)


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


def _process_tr_response(connection: sqlite3.Connection, event: GatewayEvent) -> int:
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
    return inserted_count


def _upsert_minute_bar(
    connection: sqlite3.Connection,
    *,
    tick: BrokerPriceTick,
    interval_sec: int,
    volume_delta: int,
    trade_value_delta: float,
) -> None:
    bucket_start = datetime_to_wire(bucket_start_for(tick.trade_time, interval_sec))
    existing = connection.execute(
        """
        SELECT *
        FROM market_minute_bars
        WHERE code = ? AND interval_sec = ? AND bucket_start = ?
        """,
        (tick.code, interval_sec, bucket_start),
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code, interval_sec, bucket_start) DO UPDATE SET
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
    return datetime_to_wire(event.ts), datetime_to_wire(utc_now())


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


def _list_replayable_gateway_events(
    connection: sqlite3.Connection,
    *,
    limit: int | None,
) -> list[sqlite3.Row]:
    limit_sql = "" if limit is None else "LIMIT ?"
    params: tuple[int, ...] = () if limit is None else (max(int(limit), 1),)
    return connection.execute(
        f"""
        SELECT
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
        ORDER BY event_ts ASC, received_at ASC, event_id ASC
        {limit_sql}
        """,
        params,
    ).fetchall()


def _latest_tick_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["tick_age_sec"] = tick_age_seconds(row["event_ts"])
    return data


def _bar_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return _row_to_dict(row)


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


def _bar_presence(connection: sqlite3.Connection, code: str) -> dict[int, bool]:
    rows = connection.execute(
        """
        SELECT interval_sec, COUNT(*) AS count
        FROM market_minute_bars
        WHERE code = ?
        GROUP BY interval_sec
        """,
        (code,),
    ).fetchall()
    return {int(row["interval_sec"]): int(row["count"]) > 0 for row in rows}


def _has_vwap(connection: sqlite3.Connection, code: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM market_minute_bars
        WHERE code = ? AND vwap IS NOT NULL
        LIMIT 1
        """,
        (code,),
    ).fetchone()
    return row is not None


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
