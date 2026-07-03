from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.market_index import (
    DEFAULT_ALLOWED_INDEX_CODES,
    BrokerMarketIndexTick,
)
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from domain.market.bars import bucket_start_for
from domain.market.models import MarketDataQualityStatus
from domain.market.quality import freshness_status, tick_age_seconds
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings

MARKET_INDEX_EVENT_TYPES: frozenset[str] = frozenset({"market_index_tick"})
MARKET_INDEX_PROJECTION_TABLES: tuple[str, ...] = (
    "market_index_ticks_latest",
    "market_index_tick_samples",
    "market_index_bars",
    "market_index_projection_errors",
)


@dataclass(frozen=True, kw_only=True)
class MarketIndexProcessResult:
    event_id: str
    event_type: str
    status: str
    applied_count: int = 0
    ignored_count: int = 0
    error_count: int = 0
    error_message: str | None = None


def process_market_index_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> MarketIndexProcessResult:
    resolved_settings = settings or load_settings()
    event_type = event.event_type.strip().lower()
    if event_type not in MARKET_INDEX_EVENT_TYPES:
        return MarketIndexProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="IGNORED",
            ignored_count=1,
        )
    if _projection_exists(connection, event.event_id):
        return MarketIndexProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="DUPLICATE",
            ignored_count=1,
        )
    if _is_older_than_latest_index_tick(connection, event):
        return MarketIndexProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="IGNORED",
            ignored_count=1,
        )

    try:
        tick = BrokerMarketIndexTick.from_dict(event.payload)
        implausible_reason = _index_implausible_reason(tick)
        if implausible_reason is not None:
            _record_projection_error(
                connection,
                event,
                error_message=implausible_reason,
                reason_code="INDEX_IMPLAUSIBLE",
            )
            connection.commit()
            return MarketIndexProcessResult(
                event_id=event.event_id,
                event_type=event_type,
                status="ERROR",
                error_count=1,
                error_message=implausible_reason,
            )
        event_ts, received_at = _event_store_times(connection, event)
        connection.execute("BEGIN IMMEDIATE")
        _upsert_latest_tick(connection, tick, event, event_ts=event_ts, received_at=received_at)
        _insert_sample(connection, tick, event, event_ts=event_ts, received_at=received_at)
        for interval_sec in resolved_settings.market_data_bar_intervals_sec:
            _upsert_index_bar(connection, tick=tick, interval_sec=interval_sec)
        connection.commit()
    except Exception as exc:
        connection.rollback()
        _record_projection_error(connection, event, error_message=str(exc))
        connection.commit()
        return MarketIndexProcessResult(
            event_id=event.event_id,
            event_type=event_type,
            status="ERROR",
            error_count=1,
            error_message=str(exc),
        )

    return MarketIndexProcessResult(
        event_id=event.event_id,
        event_type=event_type,
        status="APPLIED",
        applied_count=2 + len(resolved_settings.market_data_bar_intervals_sec),
    )


def get_latest_market_index_tick(
    connection: sqlite3.Connection,
    index_code: str,
) -> dict[str, Any] | None:
    normalized_code = normalize_index_code(index_code)
    row = connection.execute(
        """
        SELECT *
        FROM market_index_ticks_latest
        WHERE index_code = ?
        """,
        (normalized_code,),
    ).fetchone()
    if row is None:
        return None
    return _latest_tick_row_to_dict(row)


def list_latest_market_index_ticks(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_index_ticks_latest
        ORDER BY updated_at DESC, index_code ASC
        LIMIT ?
        """,
        (_bounded_limit(limit),),
    ).fetchall()
    return [_latest_tick_row_to_dict(row) for row in rows]


def list_market_index_bars(
    connection: sqlite3.Connection,
    index_code: str,
    *,
    interval_sec: int = 60,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_code = normalize_index_code(index_code)
    rows = connection.execute(
        """
        SELECT *
        FROM market_index_bars
        WHERE index_code = ? AND interval_sec = ?
        ORDER BY bucket_start DESC
        LIMIT ?
        """,
        (normalized_code, int(interval_sec), _bounded_limit(limit)),
    ).fetchall()
    return [_bar_row_to_dict(row) for row in rows]


def get_market_index_readiness(
    connection: sqlite3.Connection,
    index_code: str,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    normalized_code = normalize_index_code(index_code)
    latest = get_latest_market_index_tick(connection, normalized_code)
    reason_codes: list[str] = []
    tick_age_sec: float | None = None
    quality_status = MarketDataQualityStatus.MISSING

    if latest is None:
        reason_codes.append("INDEX_TICK_MISSING")
    else:
        tick_age_sec = tick_age_seconds(latest["event_ts"])
        quality_status = freshness_status(
            latest["event_ts"],
            stale_sec=resolved_settings.market_index_stale_sec,
            degraded_sec=resolved_settings.market_index_stale_sec * 2,
            base_status=MarketDataQualityStatus(latest["quality_status"]),
        )
        if quality_status is MarketDataQualityStatus.STALE:
            reason_codes.append("INDEX_TICK_STALE")
        elif quality_status is MarketDataQualityStatus.DEGRADED:
            reason_codes.append("INDEX_TICK_DEGRADED")
        elif quality_status is MarketDataQualityStatus.INVALID:
            reason_codes.append("INDEX_TICK_INVALID")

    bar_presence = _bar_presence(connection, normalized_code)
    for interval_sec in resolved_settings.market_data_bar_intervals_sec:
        if not bar_presence.get(interval_sec, False):
            if "INDEX_BAR_MISSING" not in reason_codes:
                reason_codes.append("INDEX_BAR_MISSING")
            reason_codes.append(f"INDEX_BAR_MISSING_{interval_sec}")

    return {
        "index_code": normalized_code,
        "quality_status": quality_status.value,
        "has_latest_tick": latest is not None,
        "tick_age_sec": tick_age_sec,
        "has_1m_bar": bar_presence.get(60, False),
        "has_3m_bar": bar_presence.get(180, False),
        "has_5m_bar": bar_presence.get(300, False),
        "reason_codes": reason_codes,
    }


def get_market_index_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    readiness = {
        index_code: get_market_index_readiness(
            connection,
            index_code,
            settings=resolved_settings,
        )
        for index_code in sorted(DEFAULT_ALLOWED_INDEX_CODES)
    }
    freshness_counts = {status.value: 0 for status in MarketDataQualityStatus}
    for item in readiness.values():
        freshness_counts[item["quality_status"]] += 1
    return {
        "enabled": True,
        "allowed_index_codes": sorted(DEFAULT_ALLOWED_INDEX_CODES),
        "latest_tick_count": _count_rows(connection, "market_index_ticks_latest"),
        "sample_count": _count_rows(connection, "market_index_tick_samples"),
        "bar_count": _count_rows(connection, "market_index_bars"),
        "projection_error_count": _count_rows(connection, "market_index_projection_errors"),
        "latest_projection_error_at": _latest_projection_error_at(connection),
        "freshness_counts": freshness_counts,
        "readiness": readiness,
        "core_status": _market_index_core_status(readiness),
        "sanity_warnings": _market_index_sanity_warnings(connection),
        "unverified": _has_unverified_index_parser(connection),
        "stale_sec": resolved_settings.market_index_stale_sec,
        "bar_intervals_sec": list(resolved_settings.market_data_bar_intervals_sec),
    }


def normalize_index_code(value: object) -> str:
    normalized = str(value).strip().upper()
    if normalized not in DEFAULT_ALLOWED_INDEX_CODES:
        allowed = ", ".join(sorted(DEFAULT_ALLOWED_INDEX_CODES))
        raise ValueError(f"index_code must be one of: {allowed}")
    return normalized


def _upsert_latest_tick(
    connection: sqlite3.Connection,
    tick: BrokerMarketIndexTick,
    event: GatewayEvent,
    *,
    event_ts: str,
    received_at: str,
) -> None:
    now = datetime_to_wire(utc_now())
    connection.execute(
        """
        INSERT INTO market_index_ticks_latest (
            index_code,
            index_name,
            price,
            change_rate,
            change_value,
            trade_time,
            event_ts,
            received_at,
            source,
            event_id,
            quality_status,
            metadata_json,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(index_code) DO UPDATE SET
            index_name = excluded.index_name,
            price = excluded.price,
            change_rate = excluded.change_rate,
            change_value = excluded.change_value,
            trade_time = excluded.trade_time,
            event_ts = excluded.event_ts,
            received_at = excluded.received_at,
            source = excluded.source,
            event_id = excluded.event_id,
            quality_status = excluded.quality_status,
            metadata_json = excluded.metadata_json,
            updated_at = excluded.updated_at
        """,
        (
            tick.index_code,
            tick.index_name,
            tick.price,
            tick.change_rate,
            tick.change_value,
            datetime_to_wire(tick.trade_time),
            event_ts,
            received_at,
            event.source,
            event.event_id,
            MarketDataQualityStatus.FRESH.value,
            canonical_json(tick.metadata),
            now,
        ),
    )


def _insert_sample(
    connection: sqlite3.Connection,
    tick: BrokerMarketIndexTick,
    event: GatewayEvent,
    *,
    event_ts: str,
    received_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO market_index_tick_samples (
            event_id,
            index_code,
            index_name,
            price,
            change_rate,
            change_value,
            trade_time,
            event_ts,
            received_at,
            source,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            tick.index_code,
            tick.index_name,
            tick.price,
            tick.change_rate,
            tick.change_value,
            datetime_to_wire(tick.trade_time),
            event_ts,
            received_at,
            event.source,
            canonical_json(tick.metadata),
        ),
    )


def _upsert_index_bar(
    connection: sqlite3.Connection,
    *,
    tick: BrokerMarketIndexTick,
    interval_sec: int,
) -> None:
    bucket_start = datetime_to_wire(bucket_start_for(tick.trade_time, interval_sec))
    existing = connection.execute(
        """
        SELECT *
        FROM market_index_bars
        WHERE index_code = ? AND interval_sec = ? AND bucket_start = ?
        """,
        (tick.index_code, interval_sec, bucket_start),
    ).fetchone()
    if existing is None:
        bar_open = tick.price
        high = tick.price
        low = tick.price
        tick_count = 1
        change_rate_open = tick.change_rate
    else:
        bar_open = float(existing["open"])
        high = max(float(existing["high"]), tick.price)
        low = min(float(existing["low"]), tick.price)
        tick_count = int(existing["tick_count"]) + 1
        change_rate_open = float(existing["change_rate_open"])
    connection.execute(
        """
        INSERT INTO market_index_bars (
            index_code,
            interval_sec,
            bucket_start,
            open,
            high,
            low,
            close,
            change_rate_open,
            change_rate_close,
            tick_count,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(index_code, interval_sec, bucket_start) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            change_rate_open = excluded.change_rate_open,
            change_rate_close = excluded.change_rate_close,
            tick_count = excluded.tick_count,
            updated_at = excluded.updated_at
        """,
        (
            tick.index_code,
            interval_sec,
            bucket_start,
            bar_open,
            high,
            low,
            tick.price,
            change_rate_open,
            tick.change_rate,
            tick_count,
            datetime_to_wire(utc_now()),
        ),
    )


def _projection_exists(connection: sqlite3.Connection, event_id: str) -> bool:
    for table_name in ("market_index_tick_samples", "market_index_projection_errors"):
        row = connection.execute(
            f"SELECT 1 FROM {table_name} WHERE event_id = ? LIMIT 1",
            (event_id,),
        ).fetchone()
        if row is not None:
            return True
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


def _is_older_than_latest_index_tick(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> bool:
    try:
        index_code = normalize_index_code(event.payload.get("index_code"))
    except ValueError:
        return False
    row = connection.execute(
        """
        SELECT event_ts
        FROM market_index_ticks_latest
        WHERE index_code = ?
        """,
        (index_code,),
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
    reason_code: str = "INDEX_PROJECTION_ERROR",
) -> None:
    connection.execute(
        """
        INSERT INTO market_index_projection_errors (
            event_id,
            event_type,
            index_code,
            reason_code,
            error_message,
            payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            event.event_id,
            event.event_type.strip().lower(),
            _payload_index_code(event.payload),
            reason_code,
            error_message,
            canonical_json(event.payload),
        ),
    )


def _payload_index_code(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return None
    value = payload.get("index_code")
    if value is None:
        return None
    try:
        return normalize_index_code(value)
    except ValueError:
        return str(value)


def _bar_presence(connection: sqlite3.Connection, index_code: str) -> dict[int, bool]:
    rows = connection.execute(
        """
        SELECT interval_sec, COUNT(*) AS count
        FROM market_index_bars
        WHERE index_code = ?
        GROUP BY interval_sec
        """,
        (index_code,),
    ).fetchall()
    return {int(row["interval_sec"]): int(row["count"]) > 0 for row in rows}


def _latest_projection_error_at(connection: sqlite3.Connection) -> str | None:
    row = connection.execute(
        """
        SELECT created_at
        FROM market_index_projection_errors
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """
    ).fetchone()
    return None if row is None else str(row["created_at"])


def _market_index_core_status(readiness: dict[str, dict[str, Any]]) -> dict[str, Any]:
    required_codes = ("KOSPI", "KOSDAQ")
    quality_by_code: dict[str, str] = {}
    reason_codes: list[str] = []
    for code in required_codes:
        item = readiness.get(code) or {}
        quality = str(item.get("quality_status") or "MISSING").upper()
        quality_by_code[code] = quality
        for reason in item.get("reason_codes") or []:
            reason_codes.append(str(reason).upper())
        if quality == "MISSING" and "INDEX_TICK_MISSING" not in reason_codes:
            reason_codes.append("INDEX_TICK_MISSING")

    qualities = set(quality_by_code.values())
    if qualities == {"FRESH"}:
        status = "READY"
        badge_status = "PASS"
        label = "index core ready"
    elif qualities & {"MISSING", "STALE", "INVALID"}:
        status = "DATA_WAIT"
        badge_status = "DATA_WAIT"
        label = "index core waiting"
    elif "DEGRADED" in qualities:
        status = "DEGRADED"
        badge_status = "DEGRADED"
        label = "index core degraded"
    else:
        status = "DATA_WAIT"
        badge_status = "DATA_WAIT"
        label = "index core waiting"

    return {
        "status": status,
        "badge_status": badge_status,
        "label": label,
        "required_index_codes": list(required_codes),
        "quality_statuses": quality_by_code,
        "reason_codes": _dedupe(reason_codes),
    }


def _market_index_sanity_warnings(connection: sqlite3.Connection) -> list[str]:
    rows = connection.execute(
        """
        SELECT index_code, price, change_rate
        FROM market_index_ticks_latest
        """
    ).fetchall()
    warnings: list[str] = []
    for row in rows:
        if float(row["price"]) <= 0:
            warnings.append("MARKET_INDEX_VALUE_INVALID")
        if abs(float(row["change_rate"])) > 50:
            warnings.append("MARKET_INDEX_CHANGE_RATE_SUSPECT")
    return _dedupe(warnings)


def _index_implausible_reason(tick: BrokerMarketIndexTick) -> str | None:
    if tick.index_code == "KOSPI" and (tick.price < 1000 or tick.price > 15000):
        return "INDEX_IMPLAUSIBLE:KOSPI_PRICE_OUT_OF_RANGE"
    if abs(float(tick.change_rate)) > 15.0:
        return "INDEX_IMPLAUSIBLE:CHANGE_RATE_OUT_OF_RANGE"
    return None


def _has_unverified_index_parser(connection: sqlite3.Connection) -> bool:
    rows = connection.execute(
        """
        SELECT metadata_json
        FROM market_index_ticks_latest
        """
    ).fetchall()
    for row in rows:
        try:
            metadata = json.loads(row["metadata_json"] or "{}")
        except json.JSONDecodeError:
            return True
        parser_status = str(metadata.get("parser_status") or "VERIFIED").upper()
        if parser_status != "VERIFIED":
            return True
    return False


def _latest_tick_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["metadata"] = json.loads(data.pop("metadata_json"))
    data["tick_age_sec"] = tick_age_seconds(data["event_ts"])
    data["parser_status"] = str(data["metadata"].get("parser_status") or "VERIFIED").upper()
    data["unverified"] = data["parser_status"] != "VERIFIED"
    return data


def _bar_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return _row_to_dict(row)


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


def _bounded_limit(limit: int) -> int:
    return min(max(int(limit), 1), 500)


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
