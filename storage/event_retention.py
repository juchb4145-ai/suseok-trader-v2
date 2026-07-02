from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from domain.broker.utils import datetime_to_wire, utc_now
from services.config import Settings, load_settings

from storage.projection_watermarks import get_projection_watermark

MARKET_DATA_PROJECTION_NAME = "market_data"
DEFAULT_PRUNABLE_EVENT_TYPES: tuple[str, ...] = (
    "heartbeat",
    "gateway_log",
    "price_tick",
    "quote_tick",
)
WATERMARK_GATED_EVENT_TYPES: frozenset[str] = frozenset({"price_tick"})


@dataclass(frozen=True, kw_only=True)
class EventRetentionResult:
    run_id: str
    dry_run: bool
    cutoff_at: str
    retention_days: int
    candidate_event_count: int
    selected_event_count: int
    deleted_gateway_event_count: int
    deleted_raw_event_count: int
    market_data_watermark_rowid: int
    prunable_event_types: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "dry_run": self.dry_run,
            "cutoff_at": self.cutoff_at,
            "retention_days": self.retention_days,
            "candidate_event_count": self.candidate_event_count,
            "selected_event_count": self.selected_event_count,
            "deleted_gateway_event_count": self.deleted_gateway_event_count,
            "deleted_raw_event_count": self.deleted_raw_event_count,
            "market_data_watermark_rowid": self.market_data_watermark_rowid,
            "prunable_event_types": list(self.prunable_event_types),
        }


def get_event_retention_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    retention_days: int | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    resolved_days = _resolve_retention_days(resolved_settings, retention_days)
    cutoff_at = _cutoff_for_days(resolved_days)
    watermark = get_projection_watermark(connection, MARKET_DATA_PROJECTION_NAME)
    candidate_count = _count_retention_candidates(
        connection,
        cutoff_at=cutoff_at,
        market_data_watermark_rowid=watermark.last_event_rowid,
        event_types=DEFAULT_PRUNABLE_EVENT_TYPES,
    )
    return {
        "enabled": resolved_settings.event_store_retention_enabled,
        "retention_days": resolved_days,
        "cutoff_at": cutoff_at,
        "candidate_event_count": candidate_count,
        "market_data_watermark": watermark.to_dict(),
        "prunable_event_types": list(DEFAULT_PRUNABLE_EVENT_TYPES),
        "dry_run_default": True,
        "batch_size": resolved_settings.event_store_retention_batch_size,
        "interval_sec": resolved_settings.event_store_retention_interval_sec,
    }


def prune_event_store_events(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    retention_days: int | None = None,
    dry_run: bool = True,
    limit: int | None = None,
) -> EventRetentionResult:
    resolved_settings = settings or load_settings()
    resolved_days = _resolve_retention_days(resolved_settings, retention_days)
    bounded_limit = _resolve_limit(resolved_settings, limit)
    cutoff_at = _cutoff_for_days(resolved_days)
    watermark = get_projection_watermark(connection, MARKET_DATA_PROJECTION_NAME)
    event_types = DEFAULT_PRUNABLE_EVENT_TYPES
    candidate_count = _count_retention_candidates(
        connection,
        cutoff_at=cutoff_at,
        market_data_watermark_rowid=watermark.last_event_rowid,
        event_types=event_types,
    )
    event_ids = _select_retention_candidate_event_ids(
        connection,
        cutoff_at=cutoff_at,
        market_data_watermark_rowid=watermark.last_event_rowid,
        event_types=event_types,
        limit=bounded_limit,
    )
    result = EventRetentionResult(
        run_id=f"event-retention-{uuid.uuid4().hex}",
        dry_run=bool(dry_run),
        cutoff_at=cutoff_at,
        retention_days=resolved_days,
        candidate_event_count=candidate_count,
        selected_event_count=len(event_ids),
        deleted_gateway_event_count=0,
        deleted_raw_event_count=0,
        market_data_watermark_rowid=watermark.last_event_rowid,
        prunable_event_types=event_types,
    )
    if dry_run:
        return result

    try:
        connection.execute("BEGIN IMMEDIATE")
        deleted_gateway_count = _delete_events_by_id(
            connection,
            "gateway_events",
            event_ids,
        )
        deleted_raw_count = _delete_events_by_id(connection, "raw_events", event_ids)
        result = EventRetentionResult(
            run_id=result.run_id,
            dry_run=False,
            cutoff_at=result.cutoff_at,
            retention_days=result.retention_days,
            candidate_event_count=result.candidate_event_count,
            selected_event_count=result.selected_event_count,
            deleted_gateway_event_count=deleted_gateway_count,
            deleted_raw_event_count=deleted_raw_count,
            market_data_watermark_rowid=result.market_data_watermark_rowid,
            prunable_event_types=result.prunable_event_types,
        )
        _record_event_retention_run(connection, result)
        connection.commit()
        return result
    except Exception:
        connection.rollback()
        raise


def _resolve_retention_days(settings: Settings, retention_days: int | None) -> int:
    return max(int(retention_days or settings.event_store_retention_days), 1)


def _resolve_limit(settings: Settings, limit: int | None) -> int:
    return max(int(limit or settings.event_store_retention_batch_size), 1)


def _cutoff_for_days(retention_days: int) -> str:
    return datetime_to_wire(utc_now() - timedelta(days=max(int(retention_days), 1)))


def _count_retention_candidates(
    connection: sqlite3.Connection,
    *,
    cutoff_at: str,
    market_data_watermark_rowid: int,
    event_types: Iterable[str],
) -> int:
    where_sql, params = _retention_where_clause(
        cutoff_at=cutoff_at,
        market_data_watermark_rowid=market_data_watermark_rowid,
        event_types=event_types,
    )
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM gateway_events
        WHERE {where_sql}
        """,
        params,
    ).fetchone()
    return int(row["count"] if row else 0)


def _select_retention_candidate_event_ids(
    connection: sqlite3.Connection,
    *,
    cutoff_at: str,
    market_data_watermark_rowid: int,
    event_types: Iterable[str],
    limit: int,
) -> list[str]:
    where_sql, params = _retention_where_clause(
        cutoff_at=cutoff_at,
        market_data_watermark_rowid=market_data_watermark_rowid,
        event_types=event_types,
    )
    rows = connection.execute(
        f"""
        SELECT event_id
        FROM gateway_events
        WHERE {where_sql}
        ORDER BY rowid ASC
        LIMIT ?
        """,
        (*params, max(int(limit), 1)),
    ).fetchall()
    return [str(row["event_id"]) for row in rows]


def _retention_where_clause(
    *,
    cutoff_at: str,
    market_data_watermark_rowid: int,
    event_types: Iterable[str],
) -> tuple[str, tuple[Any, ...]]:
    normalized_types = tuple(
        sorted({str(event_type).strip().lower() for event_type in event_types if event_type})
    )
    ungated_types = tuple(
        event_type
        for event_type in normalized_types
        if event_type not in WATERMARK_GATED_EVENT_TYPES
    )
    gated_types = tuple(
        event_type
        for event_type in normalized_types
        if event_type in WATERMARK_GATED_EVENT_TYPES
    )
    clauses: list[str] = ["received_at < ?"]
    params: list[Any] = [cutoff_at]
    type_clauses: list[str] = []
    if ungated_types:
        placeholders = ", ".join("?" for _ in ungated_types)
        type_clauses.append(f"event_type IN ({placeholders})")
        params.extend(ungated_types)
    if gated_types and market_data_watermark_rowid > 0:
        placeholders = ", ".join("?" for _ in gated_types)
        type_clauses.append(
            f"(event_type IN ({placeholders}) AND rowid <= ?)"
        )
        params.extend(gated_types)
        params.append(int(market_data_watermark_rowid))
    if not type_clauses:
        return "0 = 1", ()
    clauses.append(f"({' OR '.join(type_clauses)})")
    return " AND ".join(clauses), tuple(params)


def _delete_events_by_id(
    connection: sqlite3.Connection,
    table_name: str,
    event_ids: list[str],
) -> int:
    if not event_ids:
        return 0
    placeholders = ", ".join("?" for _ in event_ids)
    cursor = connection.execute(
        f"DELETE FROM {table_name} WHERE event_id IN ({placeholders})",
        tuple(event_ids),
    )
    return int(cursor.rowcount if cursor.rowcount is not None else 0)


def _record_event_retention_run(
    connection: sqlite3.Connection,
    result: EventRetentionResult,
) -> None:
    connection.execute(
        """
        INSERT INTO event_retention_runs (
            run_id,
            cutoff_at,
            retention_days,
            dry_run,
            candidate_event_count,
            selected_event_count,
            deleted_gateway_event_count,
            deleted_raw_event_count,
            market_data_watermark_rowid,
            prunable_event_types_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            result.run_id,
            result.cutoff_at,
            result.retention_days,
            int(result.dry_run),
            result.candidate_event_count,
            result.selected_event_count,
            result.deleted_gateway_event_count,
            result.deleted_raw_event_count,
            result.market_data_watermark_rowid,
            json.dumps(list(result.prunable_event_types), sort_keys=True),
        ),
    )
