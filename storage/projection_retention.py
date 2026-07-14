from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from domain.broker.utils import parse_timestamp

from storage.projection_watermarks import list_projection_watermarks

UNCONDITIONALLY_PRUNABLE_EVENT_TYPES: tuple[str, ...] = (
    "gateway_log",
    "heartbeat",
    "quote_tick",
)
PROJECTION_GATED_EVENT_TYPES: tuple[str, ...] = (
    "condition_event",
    "market_index_tick",
    "market_scan",
    "market_symbols",
    "price_tick",
    "tr_response",
)
DEFAULT_PRUNABLE_EVENT_TYPES: tuple[str, ...] = tuple(
    sorted(
        set(UNCONDITIONALLY_PRUNABLE_EVENT_TYPES)
        | set(PROJECTION_GATED_EVENT_TYPES)
    )
)


def projection_retention_ready_sql(event_alias: str = "ge") -> str:
    alias = _sql_identifier(event_alias)
    ungated = _sql_string_list(UNCONDITIONALLY_PRUNABLE_EVENT_TYPES)
    gated = _sql_string_list(PROJECTION_GATED_EVENT_TYPES)
    return f"""
        {alias}.command_id IS NULL
        AND EXISTS (
            SELECT 1 FROM raw_events AS retained_raw
            WHERE retained_raw.event_id = {alias}.event_id
        )
        AND (
            {alias}.status != 'ACCEPTED'
            OR {alias}.event_type IN ({ungated})
            OR (
                {alias}.event_type IN ({gated})
                AND EXISTS (
                    SELECT 1
                    FROM projection_outbox AS required_job
                    WHERE required_job.event_id = {alias}.event_id
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM projection_outbox AS blocked_job
                    LEFT JOIN projection_event_results AS projection_result
                        ON projection_result.projection_name = blocked_job.projection_name
                        AND projection_result.event_id = blocked_job.event_id
                    LEFT JOIN projection_watermarks AS projection_watermark
                        ON projection_watermark.projection_name = blocked_job.projection_name
                    WHERE blocked_job.event_id = {alias}.event_id
                      AND (
                          blocked_job.status != 'APPLIED'
                          OR projection_result.status IS NULL
                          OR projection_result.status != 'SUCCESS'
                          OR projection_watermark.last_success_event_rowid IS NULL
                          OR projection_watermark.last_success_event_rowid < {alias}.rowid
                      )
                )
            )
        )
    """.strip()


def count_retention_age_eligible_events(
    connection: sqlite3.Connection,
    *,
    cutoff_at: str,
    event_types: Iterable[str] = DEFAULT_PRUNABLE_EVENT_TYPES,
) -> int:
    type_sql, params = _event_type_clause(event_types, alias="ge")
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM gateway_events AS ge
        INDEXED BY idx_gateway_events_retention_received_type_status
        WHERE ge.received_at < ?
          AND ge.command_id IS NULL
          AND {type_sql}
        """,
        (cutoff_at, *params),
    ).fetchone()
    return int(row["count"] if row else 0)


def count_retention_eligible_events(
    connection: sqlite3.Connection,
    *,
    cutoff_at: str,
    event_types: Iterable[str] = DEFAULT_PRUNABLE_EVENT_TYPES,
) -> int:
    type_sql, params = _event_type_clause(event_types, alias="ge")
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM gateway_events AS ge
        INDEXED BY idx_gateway_events_retention_received_type_status
        WHERE ge.received_at < ?
          AND {type_sql}
          AND ({projection_retention_ready_sql('ge')})
        """,
        (cutoff_at, *params),
    ).fetchone()
    return int(row["count"] if row else 0)


def select_retention_eligible_event_ids(
    connection: sqlite3.Connection,
    *,
    cutoff_at: str,
    event_types: Iterable[str] = DEFAULT_PRUNABLE_EVENT_TYPES,
    limit: int,
) -> list[str]:
    type_sql, params = _event_type_clause(event_types, alias="ge")
    rows = connection.execute(
        f"""
        SELECT ge.event_id
        FROM gateway_events AS ge
        INDEXED BY idx_gateway_events_retention_received_type_status
        WHERE ge.received_at < ?
          AND {type_sql}
          AND ({projection_retention_ready_sql('ge')})
        ORDER BY ge.rowid ASC
        LIMIT ?
        """,
        (cutoff_at, *params, max(int(limit), 1)),
    ).fetchall()
    return [str(row["event_id"]) for row in rows]


def build_projection_retention_rca(
    connection: sqlite3.Connection,
    *,
    cutoff_at: str,
    event_id: str | None = None,
    event_types: Iterable[str] = DEFAULT_PRUNABLE_EVENT_TYPES,
    limit: int = 100,
    blocked_only: bool = False,
) -> dict[str, Any]:
    normalized_types = _normalize_event_types(event_types)
    bounded_limit = min(max(int(limit), 1), 500)
    clauses: list[str] = []
    params: list[Any] = []
    if event_id is not None:
        clauses.append("ge.event_id = ?")
        params.append(_require_non_empty(event_id, "event_id"))
    else:
        placeholders = ",".join("?" for _ in normalized_types)
        clauses.extend(
            [
                f"ge.event_type IN ({placeholders})",
                "ge.received_at < ?",
                "ge.command_id IS NULL",
            ]
        )
        params.extend(normalized_types)
        params.append(cutoff_at)
        if blocked_only:
            clauses.append(f"NOT ({projection_retention_ready_sql('ge')})")
    where_sql = " AND ".join(clauses) if clauses else "1 = 1"
    index_hint = (
        ""
        if event_id is not None
        else " INDEXED BY idx_gateway_events_retention_received_type_status"
    )
    order_sql = (
        "ge.rowid DESC"
        if event_id is not None
        else "ge.received_at DESC, ge.event_id DESC"
    )
    rows = connection.execute(
        f"""
        SELECT
            ge.rowid AS event_rowid,
            ge.event_id,
            ge.event_type,
            ge.status,
            ge.command_id,
            ge.received_at,
            ge.payload_json,
            CASE WHEN re.event_id IS NULL THEN 0 ELSE 1 END AS raw_event_exists
        FROM gateway_events AS ge{index_hint}
        LEFT JOIN raw_events AS re ON re.event_id = ge.event_id
        WHERE {where_sql}
        ORDER BY {order_sql}
        LIMIT ?
        """,
        (*params, bounded_limit),
    ).fetchall()
    watermarks = {
        item.projection_name: item.to_dict()
        for item in list_projection_watermarks(connection)
    }
    items: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    venue_counts: Counter[str] = Counter()
    for row in rows:
        item = _event_rca(
            connection,
            row,
            cutoff_at=cutoff_at,
            event_types=normalized_types,
            watermarks=watermarks,
        )
        if blocked_only and item["retention_eligible"]:
            continue
        items.append(item)
        reason_counts.update(item["reason_codes"])
        venue_counts[str(item["venue"])] += 1
    blocked_count = sum(not bool(item["retention_eligible"]) for item in items)
    return {
        "status": "WARN" if blocked_count else "PASS",
        "cutoff_at": cutoff_at,
        "event_id": event_id,
        "event_types": list(normalized_types),
        "blocked_only": bool(blocked_only),
        "item_count": len(items),
        "blocked_item_count": blocked_count,
        "eligible_item_count": len(items) - blocked_count,
        "reason_counts": dict(sorted(reason_counts.items())),
        "venue_counts": dict(sorted(venue_counts.items())),
        "items": items,
        "read_only": True,
        "replay_preservation_checked": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def _event_rca(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    cutoff_at: str,
    event_types: tuple[str, ...],
    watermarks: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    event_id = str(row["event_id"])
    event_type = str(row["event_type"])
    event_rowid = int(row["event_rowid"])
    jobs = connection.execute(
        """
        SELECT
            po.projection_name,
            po.status AS outbox_status,
            po.last_error AS outbox_error,
            po.attempts AS outbox_attempts,
            per.status AS result_status,
            per.outcome AS result_outcome,
            per.error_message AS result_error,
            per.processed_at AS result_processed_at
        FROM projection_outbox AS po
        LEFT JOIN projection_event_results AS per
            ON per.projection_name = po.projection_name
            AND per.event_id = po.event_id
        WHERE po.event_id = ?
        ORDER BY po.projection_name
        """,
        (event_id,),
    ).fetchall()
    reason_codes: list[str] = []
    projection_details: list[dict[str, Any]] = []
    age_eligible = _timestamp_before(str(row["received_at"]), cutoff_at)
    gateway_event_accepted = str(row["status"]) == "ACCEPTED"
    gated = event_type in PROJECTION_GATED_EVENT_TYPES and gateway_event_accepted
    if event_type not in event_types:
        reason_codes.append("EVENT_TYPE_NOT_PRUNABLE")
    if row["command_id"] is not None:
        reason_codes.append("COMMAND_LINKED_EVENT_PROTECTED")
    if not bool(row["raw_event_exists"]):
        reason_codes.append("RAW_EVENT_MISSING")
    if not age_eligible:
        reason_codes.append("EVENT_NOT_OLD_ENOUGH")
    if gated and not jobs:
        reason_codes.append("PROJECTION_OUTBOX_MISSING")
    for job in jobs:
        projection_name = str(job["projection_name"])
        watermark = dict(watermarks.get(projection_name) or {})
        detail_reasons: list[str] = []
        if str(job["outbox_status"]) != "APPLIED":
            detail_reasons.append("PROJECTION_OUTBOX_NOT_APPLIED")
        if job["result_status"] is None:
            detail_reasons.append("PROJECTION_RESULT_MISSING")
        elif str(job["result_status"]) != "SUCCESS":
            detail_reasons.append("PROJECTION_RESULT_ERROR")
        if int(watermark.get("last_success_event_rowid") or 0) < event_rowid:
            detail_reasons.append("PROJECTION_SUCCESS_WATERMARK_BEHIND")
        reason_codes.extend(detail_reasons)
        projection_details.append(
            {
                "projection_name": projection_name,
                "outbox_status": job["outbox_status"],
                "outbox_error": job["outbox_error"],
                "outbox_attempts": int(job["outbox_attempts"] or 0),
                "result_status": job["result_status"],
                "result_outcome": job["result_outcome"],
                "result_error": job["result_error"],
                "result_processed_at": job["result_processed_at"],
                "success_watermark_rowid": int(
                    watermark.get("last_success_event_rowid") or 0
                ),
                "error_watermark_rowid": int(
                    watermark.get("last_error_event_rowid") or 0
                ),
                "retention_ready": not detail_reasons,
                "reason_codes": detail_reasons,
            }
        )
    if not gated:
        projection_details = []
    reason_codes = sorted(set(reason_codes))
    retention_eligible = not reason_codes
    if retention_eligible:
        reason_codes = ["RETENTION_ELIGIBLE"]
    return {
        "event_rowid": event_rowid,
        "event_id": event_id,
        "event_type": event_type,
        "gateway_event_status": row["status"],
        "received_at": row["received_at"],
        "age_eligible": age_eligible,
        "projection_gated": gated,
        "required_projection_count": len(jobs) if gated else 0,
        "projection_details": projection_details,
        "retention_eligible": retention_eligible,
        "replay_available": bool(row["raw_event_exists"]),
        "venue": _event_venue(event_type, row["payload_json"]),
        "reason_codes": reason_codes,
    }


def _event_type_clause(
    event_types: Iterable[str],
    *,
    alias: str,
) -> tuple[str, tuple[str, ...]]:
    normalized = _normalize_event_types(event_types)
    placeholders = ",".join("?" for _ in normalized)
    return f"{_sql_identifier(alias)}.event_type IN ({placeholders})", normalized


def _normalize_event_types(event_types: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(
        sorted(
            {
                str(event_type).strip().lower()
                for event_type in event_types
                if str(event_type).strip()
            }
        )
    )
    if not normalized:
        raise ValueError("at least one event_type is required")
    unknown = sorted(set(normalized) - set(DEFAULT_PRUNABLE_EVENT_TYPES))
    if unknown:
        raise ValueError(f"unsupported retention event_types: {unknown}")
    return normalized


def _event_venue(event_type: str, payload_json: object) -> str:
    try:
        payload = json.loads(str(payload_json or "{}"))
    except json.JSONDecodeError:
        return "UNKNOWN"
    if not isinstance(payload, Mapping):
        return "UNKNOWN"
    if event_type == "condition_event":
        return "KRX"
    venues: set[str] = set()
    _add_venue(venues, payload.get("exchange"))
    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping):
        _add_venue(venues, metadata.get("exchange"))
        _add_venue(venues, metadata.get("venue"))
    rows = payload.get("rows")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, Mapping):
                _add_venue(venues, row.get("exchange"))
                _add_venue(venues, row.get("venue"))
    if len(venues) > 1:
        return "MIXED"
    if venues:
        return next(iter(venues))
    return "KRX" if event_type == "price_tick" else "UNKNOWN"


def _add_venue(venues: set[str], value: object) -> None:
    normalized = str(value or "").strip().upper()
    if normalized in {"KRX", "NXT"}:
        venues.add(normalized)
    elif normalized:
        venues.add("UNKNOWN")


def _timestamp_before(value: str, cutoff_at: str) -> bool:
    try:
        return parse_timestamp(value, "received_at") < parse_timestamp(
            cutoff_at,
            "cutoff_at",
        )
    except ValueError:
        return value < cutoff_at


def _sql_identifier(value: str) -> str:
    normalized = str(value).strip()
    if not normalized.replace("_", "").isalnum():
        raise ValueError("invalid SQL identifier")
    return normalized


def _sql_string_list(values: Iterable[str]) -> str:
    return ",".join(f"'{value}'" for value in values)


def _require_non_empty(value: object, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized
