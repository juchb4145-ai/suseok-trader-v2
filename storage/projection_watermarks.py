from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from domain.broker.utils import datetime_to_wire, utc_now

ProjectionResultStatus = Literal["SUCCESS", "ERROR"]
PROJECTION_RESULT_STATUSES: frozenset[str] = frozenset({"SUCCESS", "ERROR"})


class ProjectionWatermarkBackfillSafetyError(ValueError):
    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "BLOCKED",
            "error_message": str(self),
            "reason_codes": ["PROJECTION_EVENT_RESULT_BACKFILL_DISABLED"],
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionWatermark:
    projection_name: str
    last_event_rowid: int
    last_event_id: str | None = None
    last_event_received_at: str | None = None
    last_processed_at: str | None = None
    updated_at: str | None = None
    metadata: dict[str, Any] | None = None
    last_success_event_rowid: int = 0
    last_success_event_id: str | None = None
    last_success_event_received_at: str | None = None
    last_success_processed_at: str | None = None
    last_error_event_rowid: int = 0
    last_error_event_id: str | None = None
    last_error_event_received_at: str | None = None
    last_error_processed_at: str | None = None
    last_error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "projection_name": self.projection_name,
            "last_event_rowid": self.last_event_rowid,
            "last_event_id": self.last_event_id,
            "last_event_received_at": self.last_event_received_at,
            "last_processed_at": self.last_processed_at,
            "updated_at": self.updated_at,
            "metadata": self.metadata or {},
            "last_success_event_rowid": self.last_success_event_rowid,
            "last_success_event_id": self.last_success_event_id,
            "last_success_event_received_at": self.last_success_event_received_at,
            "last_success_processed_at": self.last_success_processed_at,
            "last_error_event_rowid": self.last_error_event_rowid,
            "last_error_event_id": self.last_error_event_id,
            "last_error_event_received_at": self.last_error_event_received_at,
            "last_error_processed_at": self.last_error_processed_at,
            "last_error_message": self.last_error_message,
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionEventResult:
    projection_name: str
    event_id: str
    event_rowid: int
    event_type: str
    status: ProjectionResultStatus
    outcome: str
    error_message: str | None
    first_processed_at: str
    processed_at: str
    attempt_count: int
    metadata: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "projection_name": self.projection_name,
            "event_id": self.event_id,
            "event_rowid": self.event_rowid,
            "event_type": self.event_type,
            "status": self.status,
            "outcome": self.outcome,
            "error_message": self.error_message,
            "first_processed_at": self.first_processed_at,
            "processed_at": self.processed_at,
            "attempt_count": self.attempt_count,
            "metadata": dict(self.metadata),
        }


def get_projection_watermark(
    connection: sqlite3.Connection,
    projection_name: str,
) -> ProjectionWatermark:
    normalized = _normalize_projection_name(projection_name)
    row = connection.execute(
        "SELECT * FROM projection_watermarks WHERE projection_name = ?",
        (normalized,),
    ).fetchone()
    if row is None:
        return ProjectionWatermark(
            projection_name=normalized,
            last_event_rowid=0,
            metadata={},
        )
    return _row_to_watermark(row)


def list_projection_watermarks(
    connection: sqlite3.Connection,
) -> list[ProjectionWatermark]:
    rows = connection.execute(
        "SELECT * FROM projection_watermarks ORDER BY projection_name"
    ).fetchall()
    return [_row_to_watermark(row) for row in rows]


def advance_projection_watermark(
    connection: sqlite3.Connection,
    projection_name: str,
    *,
    last_event_rowid: int,
    last_event_id: str | None = None,
    last_event_received_at: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    commit: bool = False,
) -> ProjectionWatermark:
    normalized = _normalize_projection_name(projection_name)
    rowid = max(int(last_event_rowid), 0)
    now = datetime_to_wire(utc_now())
    _ensure_watermark_row(connection, normalized, now=now)
    metadata_json = json.dumps(dict(metadata or {}), ensure_ascii=False, sort_keys=True)
    connection.execute(
        """
        UPDATE projection_watermarks
        SET
            last_event_rowid = CASE
                WHEN last_event_rowid < ? THEN ? ELSE last_event_rowid END,
            last_event_id = CASE
                WHEN last_event_rowid < ? THEN ? ELSE last_event_id END,
            last_event_received_at = CASE
                WHEN last_event_rowid < ? THEN ? ELSE last_event_received_at END,
            last_success_event_rowid = CASE
                WHEN last_success_event_rowid < ? THEN ?
                ELSE last_success_event_rowid END,
            last_success_event_id = CASE
                WHEN last_success_event_rowid < ? THEN ?
                ELSE last_success_event_id END,
            last_success_event_received_at = CASE
                WHEN last_success_event_rowid < ? THEN ?
                ELSE last_success_event_received_at END,
            last_success_processed_at = CASE
                WHEN last_success_event_rowid < ? THEN ?
                ELSE last_success_processed_at END,
            last_processed_at = ?,
            updated_at = ?,
            metadata_json = CASE
                WHEN last_event_rowid < ? THEN ? ELSE metadata_json END
        WHERE projection_name = ?
        """,
        (
            rowid,
            rowid,
            rowid,
            last_event_id,
            rowid,
            last_event_received_at,
            rowid,
            rowid,
            rowid,
            last_event_id,
            rowid,
            last_event_received_at,
            rowid,
            now,
            now,
            now,
            rowid,
            metadata_json,
            normalized,
        ),
    )
    if commit:
        connection.commit()
    return get_projection_watermark(connection, normalized)


def advance_projection_error_watermark(
    connection: sqlite3.Connection,
    projection_name: str,
    *,
    last_event_rowid: int,
    last_event_id: str | None,
    last_event_received_at: str | None,
    error_message: str,
    commit: bool = False,
) -> ProjectionWatermark:
    normalized = _normalize_projection_name(projection_name)
    rowid = max(int(last_event_rowid), 0)
    now = datetime_to_wire(utc_now())
    _ensure_watermark_row(connection, normalized, now=now)
    connection.execute(
        """
        UPDATE projection_watermarks
        SET
            last_error_event_rowid = CASE
                WHEN last_error_event_rowid <= ? THEN ?
                ELSE last_error_event_rowid END,
            last_error_event_id = CASE
                WHEN last_error_event_rowid <= ? THEN ?
                ELSE last_error_event_id END,
            last_error_event_received_at = CASE
                WHEN last_error_event_rowid <= ? THEN ?
                ELSE last_error_event_received_at END,
            last_error_processed_at = CASE
                WHEN last_error_event_rowid <= ? THEN ?
                ELSE last_error_processed_at END,
            last_error_message = CASE
                WHEN last_error_event_rowid <= ? THEN ?
                ELSE last_error_message END,
            last_processed_at = ?,
            updated_at = ?
        WHERE projection_name = ?
        """,
        (
            rowid,
            rowid,
            rowid,
            last_event_id,
            rowid,
            last_event_received_at,
            rowid,
            now,
            rowid,
            str(error_message),
            now,
            now,
            normalized,
        ),
    )
    if commit:
        connection.commit()
    return get_projection_watermark(connection, normalized)


def record_projection_event_result(
    connection: sqlite3.Connection,
    *,
    projection_name: str,
    event_id: str,
    status: ProjectionResultStatus | str,
    outcome: str,
    event_rowid: int | None = None,
    event_type: str | None = None,
    event_received_at: str | None = None,
    error_message: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    commit: bool = False,
) -> ProjectionEventResult | None:
    normalized_projection = _normalize_projection_name(projection_name)
    normalized_event_id = _require_non_empty(event_id, "event_id")
    normalized_status = str(status).strip().upper()
    if normalized_status not in PROJECTION_RESULT_STATUSES:
        raise ValueError("status must be SUCCESS or ERROR")
    normalized_outcome = _require_non_empty(outcome, "outcome").upper()
    position = _resolve_event_position(
        connection,
        event_id=normalized_event_id,
        event_rowid=event_rowid,
        event_type=event_type,
        event_received_at=event_received_at,
    )
    if position is None:
        return None
    if normalized_status == "ERROR" and not str(error_message or "").strip():
        raise ValueError("error_message is required for ERROR projection result")
    now = datetime_to_wire(utc_now())
    metadata_json = json.dumps(dict(metadata or {}), ensure_ascii=False, sort_keys=True)
    connection.execute(
        """
        INSERT INTO projection_event_results (
            projection_name,
            event_id,
            event_rowid,
            event_type,
            status,
            outcome,
            error_message,
            first_processed_at,
            processed_at,
            attempt_count,
            metadata_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
        ON CONFLICT(projection_name, event_id) DO UPDATE SET
            event_rowid = excluded.event_rowid,
            event_type = excluded.event_type,
            status = excluded.status,
            outcome = excluded.outcome,
            error_message = excluded.error_message,
            processed_at = excluded.processed_at,
            attempt_count = projection_event_results.attempt_count + 1,
            metadata_json = excluded.metadata_json
        """,
        (
            normalized_projection,
            normalized_event_id,
            position["event_rowid"],
            position["event_type"],
            normalized_status,
            normalized_outcome,
            str(error_message) if error_message is not None else None,
            now,
            now,
            metadata_json,
        ),
    )
    if normalized_status == "SUCCESS":
        advance_projection_watermark(
            connection,
            normalized_projection,
            last_event_rowid=position["event_rowid"],
            last_event_id=normalized_event_id,
            last_event_received_at=position["received_at"],
            metadata={"event_type": position["event_type"], "outcome": normalized_outcome},
            commit=False,
        )
    else:
        advance_projection_error_watermark(
            connection,
            normalized_projection,
            last_event_rowid=position["event_rowid"],
            last_event_id=normalized_event_id,
            last_event_received_at=position["received_at"],
            error_message=str(error_message),
            commit=False,
        )
    if commit:
        connection.commit()
    return get_projection_event_result(
        connection,
        projection_name=normalized_projection,
        event_id=normalized_event_id,
    )


def get_projection_event_result(
    connection: sqlite3.Connection,
    *,
    projection_name: str,
    event_id: str,
) -> ProjectionEventResult | None:
    row = connection.execute(
        """
        SELECT *
        FROM projection_event_results
        WHERE projection_name = ? AND event_id = ?
        """,
        (_normalize_projection_name(projection_name), _require_non_empty(event_id, "event_id")),
    ).fetchone()
    return None if row is None else _row_to_event_result(row)


def list_projection_event_results(
    connection: sqlite3.Connection,
    *,
    projection_name: str | None = None,
    status: str | None = None,
    event_id: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if projection_name is not None:
        clauses.append("projection_name = ?")
        params.append(_normalize_projection_name(projection_name))
    if status is not None:
        normalized_status = str(status).strip().upper()
        if normalized_status not in PROJECTION_RESULT_STATUSES:
            raise ValueError("status must be SUCCESS or ERROR")
        clauses.append("status = ?")
        params.append(normalized_status)
    if event_id is not None:
        clauses.append("event_id = ?")
        params.append(_require_non_empty(event_id, "event_id"))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(min(max(int(limit), 1), 500))
    rows = connection.execute(
        f"""
        SELECT *
        FROM projection_event_results
        {where_sql}
        ORDER BY event_rowid DESC, projection_name ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_row_to_event_result(row).to_dict() for row in rows]


def get_projection_watermark_status(connection: sqlite3.Connection) -> dict[str, Any]:
    watermarks = list_projection_watermarks(connection)
    rows = connection.execute(
        """
        SELECT projection_name, status, COUNT(*) AS count
        FROM projection_event_results
        GROUP BY projection_name, status
        ORDER BY projection_name, status
        """
    ).fetchall()
    by_projection: dict[str, dict[str, int]] = {}
    for row in rows:
        bucket = by_projection.setdefault(
            str(row["projection_name"]),
            {"success_count": 0, "error_count": 0},
        )
        bucket[f"{str(row['status']).lower()}_count"] = int(row["count"])
    unresolved_error_count = sum(
        int(item.get("error_count") or 0) for item in by_projection.values()
    )
    return {
        "status": "WARN" if unresolved_error_count else "PASS",
        "watermarks": [watermark.to_dict() for watermark in watermarks],
        "by_projection": by_projection,
        "projection_count": len({item.projection_name for item in watermarks} | set(by_projection)),
        "unresolved_error_count": unresolved_error_count,
        "recent_errors": list_projection_event_results(
            connection,
            status="ERROR",
            limit=20,
        ),
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def backfill_projection_event_results_from_outbox(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
    dry_run: bool = True,
    apply_enabled: bool = False,
) -> dict[str, Any]:
    bounded_limit = min(max(int(limit), 1), 5000)
    total_row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM projection_outbox AS po
        LEFT JOIN projection_event_results AS per
            ON per.projection_name = po.projection_name
            AND per.event_id = po.event_id
        WHERE po.status = 'APPLIED'
          AND (per.status IS NULL OR per.status != 'SUCCESS')
        """
    ).fetchone()
    rows = connection.execute(
        """
        SELECT
            po.projection_name,
            po.event_id,
            po.event_type,
            po.event_rowid,
            ge.received_at
        FROM projection_outbox AS po
        JOIN gateway_events AS ge ON ge.event_id = po.event_id
        LEFT JOIN projection_event_results AS per
            ON per.projection_name = po.projection_name
            AND per.event_id = po.event_id
        WHERE po.status = 'APPLIED'
          AND (per.status IS NULL OR per.status != 'SUCCESS')
        ORDER BY po.event_rowid ASC, po.projection_name ASC
        LIMIT ?
        """,
        (bounded_limit,),
    ).fetchall()
    applied_count = 0
    if not dry_run:
        if not apply_enabled:
            raise ProjectionWatermarkBackfillSafetyError(
                "projection event-result backfill apply is disabled"
            )
        try:
            for row in rows:
                result = record_projection_event_result(
                    connection,
                    projection_name=row["projection_name"],
                    event_id=row["event_id"],
                    event_rowid=row["event_rowid"],
                    event_type=row["event_type"],
                    event_received_at=row["received_at"],
                    status="SUCCESS",
                    outcome="BACKFILLED_FROM_APPLIED_OUTBOX",
                    metadata={"source": "projection_outbox_backfill"},
                    commit=False,
                )
                applied_count += int(result is not None)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    return {
        "dry_run": bool(dry_run),
        "candidate_count": int(total_row["count"] if total_row else 0),
        "selected_count": len(rows),
        "applied_count": applied_count,
        "remaining_count": max(
            int(total_row["count"] if total_row else 0) - applied_count,
            0,
        ),
        "limit": bounded_limit,
        "source_status_required": "APPLIED",
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }


def reset_projection_watermark(
    connection: sqlite3.Connection,
    projection_name: str,
    *,
    commit: bool = False,
) -> ProjectionWatermark:
    normalized = _normalize_projection_name(projection_name)
    connection.execute(
        "DELETE FROM projection_event_results WHERE projection_name = ?",
        (normalized,),
    )
    connection.execute(
        "DELETE FROM projection_watermarks WHERE projection_name = ?",
        (normalized,),
    )
    if commit:
        connection.commit()
    return get_projection_watermark(connection, normalized)


def _ensure_watermark_row(
    connection: sqlite3.Connection,
    projection_name: str,
    *,
    now: str,
) -> None:
    connection.execute(
        """
        INSERT INTO projection_watermarks (
            projection_name,
            last_event_rowid,
            last_processed_at,
            updated_at,
            metadata_json
        )
        VALUES (?, 0, ?, ?, '{}')
        ON CONFLICT(projection_name) DO NOTHING
        """,
        (projection_name, now, now),
    )


def _resolve_event_position(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    event_rowid: int | None,
    event_type: str | None,
    event_received_at: str | None,
) -> dict[str, Any] | None:
    if event_rowid is not None and event_type and event_received_at:
        return {
            "event_rowid": int(event_rowid),
            "event_type": str(event_type).strip().lower(),
            "received_at": str(event_received_at),
        }
    row = connection.execute(
        """
        SELECT rowid AS event_rowid, event_type, received_at
        FROM gateway_events
        WHERE event_id = ?
        """,
        (event_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "event_rowid": int(event_rowid or row["event_rowid"]),
        "event_type": str(event_type or row["event_type"]).strip().lower(),
        "received_at": str(event_received_at or row["received_at"]),
    }


def _normalize_projection_name(projection_name: str) -> str:
    normalized = str(projection_name or "").strip().lower()
    if not normalized:
        raise ValueError("projection_name must not be empty")
    return normalized


def _require_non_empty(value: object, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _json_object(value: object) -> dict[str, Any]:
    try:
        payload = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _row_to_watermark(row: sqlite3.Row) -> ProjectionWatermark:
    keys = set(row.keys())
    return ProjectionWatermark(
        projection_name=str(row["projection_name"]),
        last_event_rowid=int(row["last_event_rowid"]),
        last_event_id=row["last_event_id"],
        last_event_received_at=row["last_event_received_at"],
        last_processed_at=row["last_processed_at"],
        updated_at=row["updated_at"],
        metadata=_json_object(row["metadata_json"]),
        last_success_event_rowid=int(row["last_success_event_rowid"] or 0)
        if "last_success_event_rowid" in keys
        else 0,
        last_success_event_id=row["last_success_event_id"]
        if "last_success_event_id" in keys
        else None,
        last_success_event_received_at=row["last_success_event_received_at"]
        if "last_success_event_received_at" in keys
        else None,
        last_success_processed_at=row["last_success_processed_at"]
        if "last_success_processed_at" in keys
        else None,
        last_error_event_rowid=int(row["last_error_event_rowid"] or 0)
        if "last_error_event_rowid" in keys
        else 0,
        last_error_event_id=row["last_error_event_id"]
        if "last_error_event_id" in keys
        else None,
        last_error_event_received_at=row["last_error_event_received_at"]
        if "last_error_event_received_at" in keys
        else None,
        last_error_processed_at=row["last_error_processed_at"]
        if "last_error_processed_at" in keys
        else None,
        last_error_message=row["last_error_message"]
        if "last_error_message" in keys
        else None,
    )


def _row_to_event_result(row: sqlite3.Row) -> ProjectionEventResult:
    return ProjectionEventResult(
        projection_name=str(row["projection_name"]),
        event_id=str(row["event_id"]),
        event_rowid=int(row["event_rowid"]),
        event_type=str(row["event_type"]),
        status=str(row["status"]),
        outcome=str(row["outcome"]),
        error_message=row["error_message"],
        first_processed_at=str(row["first_processed_at"]),
        processed_at=str(row["processed_at"]),
        attempt_count=int(row["attempt_count"]),
        metadata=_json_object(row["metadata_json"]),
    )
