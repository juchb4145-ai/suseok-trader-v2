from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, new_message_id, parse_timestamp, utc_now
from storage.projection_watermarks import (
    get_projection_watermark,
    record_projection_event_result,
)

from services.config import Settings, load_settings
from services.live_sim.live_sim_service import handle_live_sim_gateway_event

LIVE_SIM_LIFECYCLE_PROJECTION = "live_sim_lifecycle"
LIVE_SIM_LIFECYCLE_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "command_started",
        "command_ack",
        "command_failed",
        "execution_event",
        "order_rejected",
        "cancel_ack",
        "cancel_rejected",
        "balance_snapshot",
        "account_snapshot",
        "kiwoom_balance_chejan",
        "kiwoom_order_chejan",
    }
)
_UNRESOLVED_STATUSES = ("PENDING", "PROCESSING", "DEAD_LETTER")


@dataclass(frozen=True, kw_only=True)
class LiveSimLifecycleBatchResult:
    run_id: str
    status: str
    claimed_count: int = 0
    applied_count: int = 0
    duplicate_count: int = 0
    error_count: int = 0
    dead_letter_count: int = 0
    stale_reset_count: int = 0
    blocked_by_dead_letter: bool = False
    errors: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "claimed_count": self.claimed_count,
            "applied_count": self.applied_count,
            "duplicate_count": self.duplicate_count,
            "error_count": self.error_count,
            "dead_letter_count": self.dead_letter_count,
            "stale_reset_count": self.stale_reset_count,
            "blocked_by_dead_letter": self.blocked_by_dead_letter,
            "errors": list(self.errors),
            "consumer_source": "durable_worker",
            "no_order_commands_created": True,
            "live_real_allowed": False,
        }


class _DeferredCommitConnection:
    """Delegate SQLite work while keeping the caller's transaction boundary."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        raise RuntimeError("lifecycle handler cannot roll back the consumer transaction")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)


def is_live_sim_lifecycle_event(event_type: str) -> bool:
    return str(event_type).strip().lower() in LIVE_SIM_LIFECYCLE_EVENT_TYPES


def enqueue_live_sim_lifecycle_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
) -> dict[str, Any]:
    event_type = event.event_type.strip().lower()
    if event_type not in LIVE_SIM_LIFECYCLE_EVENT_TYPES:
        return {
            "status": "IGNORED_EVENT_TYPE",
            "event_id": event.event_id,
            "event_type": event_type,
            "created": False,
        }
    source_row = connection.execute(
        """
        SELECT rowid AS event_rowid, status
        FROM gateway_events
        WHERE event_id = ?
        """,
        (event.event_id,),
    ).fetchone()
    if source_row is None:
        raise ValueError(
            "gateway event must be durable before lifecycle enqueue: "
            f"{event.event_id}"
        )
    if str(source_row["status"]) != "ACCEPTED":
        return {
            "status": "IGNORED_SOURCE_NOT_ACCEPTED",
            "event_id": event.event_id,
            "event_type": event_type,
            "created": False,
        }
    now = datetime_to_wire(utc_now())
    cursor = connection.execute(
        """
        INSERT INTO live_sim_lifecycle_inbox (
            event_id,
            event_rowid,
            event_type,
            source,
            status,
            attempts,
            available_at,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, 'PENDING', 0, ?, ?, ?)
        ON CONFLICT(event_id) DO NOTHING
        """,
        (
            event.event_id,
            int(source_row["event_rowid"]),
            event_type,
            event.source,
            now,
            now,
            now,
        ),
    )
    connection.commit()
    row = _inbox_row(connection, event.event_id)
    return {
        "status": "ENQUEUED" if cursor.rowcount > 0 else "DUPLICATE",
        "event_id": event.event_id,
        "event_type": event_type,
        "created": cursor.rowcount > 0,
        "inbox_status": None if row is None else row["status"],
        "event_rowid": None if row is None else int(row["event_rowid"]),
    }


def process_live_sim_lifecycle_inline(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    if not is_live_sim_lifecycle_event(event.event_type):
        return {
            "status": "IGNORED_EVENT_TYPE",
            "event_id": event.event_id,
            "inline_compatibility": True,
        }
    enqueue = enqueue_live_sim_lifecycle_event(connection, event)
    result = _apply_lifecycle_event(
        connection,
        event,
        settings=settings or load_settings(),
        consumer_source="gateway_inline_compatibility",
        expected_owner=None,
    )
    result["enqueue_status"] = enqueue["status"]
    result["inline_compatibility"] = True
    if result["status"] in {"ERROR", "DEAD_LETTER"}:
        raise RuntimeError(str(result.get("error_message") or "lifecycle apply failed"))
    return result


def process_live_sim_lifecycle_batch(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    limit: int | None = None,
    owner_id: str | None = None,
) -> LiveSimLifecycleBatchResult:
    resolved_settings = settings or load_settings()
    run_id = new_message_id("live_sim_lifecycle_consumer")
    if not (
        resolved_settings.live_sim_lifecycle_consumer_enabled
        and resolved_settings.live_sim_lifecycle_worker_enabled
    ):
        return LiveSimLifecycleBatchResult(run_id=run_id, status="DISABLED")
    resolved_owner = owner_id or run_id
    bounded_limit = min(
        max(int(limit or resolved_settings.live_sim_lifecycle_batch_size), 1),
        500,
    )
    stale_reset_count = reset_stale_live_sim_lifecycle_processing(
        connection,
        settings=resolved_settings,
    )
    claimed_count = 0
    applied_count = 0
    duplicate_count = 0
    error_count = 0
    dead_letter_count = 0
    errors: list[dict[str, Any]] = []

    for _ in range(bounded_limit):
        claimed = _claim_next_inbox_row(connection, owner_id=resolved_owner)
        if claimed is None:
            break
        claimed_count += 1
        source_event = _gateway_event(connection, str(claimed["event_id"]))
        if source_event is None:
            failure = _mark_lifecycle_failure(
                connection,
                event_id=str(claimed["event_id"]),
                owner_id=resolved_owner,
                error_message="SOURCE_GATEWAY_EVENT_MISSING",
                settings=resolved_settings,
            )
            errors.append(failure)
            if failure["status"] == "DEAD_LETTER":
                dead_letter_count += 1
            else:
                error_count += 1
            break
        result = _apply_lifecycle_event(
            connection,
            source_event,
            settings=resolved_settings,
            consumer_source="durable_worker",
            expected_owner=resolved_owner,
        )
        if result["status"] == "APPLIED":
            applied_count += 1
        elif result["status"] == "DUPLICATE":
            duplicate_count += 1
        else:
            errors.append(result)
            if result["status"] == "DEAD_LETTER":
                dead_letter_count += 1
            else:
                error_count += 1
            break

    blocked = _earliest_unresolved_status(connection) == "DEAD_LETTER"
    status = "IDLE"
    if claimed_count:
        status = "COMPLETED_WITH_ERRORS" if errors else "COMPLETED"
    if blocked and not errors:
        status = "BLOCKED_DEAD_LETTER"
    return LiveSimLifecycleBatchResult(
        run_id=run_id,
        status=status,
        claimed_count=claimed_count,
        applied_count=applied_count,
        duplicate_count=duplicate_count,
        error_count=error_count,
        dead_letter_count=dead_letter_count,
        stale_reset_count=stale_reset_count,
        blocked_by_dead_letter=blocked,
        errors=tuple(errors),
    )


def reset_stale_live_sim_lifecycle_processing(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> int:
    resolved_settings = settings or load_settings()
    now = utc_now()
    cutoff = datetime_to_wire(
        now - timedelta(seconds=resolved_settings.live_sim_lifecycle_processing_ttl_sec)
    )
    rows = connection.execute(
        """
        SELECT event_id, attempts
        FROM live_sim_lifecycle_inbox
        WHERE status = 'PROCESSING'
          AND locked_at IS NOT NULL
          AND julianday(locked_at) <= julianday(?)
        ORDER BY event_rowid
        """,
        (cutoff,),
    ).fetchall()
    if not rows:
        return 0
    now_wire = datetime_to_wire(now)
    connection.execute("BEGIN IMMEDIATE")
    try:
        for row in rows:
            next_attempts = int(row["attempts"]) + 1
            next_status = (
                "DEAD_LETTER"
                if next_attempts >= resolved_settings.live_sim_lifecycle_retry_limit
                else "PENDING"
            )
            connection.execute(
                """
                UPDATE live_sim_lifecycle_inbox
                SET status = ?,
                    attempts = ?,
                    available_at = ?,
                    locked_by = NULL,
                    locked_at = NULL,
                    last_error = 'STALE_PROCESSING_RECLAIMED',
                    updated_at = ?,
                    processed_at = CASE WHEN ? = 'DEAD_LETTER' THEN ? ELSE NULL END
                WHERE event_id = ? AND status = 'PROCESSING'
                """,
                (
                    next_status,
                    next_attempts,
                    now_wire,
                    now_wire,
                    next_status,
                    now_wire,
                    row["event_id"],
                ),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return len(rows)


def reset_live_sim_lifecycle_dead_letter(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any]:
    normalized_event_id = str(event_id).strip()
    if not normalized_event_id:
        raise ValueError("event_id must not be empty")
    now = datetime_to_wire(utc_now())
    cursor = connection.execute(
        """
        UPDATE live_sim_lifecycle_inbox
        SET status = 'PENDING',
            attempts = 0,
            available_at = ?,
            locked_by = NULL,
            locked_at = NULL,
            last_error = NULL,
            processed_at = NULL,
            updated_at = ?
        WHERE event_id = ? AND status = 'DEAD_LETTER'
        """,
        (now, now, normalized_event_id),
    )
    connection.commit()
    return {
        "status": "RESET" if cursor.rowcount > 0 else "NOT_DEAD_LETTER",
        "event_id": normalized_event_id,
        "reset_count": int(cursor.rowcount),
        "no_order_commands_created": True,
    }


def list_live_sim_lifecycle_inbox(
    connection: sqlite3.Connection,
    *,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if status is not None:
        normalized_status = str(status).strip().upper()
        if normalized_status not in {"PENDING", "PROCESSING", "APPLIED", "DEAD_LETTER"}:
            raise ValueError("invalid lifecycle inbox status")
        clauses.append("status = ?")
        params.append(normalized_status)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(min(max(int(limit), 1), 500))
    rows = connection.execute(
        f"""
        SELECT *
        FROM live_sim_lifecycle_inbox
        {where_sql}
        ORDER BY event_rowid DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def get_live_sim_lifecycle_consumer_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    counts = {key: 0 for key in ("PENDING", "PROCESSING", "APPLIED", "DEAD_LETTER")}
    for row in connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM live_sim_lifecycle_inbox
        GROUP BY status
        """
    ).fetchall():
        counts[str(row["status"])] = int(row["count"])
    activation = connection.execute(
        "SELECT value FROM app_metadata WHERE key = 'live_sim_lifecycle_inbox_started_at'"
    ).fetchone()
    started_at = None if activation is None else str(activation["value"])
    event_types = tuple(sorted(LIVE_SIM_LIFECYCLE_EVENT_TYPES))
    placeholders = ",".join("?" for _ in event_types)
    missing_inbox_count = 0
    if started_at:
        missing_inbox_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM gateway_events AS ge
                LEFT JOIN live_sim_lifecycle_inbox AS inbox ON inbox.event_id = ge.event_id
                WHERE ge.status = 'ACCEPTED'
                  AND ge.event_type IN ({placeholders})
                  AND julianday(ge.received_at) >= julianday(?)
                  AND inbox.event_id IS NULL
                """,
                (*event_types, started_at),
            ).fetchone()["count"]
        )
    applied_without_result_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM live_sim_lifecycle_inbox AS inbox
            LEFT JOIN projection_event_results AS result
              ON result.projection_name = ? AND result.event_id = inbox.event_id
            WHERE inbox.status = 'APPLIED' AND result.event_id IS NULL
            """,
            (LIVE_SIM_LIFECYCLE_PROJECTION,),
        ).fetchone()["count"]
    )
    stale_cutoff = datetime_to_wire(
        utc_now()
        - timedelta(seconds=resolved_settings.live_sim_lifecycle_processing_ttl_sec)
    )
    stale_processing_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM live_sim_lifecycle_inbox
            WHERE status = 'PROCESSING'
              AND locked_at IS NOT NULL
              AND julianday(locked_at) <= julianday(?)
            """,
            (stale_cutoff,),
        ).fetchone()["count"]
    )
    oldest = connection.execute(
        """
        SELECT event_id, event_rowid, event_type, status, created_at, last_error
        FROM live_sim_lifecycle_inbox
        WHERE status IN ('PENDING', 'PROCESSING', 'DEAD_LETTER')
        ORDER BY event_rowid
        LIMIT 1
        """
    ).fetchone()
    watermark = get_projection_watermark(
        connection,
        LIVE_SIM_LIFECYCLE_PROJECTION,
    ).to_dict()
    failures: list[str] = []
    warnings: list[str] = []
    if counts["DEAD_LETTER"]:
        failures.append("LIFECYCLE_DEAD_LETTER_PRESENT")
    if missing_inbox_count:
        failures.append("LIFECYCLE_INBOX_GAP")
    if applied_without_result_count:
        failures.append("LIFECYCLE_APPLIED_RESULT_GAP")
    if stale_processing_count:
        failures.append("LIFECYCLE_STALE_PROCESSING")
    if counts["PENDING"] or counts["PROCESSING"]:
        warnings.append("LIFECYCLE_BACKLOG_PRESENT")
    if not resolved_settings.live_sim_lifecycle_consumer_enabled:
        warnings.append("LIFECYCLE_CONSUMER_DISABLED")
    if not resolved_settings.live_sim_lifecycle_worker_enabled:
        warnings.append("LIFECYCLE_WORKER_DISABLED")
    status = "FAIL" if failures else "WARN" if warnings else "PASS"
    return {
        "status": status,
        "reason_codes": [*failures, *warnings],
        "consumer_enabled": resolved_settings.live_sim_lifecycle_consumer_enabled,
        "worker_enabled": resolved_settings.live_sim_lifecycle_worker_enabled,
        "worker_interval_sec": resolved_settings.live_sim_lifecycle_worker_interval_sec,
        "batch_size": resolved_settings.live_sim_lifecycle_batch_size,
        "retry_limit": resolved_settings.live_sim_lifecycle_retry_limit,
        "processing_ttl_sec": resolved_settings.live_sim_lifecycle_processing_ttl_sec,
        "inline_compatibility_mode": True,
        "request_path_removed": False,
        "total_count": sum(counts.values()),
        "pending_count": counts["PENDING"],
        "processing_count": counts["PROCESSING"],
        "applied_count": counts["APPLIED"],
        "dead_letter_count": counts["DEAD_LETTER"],
        "stale_processing_count": stale_processing_count,
        "missing_inbox_count": missing_inbox_count,
        "applied_without_result_count": applied_without_result_count,
        "blocked_by_dead_letter": _earliest_unresolved_status(connection) == "DEAD_LETTER",
        "oldest_unresolved": None if oldest is None else dict(oldest),
        "watermark": watermark,
        "inbox_started_at": started_at,
        "supported_event_types": list(event_types),
        "read_only": True,
        "no_order_commands_created": True,
        "live_real_allowed": False,
    }


def _apply_lifecycle_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    consumer_source: str,
    expected_owner: str | None,
) -> dict[str, Any]:
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = _inbox_row(connection, event.event_id)
        if row is None:
            connection.rollback()
            return {
                "status": "ERROR",
                "event_id": event.event_id,
                "error_message": "LIFECYCLE_INBOX_ROW_MISSING",
            }
        if str(row["status"]) == "APPLIED":
            connection.commit()
            return {
                "status": "DUPLICATE",
                "event_id": event.event_id,
                "consumer_source": row["consumer_source"],
                "result": _json_object(row["result_json"]),
            }
        if str(row["status"]) == "DEAD_LETTER":
            connection.rollback()
            return {
                "status": "DEAD_LETTER",
                "event_id": event.event_id,
                "error_message": row["last_error"],
            }
        if expected_owner is not None:
            if str(row["status"]) != "PROCESSING" or row["locked_by"] != expected_owner:
                connection.rollback()
                return {
                    "status": "ERROR",
                    "event_id": event.event_id,
                    "error_message": "LIFECYCLE_CLAIM_LOST",
                }
        else:
            if str(row["status"]) == "PROCESSING":
                connection.rollback()
                return {
                    "status": "DEFERRED",
                    "event_id": event.event_id,
                    "consumer_source": "durable_worker",
                }
            now = datetime_to_wire(utc_now())
            connection.execute(
                """
                UPDATE live_sim_lifecycle_inbox
                SET status = 'PROCESSING',
                    locked_by = ?,
                    locked_at = ?,
                    updated_at = ?
                WHERE event_id = ? AND status = 'PENDING'
                """,
                (consumer_source, now, now, event.event_id),
            )
        deferred_connection = _DeferredCommitConnection(connection)
        handler_result = handle_live_sim_gateway_event(
            deferred_connection,  # type: ignore[arg-type]
            event,
            settings=settings,
        )
        outcome = "HANDLED" if bool(handler_result.get("handled")) else "IGNORED"
        record_projection_event_result(
            connection,
            projection_name=LIVE_SIM_LIFECYCLE_PROJECTION,
            event_id=event.event_id,
            event_type=event.event_type,
            status="SUCCESS",
            outcome=outcome,
            metadata={
                "consumer_source": consumer_source,
                "handler_result": handler_result,
            },
            commit=False,
        )
        now = datetime_to_wire(utc_now())
        connection.execute(
            """
            UPDATE live_sim_lifecycle_inbox
            SET status = 'APPLIED',
                consumer_source = ?,
                result_json = ?,
                last_error = NULL,
                locked_by = NULL,
                locked_at = NULL,
                updated_at = ?,
                processed_at = ?
            WHERE event_id = ?
            """,
            (
                consumer_source,
                _json_dumps(handler_result),
                now,
                now,
                event.event_id,
            ),
        )
        connection.commit()
        return {
            "status": "APPLIED",
            "event_id": event.event_id,
            "consumer_source": consumer_source,
            "handler_result": handler_result,
        }
    except Exception as exc:
        connection.rollback()
        return _mark_lifecycle_failure(
            connection,
            event_id=event.event_id,
            owner_id=expected_owner or consumer_source,
            error_message=str(exc),
            settings=settings,
        )


def _mark_lifecycle_failure(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    owner_id: str,
    error_message: str,
    settings: Settings,
) -> dict[str, Any]:
    del owner_id
    connection.rollback()
    connection.execute("BEGIN IMMEDIATE")
    try:
        row = _inbox_row(connection, event_id)
        if row is None:
            connection.rollback()
            return {
                "status": "ERROR",
                "event_id": event_id,
                "error_message": error_message,
            }
        attempts = int(row["attempts"]) + 1
        status = (
            "DEAD_LETTER"
            if attempts >= settings.live_sim_lifecycle_retry_limit
            else "PENDING"
        )
        now = utc_now()
        now_wire = datetime_to_wire(now)
        available_at = datetime_to_wire(
            now + timedelta(seconds=settings.live_sim_lifecycle_retry_delay_sec)
        )
        record_projection_event_result(
            connection,
            projection_name=LIVE_SIM_LIFECYCLE_PROJECTION,
            event_id=event_id,
            event_type=str(row["event_type"]),
            status="ERROR",
            outcome=status,
            error_message=error_message,
            metadata={"consumer_source": "durable_consumer", "attempts": attempts},
            commit=False,
        )
        connection.execute(
            """
            UPDATE live_sim_lifecycle_inbox
            SET status = ?,
                attempts = ?,
                available_at = ?,
                locked_by = NULL,
                locked_at = NULL,
                last_error = ?,
                updated_at = ?,
                processed_at = CASE WHEN ? = 'DEAD_LETTER' THEN ? ELSE NULL END
            WHERE event_id = ?
            """,
            (
                status,
                attempts,
                available_at,
                error_message,
                now_wire,
                status,
                now_wire,
                event_id,
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return {
        "status": status,
        "event_id": event_id,
        "attempts": attempts,
        "error_message": error_message,
        "retryable": status == "PENDING",
    }


def _claim_next_inbox_row(
    connection: sqlite3.Connection,
    *,
    owner_id: str,
) -> dict[str, Any] | None:
    now = datetime_to_wire(utc_now())
    connection.execute("BEGIN IMMEDIATE")
    try:
        row = connection.execute(
            """
            SELECT *
            FROM live_sim_lifecycle_inbox
            WHERE event_rowid = (
                SELECT MIN(event_rowid)
                FROM live_sim_lifecycle_inbox
                WHERE status IN ('PENDING', 'PROCESSING', 'DEAD_LETTER')
            )
              AND status = 'PENDING'
              AND julianday(available_at) <= julianday(?)
            LIMIT 1
            """,
            (now,),
        ).fetchone()
        if row is None:
            connection.commit()
            return None
        cursor = connection.execute(
            """
            UPDATE live_sim_lifecycle_inbox
            SET status = 'PROCESSING',
                locked_by = ?,
                locked_at = ?,
                updated_at = ?
            WHERE event_id = ? AND status = 'PENDING'
            """,
            (owner_id, now, now, row["event_id"]),
        )
        if cursor.rowcount <= 0:
            connection.commit()
            return None
        claimed = _inbox_row(connection, str(row["event_id"]))
        connection.commit()
        return None if claimed is None else dict(claimed)
    except Exception:
        connection.rollback()
        raise


def _gateway_event(connection: sqlite3.Connection, event_id: str) -> GatewayEvent | None:
    row = connection.execute(
        """
        SELECT event_id, event_type, source, command_id, idempotency_key, event_ts, payload_json
        FROM gateway_events
        WHERE event_id = ? AND status = 'ACCEPTED'
        """,
        (event_id,),
    ).fetchone()
    if row is None:
        return None
    return GatewayEvent(
        event_id=str(row["event_id"]),
        event_type=str(row["event_type"]),
        source=str(row["source"]),
        command_id=row["command_id"],
        idempotency_key=row["idempotency_key"],
        ts=parse_timestamp(row["event_ts"], "event_ts"),
        payload=_json_object(row["payload_json"]),
    )


def _inbox_row(connection: sqlite3.Connection, event_id: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM live_sim_lifecycle_inbox WHERE event_id = ?",
        (event_id,),
    ).fetchone()


def _earliest_unresolved_status(connection: sqlite3.Connection) -> str | None:
    placeholders = ",".join("?" for _ in _UNRESOLVED_STATUSES)
    row = connection.execute(
        f"""
        SELECT status
        FROM live_sim_lifecycle_inbox
        WHERE status IN ({placeholders})
        ORDER BY event_rowid
        LIMIT 1
        """,
        _UNRESOLVED_STATUSES,
    ).fetchone()
    return None if row is None else str(row["status"])


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    result = dict(row)
    result["result"] = _json_object(result.pop("result_json", "{}"))
    return result


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _json_dumps(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
