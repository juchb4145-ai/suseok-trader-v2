from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now

from storage.gateway_command_store import canonical_json
from storage.projection_watermarks import record_projection_event_result

PROJECTION_OUTBOX_STATUSES: tuple[str, ...] = (
    "PENDING",
    "PROCESSING",
    "APPLIED",
    "SKIPPED",
    "ERROR",
    "DEAD_LETTER",
)
_COUNT_KEYS = {status: f"{status.lower()}_count" for status in PROJECTION_OUTBOX_STATUSES}
_DIRECT_EVENT_PROJECTIONS: dict[str, tuple[str, ...]] = {
    "price_tick": ("market_data",),
    "condition_event": ("market_data", "condition_fusion"),
    "tr_response": ("market_data",),
    "market_symbols": ("market_reference",),
    "market_index_tick": ("market_index", "market_regime"),
}
_EXCLUDED_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "heartbeat",
        "gateway_log",
        "command_started",
        "command_ack",
        "command_failed",
        "rate_limited",
        "order_pre_ack",
        "execution_event",
        "kiwoom_order_chejan",
        "kiwoom_balance_chejan",
        "kiwoom_special_chejan",
    }
)


@dataclass(frozen=True, kw_only=True)
class ProjectionJobSpec:
    projection_name: str
    priority: int = 0
    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True, kw_only=True)
class ProjectionOutboxEnqueueResult:
    status: str
    event_id: str
    event_type: str
    job_count: int
    created_count: int = 0
    duplicate_count: int = 0
    jobs: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "job_count": self.job_count,
            "created_count": self.created_count,
            "duplicate_count": self.duplicate_count,
            "jobs": list(self.jobs),
            "shadow_mode": True,
            "worker_enabled": False,
        }


def projection_jobs_for_gateway_event(event_type: str) -> list[ProjectionJobSpec]:
    normalized = _normalize_event_type(event_type)
    if normalized in _EXCLUDED_EVENT_TYPES:
        return []
    projections = _DIRECT_EVENT_PROJECTIONS.get(normalized)
    if projections is None and _is_scan_event_type(normalized):
        projections = ("market_scan",)
    if projections is None:
        return []
    return [ProjectionJobSpec(projection_name=name) for name in projections]


def enqueue_projection_jobs_for_gateway_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    event_rowid: int | None = None,
) -> ProjectionOutboxEnqueueResult:
    event_type = _normalize_event_type(event.event_type)
    specs = _projection_jobs_for_event(event)
    if not specs:
        return ProjectionOutboxEnqueueResult(
            status="NOOP",
            event_id=event.event_id,
            event_type=event_type,
            job_count=0,
        )

    resolved_event_rowid = (
        event_rowid
        if event_rowid is not None
        else _lookup_gateway_event_rowid(connection, event.event_id)
    )
    now = datetime_to_wire(utc_now())
    jobs: list[dict[str, Any]] = []
    created_count = 0
    duplicate_count = 0
    for spec in specs:
        outbox_id = _outbox_id(spec.projection_name, event.event_id)
        metadata = _job_metadata(event=event, spec=spec)
        cursor = connection.execute(
            """
            INSERT INTO projection_outbox (
                outbox_id,
                projection_name,
                event_id,
                event_type,
                event_rowid,
                source,
                status,
                priority,
                attempts,
                available_at,
                created_at,
                updated_at,
                metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, 'PENDING', ?, 0, ?, ?, ?, ?)
            ON CONFLICT(projection_name, event_id) DO NOTHING
            """,
            (
                outbox_id,
                spec.projection_name,
                event.event_id,
                event_type,
                resolved_event_rowid,
                event.source,
                int(spec.priority),
                now,
                now,
                now,
                canonical_json(metadata),
            ),
        )
        created = cursor.rowcount > 0
        if created:
            created_count += 1
        else:
            duplicate_count += 1
        jobs.append(
            {
                "outbox_id": outbox_id,
                "projection_name": spec.projection_name,
                "event_id": event.event_id,
                "event_type": event_type,
                "status": "PENDING",
                "created": created,
            }
        )
    connection.commit()
    status = "ENQUEUED" if created_count else "DUPLICATE"
    return ProjectionOutboxEnqueueResult(
        status=status,
        event_id=event.event_id,
        event_type=event_type,
        job_count=len(specs),
        created_count=created_count,
        duplicate_count=duplicate_count,
        jobs=tuple(jobs),
    )


def claim_projection_outbox_jobs(
    connection: sqlite3.Connection,
    *,
    owner_id: str,
    limit: int,
    processing_ttl_sec: int,
    min_age_sec: float = 0.0,
    projection_name: str | None = None,
) -> list[dict[str, Any]]:
    del processing_ttl_sec
    normalized_owner_id = _require_non_empty(owner_id, "owner_id")
    bounded_limit = min(max(int(limit), 1), 500)
    now = utc_now()
    now_wire = datetime_to_wire(now)
    min_created_at = datetime_to_wire(now - timedelta(seconds=max(float(min_age_sec), 0.0)))
    normalized_projection_name = (
        None
        if projection_name is None
        else _require_non_empty(projection_name, "projection_name").lower()
    )
    projection_clause = ""
    candidate_params: list[Any] = [now_wire, min_created_at]
    if normalized_projection_name is not None:
        projection_clause = "AND projection_name = ?"
        candidate_params.append(normalized_projection_name)
    candidate_params.append(bounded_limit)
    claimed: list[dict[str, Any]] = []
    try:
        connection.execute("BEGIN IMMEDIATE")
        candidates = connection.execute(
            f"""
            SELECT outbox_id
            FROM projection_outbox
            WHERE status = 'PENDING'
                AND (available_at IS NULL OR julianday(available_at) <= julianday(?))
                AND julianday(created_at) <= julianday(?)
                {projection_clause}
            ORDER BY
                priority DESC,
                event_rowid ASC,
                CASE
                    WHEN projection_name IN (
                        'condition_fusion',
                        'market_regime',
                        'market_scan'
                    )
                    THEN 1
                    ELSE 0
                END ASC,
                created_at ASC,
                outbox_id ASC
            LIMIT ?
            """,
            tuple(candidate_params),
        ).fetchall()
        for candidate in candidates:
            cursor = connection.execute(
                """
                UPDATE projection_outbox
                SET
                    status = 'PROCESSING',
                    locked_by = ?,
                    locked_at = ?,
                    updated_at = ?
                WHERE outbox_id = ?
                    AND status = 'PENDING'
                    AND (available_at IS NULL OR julianday(available_at) <= julianday(?))
                    AND julianday(created_at) <= julianday(?)
                """,
                (
                    normalized_owner_id,
                    now_wire,
                    now_wire,
                    candidate["outbox_id"],
                    now_wire,
                    min_created_at,
                ),
            )
            if cursor.rowcount <= 0:
                continue
            row = connection.execute(
                "SELECT * FROM projection_outbox WHERE outbox_id = ?",
                (candidate["outbox_id"],),
            ).fetchone()
            if row is not None:
                claimed.append(_job_row_to_dict(row))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return claimed


def mark_projection_outbox_applied(
    connection: sqlite3.Connection,
    outbox_id: str,
    *,
    owner_id: str,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    _mark_projection_outbox_terminal(
        connection,
        outbox_id,
        owner_id=owner_id,
        status="APPLIED",
        evidence=evidence,
    )


def mark_projection_outbox_skipped(
    connection: sqlite3.Connection,
    outbox_id: str,
    *,
    owner_id: str,
    reason: str,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    enriched_evidence = dict(evidence or {})
    enriched_evidence["reason"] = _require_non_empty(reason, "reason")
    _mark_projection_outbox_terminal(
        connection,
        outbox_id,
        owner_id=owner_id,
        status="SKIPPED",
        evidence=enriched_evidence,
    )


def mark_projection_outbox_error(
    connection: sqlite3.Connection,
    outbox_id: str,
    *,
    owner_id: str,
    error_message: str,
    retry_limit: int,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    normalized_outbox_id = _require_non_empty(outbox_id, "outbox_id")
    normalized_owner_id = _require_non_empty(owner_id, "owner_id")
    normalized_error = _require_non_empty(error_message, "error_message")
    now = datetime_to_wire(utc_now())
    row = connection.execute(
        """
        SELECT
            attempts,
            metadata_json,
            projection_name,
            event_id,
            event_type,
            event_rowid
        FROM projection_outbox
        WHERE outbox_id = ? AND locked_by = ?
        """,
        (normalized_outbox_id, normalized_owner_id),
    ).fetchone()
    if row is None:
        return
    next_attempts = int(row["attempts"]) + 1
    next_status = "DEAD_LETTER" if next_attempts >= int(retry_limit) else "ERROR"
    metadata_json = _merge_metadata_json(
        row["metadata_json"],
        status=next_status,
        evidence=evidence,
        error_message=normalized_error,
        marked_at=now,
    )
    processed_at = now if next_status == "DEAD_LETTER" else None
    record_projection_event_result(
        connection,
        projection_name=row["projection_name"],
        event_id=row["event_id"],
        event_rowid=row["event_rowid"],
        event_type=row["event_type"],
        status="ERROR",
        outcome=next_status,
        error_message=normalized_error,
        metadata={"source": "projection_outbox", "evidence": dict(evidence or {})},
        commit=False,
    )
    connection.execute(
        """
        UPDATE projection_outbox
        SET
            status = ?,
            attempts = ?,
            last_error = ?,
            locked_by = NULL,
            locked_at = NULL,
            updated_at = ?,
            processed_at = COALESCE(?, processed_at),
            metadata_json = ?
        WHERE outbox_id = ? AND locked_by = ?
        """,
        (
            next_status,
            next_attempts,
            normalized_error,
            now,
            processed_at,
            metadata_json,
            normalized_outbox_id,
            normalized_owner_id,
        ),
    )
    connection.commit()


def mark_projection_outbox_retryable_error(
    connection: sqlite3.Connection,
    outbox_id: str,
    *,
    owner_id: str,
    error_message: str,
    retry_limit: int,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    normalized_outbox_id = _require_non_empty(outbox_id, "outbox_id")
    normalized_owner_id = _require_non_empty(owner_id, "owner_id")
    normalized_error = _require_non_empty(error_message, "error_message")
    now = datetime_to_wire(utc_now())
    row = connection.execute(
        """
        SELECT
            attempts,
            metadata_json,
            projection_name,
            event_id,
            event_type,
            event_rowid
        FROM projection_outbox
        WHERE outbox_id = ? AND locked_by = ?
        """,
        (normalized_outbox_id, normalized_owner_id),
    ).fetchone()
    if row is None:
        return
    next_attempts = int(row["attempts"]) + 1
    next_status = "DEAD_LETTER" if next_attempts >= int(retry_limit) else "PENDING"
    metadata_json = _merge_metadata_json(
        row["metadata_json"],
        status=next_status,
        evidence=evidence,
        error_message=normalized_error,
        marked_at=now,
    )
    record_projection_event_result(
        connection,
        projection_name=row["projection_name"],
        event_id=row["event_id"],
        event_rowid=row["event_rowid"],
        event_type=row["event_type"],
        status="ERROR",
        outcome=("DEAD_LETTER" if next_status == "DEAD_LETTER" else "RETRY_PENDING"),
        error_message=normalized_error,
        metadata={"source": "projection_outbox", "evidence": dict(evidence or {})},
        commit=False,
    )
    connection.execute(
        """
        UPDATE projection_outbox
        SET
            status = ?,
            attempts = ?,
            last_error = ?,
            available_at = ?,
            locked_by = NULL,
            locked_at = NULL,
            updated_at = ?,
            processed_at = CASE WHEN ? = 'DEAD_LETTER' THEN ? ELSE NULL END,
            metadata_json = ?
        WHERE outbox_id = ? AND locked_by = ?
        """,
        (
            next_status,
            next_attempts,
            normalized_error,
            now,
            now,
            next_status,
            now,
            metadata_json,
            normalized_outbox_id,
            normalized_owner_id,
        ),
    )
    connection.commit()


def reset_stale_projection_outbox_processing(
    connection: sqlite3.Connection,
    *,
    stale_sec: int,
) -> int:
    now = utc_now()
    cutoff = datetime_to_wire(now - timedelta(seconds=max(int(stale_sec), 0)))
    now_wire = datetime_to_wire(now)
    cursor = connection.execute(
        """
        UPDATE projection_outbox
        SET
            status = 'PENDING',
            locked_by = NULL,
            locked_at = NULL,
            updated_at = ?
        WHERE status = 'PROCESSING'
            AND locked_at IS NOT NULL
            AND julianday(locked_at) <= julianday(?)
        """,
        (now_wire, cutoff),
    )
    connection.commit()
    return int(cursor.rowcount)


def get_projection_outbox_status(
    connection: sqlite3.Connection,
    settings: Any | None = None,
) -> dict[str, Any]:
    counts = _empty_status_counts()
    for row in connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM projection_outbox
        GROUP BY status
        """
    ).fetchall():
        status = str(row["status"]).upper()
        if status in _COUNT_KEYS:
            counts[_COUNT_KEYS[status]] = int(row["count"])
    total_count = sum(counts.values())
    oldest_pending = connection.execute(
        """
        SELECT MIN(COALESCE(available_at, created_at)) AS oldest_pending_at
        FROM projection_outbox
        WHERE status = 'PENDING'
        """
    ).fetchone()
    latest_error = connection.execute(
        """
        SELECT outbox_id, projection_name, event_id, event_type, status, last_error, updated_at
        FROM projection_outbox
        WHERE status IN ('ERROR', 'DEAD_LETTER') OR last_error IS NOT NULL
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """
    ).fetchone()
    last_apply_mode = connection.execute(
        """
        SELECT json_extract(metadata_json, '$.last_worker_evidence.apply_mode') AS apply_mode
        FROM projection_outbox
        WHERE json_extract(metadata_json, '$.last_worker_evidence.apply_mode') IS NOT NULL
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """
    ).fetchone()
    apply_enabled = bool(
        getattr(settings, "projection_outbox_apply_projection_enabled", False)
    )
    market_data_apply_enabled = bool(
        getattr(settings, "projection_outbox_market_data_apply_enabled", False)
    )
    market_reference_apply_enabled = bool(
        getattr(settings, "projection_outbox_market_reference_apply_enabled", False)
    )
    market_index_apply_enabled = bool(
        getattr(settings, "projection_outbox_market_index_apply_enabled", False)
    )
    processing_ttl_sec = int(getattr(settings, "projection_outbox_processing_ttl_sec", 60))
    stale_cutoff = datetime_to_wire(
        utc_now() - timedelta(seconds=max(processing_ttl_sec, 0))
    )
    stale_processing_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM projection_outbox
            WHERE status = 'PROCESSING'
                AND locked_at IS NOT NULL
                AND julianday(locked_at) <= julianday(?)
            """,
            (stale_cutoff,),
        ).fetchone()["count"]
    )
    return {
        "enabled": True,
        "shadow_mode": bool(getattr(settings, "projection_outbox_shadow_mode", True)),
        "worker_enabled": bool(
            getattr(settings, "projection_outbox_worker_enabled", False)
        ),
        "apply_projection_enabled": bool(
            getattr(settings, "projection_outbox_apply_projection_enabled", False)
        ),
        "market_data_apply_enabled": market_data_apply_enabled,
        "market_reference_apply_enabled": market_reference_apply_enabled,
        "market_index_apply_enabled": market_index_apply_enabled,
        "batch_size": int(getattr(settings, "projection_outbox_batch_size", 100)),
        "apply_batch_size": int(
            getattr(settings, "projection_outbox_apply_batch_size", 50)
        ),
        "retry_limit": int(getattr(settings, "projection_outbox_retry_limit", 3)),
        "processing_ttl_sec": processing_ttl_sec,
        "shadow_min_age_sec": float(
            getattr(settings, "projection_outbox_shadow_min_age_sec", 0.5)
        ),
        "apply_min_age_sec": float(
            getattr(settings, "projection_outbox_apply_min_age_sec", 1.0)
        ),
        "market_reference_apply_batch_size": int(
            getattr(settings, "projection_outbox_market_reference_apply_batch_size", 20)
        ),
        "market_reference_apply_min_age_sec": float(
            getattr(
                settings,
                "projection_outbox_market_reference_apply_min_age_sec",
                1.0,
            )
        ),
        "market_index_apply_batch_size": int(
            getattr(settings, "projection_outbox_market_index_apply_batch_size", 20)
        ),
        "market_index_apply_min_age_sec": float(
            getattr(settings, "projection_outbox_market_index_apply_min_age_sec", 1.0)
        ),
        "read_only": True,
        "projection_side_effects_allowed": apply_enabled
        and (
            market_data_apply_enabled
            or market_reference_apply_enabled
            or market_index_apply_enabled
        ),
        "last_apply_mode": None if last_apply_mode is None else last_apply_mode["apply_mode"],
        "warnings": [
            "market_data apply worker is disabled by default",
            "inline projection remains enabled",
            "append-only gateway mode is not enabled",
        ],
        "total_count": total_count,
        **counts,
        "oldest_pending_at": (
            None if oldest_pending is None else oldest_pending["oldest_pending_at"]
        ),
        "latest_error": None if latest_error is None else _row_to_dict(latest_error),
        "by_projection_name": _outbox_counts_by_projection_name(connection),
        "projection_outbox_processing_stale_count": stale_processing_count,
        "recommended_run_once_limit": int(
            getattr(settings, "projection_outbox_live_run_once_batch_size", 50)
        ),
        "operator_lock_health": "WARN" if stale_processing_count > 0 else "OK",
    }


def list_projection_outbox_jobs(
    connection: sqlite3.Connection,
    status: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    bounded_limit = min(max(int(limit), 1), 500)
    params: list[Any] = []
    status_clause = ""
    if status is not None:
        normalized_status = str(status).strip().upper()
        if normalized_status not in PROJECTION_OUTBOX_STATUSES:
            raise ValueError(f"unsupported projection_outbox status: {status}")
        status_clause = "WHERE status = ?"
        params.append(normalized_status)
    params.append(bounded_limit)
    rows = connection.execute(
        f"""
        SELECT *
        FROM projection_outbox
        {status_clause}
        ORDER BY created_at DESC, outbox_id ASC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    return [_job_row_to_dict(row) for row in rows]


def _empty_status_counts() -> dict[str, int]:
    return {count_key: 0 for count_key in _COUNT_KEYS.values()}


def _mark_projection_outbox_terminal(
    connection: sqlite3.Connection,
    outbox_id: str,
    *,
    owner_id: str,
    status: str,
    evidence: Mapping[str, Any] | None = None,
) -> None:
    normalized_outbox_id = _require_non_empty(outbox_id, "outbox_id")
    normalized_owner_id = _require_non_empty(owner_id, "owner_id")
    now = datetime_to_wire(utc_now())
    row = connection.execute(
        """
        SELECT
            metadata_json,
            projection_name,
            event_id,
            event_type,
            event_rowid
        FROM projection_outbox
        WHERE outbox_id = ? AND locked_by = ?
        """,
        (normalized_outbox_id, normalized_owner_id),
    ).fetchone()
    if row is None:
        return
    metadata_json = _merge_metadata_json(
        row["metadata_json"],
        status=status,
        evidence=evidence,
        marked_at=now,
    )
    if status == "APPLIED":
        record_projection_event_result(
            connection,
            projection_name=row["projection_name"],
            event_id=row["event_id"],
            event_rowid=row["event_rowid"],
            event_type=row["event_type"],
            status="SUCCESS",
            outcome="OUTBOX_APPLIED",
            metadata={
                "source": "projection_outbox",
                "evidence": dict(evidence or {}),
            },
            commit=False,
        )
    connection.execute(
        """
        UPDATE projection_outbox
        SET
            status = ?,
            locked_by = NULL,
            locked_at = NULL,
            updated_at = ?,
            processed_at = ?,
            last_error = NULL,
            metadata_json = ?
        WHERE outbox_id = ? AND locked_by = ?
        """,
        (
            status,
            now,
            now,
            metadata_json,
            normalized_outbox_id,
            normalized_owner_id,
        ),
    )
    connection.commit()


def _outbox_counts_by_projection_name(
    connection: sqlite3.Connection,
) -> dict[str, dict[str, Any]]:
    by_projection: dict[str, dict[str, Any]] = {}
    rows = connection.execute(
        """
        SELECT projection_name, status, COUNT(*) AS count
        FROM projection_outbox
        GROUP BY projection_name, status
        ORDER BY projection_name ASC, status ASC
        """
    ).fetchall()
    for row in rows:
        projection_name = str(row["projection_name"])
        bucket = by_projection.setdefault(
            projection_name,
            {"total_count": 0, **_empty_status_counts()},
        )
        count = int(row["count"])
        status = str(row["status"]).upper()
        bucket["total_count"] += count
        if status in _COUNT_KEYS:
            bucket[_COUNT_KEYS[status]] = count
    return by_projection


def _normalize_event_type(event_type: str) -> str:
    return str(event_type or "").strip().lower()


def _is_scan_event_type(event_type: str) -> bool:
    return event_type == "market_scan" or event_type.startswith("market_scan_")


def _projection_jobs_for_event(event: GatewayEvent) -> list[ProjectionJobSpec]:
    specs = projection_jobs_for_gateway_event(event.event_type)
    if _is_scan_related_tr_response(event):
        specs = _append_projection_job(specs, "market_scan")
    return specs


def _append_projection_job(
    specs: list[ProjectionJobSpec],
    projection_name: str,
) -> list[ProjectionJobSpec]:
    if any(spec.projection_name == projection_name for spec in specs):
        return specs
    return [*specs, ProjectionJobSpec(projection_name=projection_name)]


def _is_scan_related_tr_response(event: GatewayEvent) -> bool:
    if _normalize_event_type(event.event_type) != "tr_response":
        return False
    payload = event.payload
    request_id = str(payload.get("request_id") or "").strip().lower()
    request_name = str(payload.get("request_name") or "").strip().lower()
    if request_id.startswith("market_scan:") or request_name.startswith("market_scan_"):
        return True
    metadata = payload.get("metadata")
    return isinstance(metadata, Mapping) and str(metadata.get("source") or "") == "market_scan"


def _lookup_gateway_event_rowid(
    connection: sqlite3.Connection,
    event_id: str,
) -> int | None:
    row = connection.execute(
        "SELECT rowid AS event_rowid FROM gateway_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return None if row is None else int(row["event_rowid"])


def _outbox_id(projection_name: str, event_id: str) -> str:
    return f"{projection_name}:{event_id}"


def _job_metadata(
    *,
    event: GatewayEvent,
    spec: ProjectionJobSpec,
) -> dict[str, Any]:
    metadata = dict(spec.metadata or {})
    metadata.update(
        {
            "shadow_mode": True,
            "worker_enabled": False,
            "source_event_id": event.event_id,
            "source_event_type": _normalize_event_type(event.event_type),
            "source_command_id": event.command_id,
            "source_idempotency_key": event.idempotency_key,
        }
    )
    return metadata


def _job_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["metadata"] = _loads_json_object(data.pop("metadata_json"))
    return data


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _loads_json_object(value: str) -> dict[str, Any]:
    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _merge_metadata_json(
    metadata_json: str,
    *,
    status: str,
    evidence: Mapping[str, Any] | None,
    marked_at: str,
    error_message: str | None = None,
) -> str:
    metadata = _loads_json_object(metadata_json)
    history = metadata.get("worker_history")
    if not isinstance(history, list):
        history = []
    entry: dict[str, Any] = {
        "status": status,
        "marked_at": marked_at,
        "evidence": dict(evidence or {}),
    }
    if error_message is not None:
        entry["error_message"] = error_message
    history.append(entry)
    metadata["worker_history"] = history[-20:]
    metadata["last_worker_status"] = status
    metadata["last_worker_evidence"] = dict(evidence or {})
    if error_message is not None:
        metadata["last_worker_error"] = error_message
    return canonical_json(metadata)


def _require_non_empty(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} must not be empty")
    return text
