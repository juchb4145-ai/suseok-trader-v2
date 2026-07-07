from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, utc_now

from storage.gateway_command_store import canonical_json

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


def get_projection_outbox_status(connection: sqlite3.Connection) -> dict[str, Any]:
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
    return {
        "enabled": True,
        "shadow_mode": True,
        "worker_enabled": False,
        "read_only": True,
        "total_count": total_count,
        **counts,
        "oldest_pending_at": (
            None if oldest_pending is None else oldest_pending["oldest_pending_at"]
        ),
        "latest_error": None if latest_error is None else _row_to_dict(latest_error),
        "by_projection_name": _outbox_counts_by_projection_name(connection),
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
