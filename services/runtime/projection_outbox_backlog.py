from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now

from services.config import Settings, load_settings
from services.runtime.gateway_projection_routing import (
    get_latest_market_data_append_only_routing_status,
)
from services.runtime.market_data_projection_reconcile import (
    get_latest_market_data_projection_reconcile,
)
from services.runtime.projection_outbox_bulk_retire import count_blocking_pending


@dataclass(frozen=True, kw_only=True)
class ProjectionOutboxBacklogBucket:
    key: str
    projection_name: str
    event_type: str
    status: str
    count: int
    oldest_age_sec: float | None
    newest_age_sec: float | None
    max_attempts: int
    sample_event_ids: Sequence[str] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "projection_name": self.projection_name,
            "event_type": self.event_type,
            "status": self.status,
            "count": self.count,
            "oldest_age_sec": self.oldest_age_sec,
            "newest_age_sec": self.newest_age_sec,
            "max_attempts": self.max_attempts,
            "sample_event_ids": list(self.sample_event_ids),
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionOutboxReadinessResult:
    status: str
    pr11_condition_event_cutover_ready: bool
    reason_codes: Sequence[str] = field(default_factory=tuple)
    operator_actions: Sequence[str] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "pr11_condition_event_cutover_ready": (
                self.pr11_condition_event_cutover_ready
            ),
            "reason_codes": list(self.reason_codes),
            "operator_actions": list(self.operator_actions),
        }


@dataclass(frozen=True, kw_only=True)
class ProjectionOutboxBacklogStatus:
    generated_at: str
    total_pending_count: int
    total_processing_count: int
    total_error_count: int
    total_dead_letter_count: int
    stale_processing_count: int
    oldest_pending_at: str | None
    oldest_pending_age_sec: float | None
    newest_pending_at: str | None
    newest_pending_age_sec: float | None
    by_projection_name: Mapping[str, Mapping[str, int]]
    by_event_type: Mapping[str, Mapping[str, int]]
    by_status: Mapping[str, int]
    buckets: Sequence[ProjectionOutboxBacklogBucket]
    recent_pending_count: int
    recent_pending_age_sec_threshold: int
    eligible_pending_count: int
    blocking_pending_count: int
    non_blocking_shadow_pending_count: int
    bulk_retire_eligible_count: int
    condition_event_pending_count: int
    condition_event_recent_pending_count: int
    condition_event_blocking_pending_count: int
    effective_skip_pending_count: int
    live_ingest_detected: bool
    latest_reconcile_status: str | None
    latest_reconcile_run_id: str | None
    condition_event_effective_skip_count: int
    invalid_effective_skip_count: int
    readiness_status: str
    pr11_condition_event_cutover_ready: bool
    reason_codes: Sequence[str] = field(default_factory=tuple)
    operator_actions: Sequence[str] = field(default_factory=tuple)
    no_trading_side_effects: bool = True
    read_only: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "total_pending_count": self.total_pending_count,
            "pending_count": self.total_pending_count,
            "total_processing_count": self.total_processing_count,
            "processing_count": self.total_processing_count,
            "total_error_count": self.total_error_count,
            "error_count": self.total_error_count,
            "total_dead_letter_count": self.total_dead_letter_count,
            "dead_letter_count": self.total_dead_letter_count,
            "stale_processing_count": self.stale_processing_count,
            "oldest_pending_at": self.oldest_pending_at,
            "oldest_pending_age_sec": self.oldest_pending_age_sec,
            "newest_pending_at": self.newest_pending_at,
            "newest_pending_age_sec": self.newest_pending_age_sec,
            "by_projection_name": {
                key: dict(value) for key, value in self.by_projection_name.items()
            },
            "by_event_type": {
                key: dict(value) for key, value in self.by_event_type.items()
            },
            "by_status": dict(self.by_status),
            "buckets": [bucket.to_dict() for bucket in self.buckets],
            "recent_pending_count": self.recent_pending_count,
            "recent_pending_age_sec_threshold": self.recent_pending_age_sec_threshold,
            "eligible_pending_count": self.eligible_pending_count,
            "blocking_pending_count": self.blocking_pending_count,
            "non_blocking_shadow_pending_count": (
                self.non_blocking_shadow_pending_count
            ),
            "bulk_retire_eligible_count": self.bulk_retire_eligible_count,
            "condition_event_pending_count": self.condition_event_pending_count,
            "condition_event_recent_pending_count": (
                self.condition_event_recent_pending_count
            ),
            "condition_event_blocking_pending_count": (
                self.condition_event_blocking_pending_count
            ),
            "effective_skip_pending_count": self.effective_skip_pending_count,
            "live_ingest_detected": self.live_ingest_detected,
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_run_id": self.latest_reconcile_run_id,
            "condition_event_effective_skip_count": (
                self.condition_event_effective_skip_count
            ),
            "invalid_effective_skip_count": self.invalid_effective_skip_count,
            "readiness_status": self.readiness_status,
            "pr11_condition_event_cutover_ready": (
                self.pr11_condition_event_cutover_ready
            ),
            "reason_codes": list(self.reason_codes),
            "operator_actions": list(self.operator_actions),
            "no_trading_side_effects": self.no_trading_side_effects,
            "read_only": self.read_only,
        }


def build_projection_outbox_backlog_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    latest_reconcile: Mapping[str, Any] | None = None,
    routing_status: Mapping[str, Any] | None = None,
    sample_limit: int = 3,
) -> ProjectionOutboxBacklogStatus:
    resolved_settings = settings or load_settings()
    now = utc_now()
    generated_at = datetime_to_wire(now)
    recent_window_sec = int(
        resolved_settings.projection_outbox_backlog_recent_window_sec
    )
    recent_cutoff = datetime_to_wire(now - timedelta(seconds=recent_window_sec))
    stale_cutoff = datetime_to_wire(
        now
        - timedelta(
            seconds=int(
                resolved_settings.projection_outbox_backlog_stale_processing_sec
            )
        )
    )
    status_counts = _status_counts(connection)
    oldest_newest = _pending_bounds(connection)
    by_projection_name = _grouped_status_counts(connection, "projection_name")
    by_event_type = _grouped_status_counts(connection, "event_type")
    recent_pending_count = _count_pending_since(connection, recent_cutoff)
    eligible_pending_count = _count_eligible_pending(connection, generated_at)
    stale_processing_count = _count_stale_processing(connection, stale_cutoff)
    blocking_counts = count_blocking_pending(
        connection,
        older_than_sec=60,
        exclude_recent_condition_events=True,
        limit=20000,
    )
    condition_event_pending_count = _count_condition_event_pending(connection)
    condition_event_recent_pending_count = _count_condition_event_pending(
        connection,
        recent_cutoff=recent_cutoff,
    )
    live_ingest_detected = _live_ingest_detected(connection, recent_cutoff)
    buckets = _backlog_buckets(
        connection,
        now_wire=generated_at,
        sample_limit=sample_limit,
    )
    latest_reconcile_payload = (
        dict(latest_reconcile)
        if latest_reconcile is not None
        else get_latest_market_data_projection_reconcile(connection)
    )
    routing_payload = (
        dict(routing_status)
        if routing_status is not None
        else get_latest_market_data_append_only_routing_status(
            connection,
            settings=resolved_settings,
        )
    )
    latest_run = _latest_run(latest_reconcile_payload)
    latest_reconcile_status = _string_or_none(
        latest_run.get("status") if latest_run is not None else None
    )
    latest_reconcile_run_id = _string_or_none(
        latest_run.get("run_id") if latest_run is not None else None
    )
    condition_event_effective_skip_count = int(
        routing_payload.get("condition_event_effective_skip_count") or 0
    )
    invalid_effective_skip_count = int(
        routing_payload.get("invalid_effective_skip_count") or 0
    )
    readiness = evaluate_projection_outbox_readiness(
        total_pending_count=status_counts["PENDING"],
        blocking_pending_count=int(blocking_counts["blocking_pending_count"]),
        total_error_count=status_counts["ERROR"],
        total_dead_letter_count=status_counts["DEAD_LETTER"],
        stale_processing_count=stale_processing_count,
        recent_pending_count=recent_pending_count,
        condition_event_pending_count=condition_event_pending_count,
        condition_event_recent_pending_count=condition_event_recent_pending_count,
        condition_event_blocking_pending_count=int(
            blocking_counts["condition_event_blocking_pending_count"]
        ),
        effective_skip_pending_count=int(blocking_counts["effective_skip_pending_count"]),
        live_ingest_detected=live_ingest_detected,
        latest_reconcile_status=latest_reconcile_status,
        condition_event_effective_skip_count=condition_event_effective_skip_count,
        invalid_effective_skip_count=invalid_effective_skip_count,
        settings=resolved_settings,
    )
    oldest_pending_at = oldest_newest.get("oldest_pending_at")
    newest_pending_at = oldest_newest.get("newest_pending_at")
    return ProjectionOutboxBacklogStatus(
        generated_at=generated_at,
        total_pending_count=status_counts["PENDING"],
        total_processing_count=status_counts["PROCESSING"],
        total_error_count=status_counts["ERROR"],
        total_dead_letter_count=status_counts["DEAD_LETTER"],
        stale_processing_count=stale_processing_count,
        oldest_pending_at=oldest_pending_at,
        oldest_pending_age_sec=_age_seconds(oldest_pending_at, now_wire=generated_at),
        newest_pending_at=newest_pending_at,
        newest_pending_age_sec=_age_seconds(newest_pending_at, now_wire=generated_at),
        by_projection_name=by_projection_name,
        by_event_type=by_event_type,
        by_status=status_counts,
        buckets=tuple(buckets),
        recent_pending_count=recent_pending_count,
        recent_pending_age_sec_threshold=recent_window_sec,
        eligible_pending_count=eligible_pending_count,
        blocking_pending_count=int(blocking_counts["blocking_pending_count"]),
        non_blocking_shadow_pending_count=int(
            blocking_counts["non_blocking_shadow_pending_count"]
        ),
        bulk_retire_eligible_count=int(blocking_counts["bulk_retire_eligible_count"]),
        condition_event_pending_count=condition_event_pending_count,
        condition_event_recent_pending_count=condition_event_recent_pending_count,
        condition_event_blocking_pending_count=int(
            blocking_counts["condition_event_blocking_pending_count"]
        ),
        effective_skip_pending_count=int(blocking_counts["effective_skip_pending_count"]),
        live_ingest_detected=live_ingest_detected,
        latest_reconcile_status=latest_reconcile_status,
        latest_reconcile_run_id=latest_reconcile_run_id,
        condition_event_effective_skip_count=condition_event_effective_skip_count,
        invalid_effective_skip_count=invalid_effective_skip_count,
        readiness_status=readiness.status,
        pr11_condition_event_cutover_ready=(
            readiness.pr11_condition_event_cutover_ready
        ),
        reason_codes=tuple(readiness.reason_codes),
        operator_actions=tuple(readiness.operator_actions),
    )


def evaluate_projection_outbox_readiness(
    *,
    total_pending_count: int,
    blocking_pending_count: int,
    total_error_count: int,
    total_dead_letter_count: int,
    stale_processing_count: int,
    recent_pending_count: int,
    condition_event_pending_count: int,
    condition_event_recent_pending_count: int,
    condition_event_blocking_pending_count: int,
    effective_skip_pending_count: int,
    live_ingest_detected: bool,
    latest_reconcile_status: str | None,
    condition_event_effective_skip_count: int,
    invalid_effective_skip_count: int,
    settings: Settings,
) -> ProjectionOutboxReadinessResult:
    fail_reasons: list[str] = []
    warn_reasons: list[str] = []
    actions: list[str] = []
    if total_error_count > 0:
        fail_reasons.append("PROJECTION_OUTBOX_ERROR")
        actions.append("CHECK_PROJECTION_OUTBOX_ERRORS")
    if total_dead_letter_count > 0:
        fail_reasons.append("PROJECTION_OUTBOX_DEAD_LETTER")
        actions.append("CHECK_PROJECTION_OUTBOX_DEAD_LETTERS")
    if stale_processing_count > 0:
        fail_reasons.append("STALE_OUTBOX_PROCESSING")
        actions.append("RESET_OR_DRAIN_STALE_PROCESSING")
    if recent_pending_count > int(
        settings.projection_outbox_backlog_recent_fail_count
    ):
        fail_reasons.append("RECENT_OUTBOX_BACKLOG")
        actions.append("RUN_PROJECTION_OUTBOX_DRAIN_ONCE")
    if blocking_pending_count >= int(
        settings.projection_outbox_backlog_fail_pending_count
    ):
        fail_reasons.append("OUTBOX_BLOCKING_PENDING_FAIL_THRESHOLD")
        actions.append("RUN_PROJECTION_OUTBOX_BACKLOG_DRAIN")
    if effective_skip_pending_count > 0:
        fail_reasons.append("EFFECTIVE_SKIP_PENDING_BACKLOG")
        actions.append("RUN_PROJECTION_OUTBOX_DRAIN_ONCE")
    if condition_event_blocking_pending_count > int(
        settings.projection_outbox_backlog_condition_event_ready_max_pending
    ):
        fail_reasons.append("CONDITION_EVENT_BLOCKING_OUTBOX_BACKLOG")
        actions.append("DRAIN_CONDITION_EVENT_OUTBOX_BEFORE_PR11")
    if condition_event_recent_pending_count > int(
        settings.projection_outbox_backlog_condition_event_ready_recent_max_pending
    ):
        fail_reasons.append("CONDITION_EVENT_RECENT_OUTBOX_BACKLOG")
        actions.append("DRAIN_CONDITION_EVENT_OUTBOX_BEFORE_PR11")
    if latest_reconcile_status is None:
        fail_reasons.append("LATEST_RECONCILE_MISSING")
        actions.append("RUN_MARKET_DATA_PROJECTION_RECONCILE")
    elif latest_reconcile_status != "PASS":
        fail_reasons.append("LATEST_RECONCILE_NOT_PASS")
        actions.append("RUN_MARKET_DATA_PROJECTION_RECONCILE")
    if condition_event_effective_skip_count > 0:
        fail_reasons.append("CONDITION_EVENT_EFFECTIVE_SKIP_FORBIDDEN")
        actions.append("KEEP_CONDITION_EVENT_INLINE_ENABLED")
    if invalid_effective_skip_count > 0:
        fail_reasons.append("INVALID_EFFECTIVE_SKIP_EVENT_TYPE")
        actions.append("CHECK_APPEND_ONLY_ROUTING_GUARD")

    if not fail_reasons:
        if blocking_pending_count >= int(
            settings.projection_outbox_backlog_warn_pending_count
        ):
            warn_reasons.append("OUTBOX_BLOCKING_BACKLOG_DRAIN_RECOMMENDED")
            actions.append("RUN_PROJECTION_OUTBOX_BACKLOG_DRAIN")
        elif total_pending_count >= int(
            settings.projection_outbox_backlog_warn_pending_count
        ):
            warn_reasons.append("NON_BLOCKING_SHADOW_BACKLOG_BULK_RETIRE_RECOMMENDED")
            actions.append("RUN_BULK_RETIRE_DRY_RUN")
        if live_ingest_detected and recent_pending_count > 0:
            warn_reasons.append("LIVE_INGEST_PENDING_BACKLOG")
            actions.append("RUN_SMALL_LIVE_SAFE_DRAIN_BATCHES")
        if latest_reconcile_status == "PASS" and total_pending_count > 0:
            warn_reasons.append("LATEST_RECONCILE_PASS_WITH_BACKLOG")

    readiness_status = "FAIL" if fail_reasons else "WARN" if warn_reasons else "PASS"
    pr11_ready = (
        readiness_status in {"PASS", "WARN"}
        and condition_event_blocking_pending_count
        <= int(settings.projection_outbox_backlog_condition_event_ready_max_pending)
        and condition_event_recent_pending_count
        <= int(
            settings.projection_outbox_backlog_condition_event_ready_recent_max_pending
        )
        and effective_skip_pending_count == 0
        and condition_event_effective_skip_count == 0
        and latest_reconcile_status == "PASS"
        and total_error_count == 0
        and total_dead_letter_count == 0
    )
    if (
        bool(settings.projection_outbox_backlog_required_for_condition_event_cutover)
        and not pr11_ready
    ):
        actions.append("BLOCK_PR11_CONDITION_EVENT_CUTOVER")
    return ProjectionOutboxReadinessResult(
        status=readiness_status,
        pr11_condition_event_cutover_ready=pr11_ready,
        reason_codes=tuple(sorted(set(fail_reasons or warn_reasons))),
        operator_actions=tuple(sorted(set(actions))),
    )


def projection_outbox_backlog_summary_fields(
    status: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "backlog_readiness_status": status.get("readiness_status"),
        "pr11_condition_event_cutover_ready": bool(
            status.get("pr11_condition_event_cutover_ready")
        ),
        "blocking_pending_count": int(status.get("blocking_pending_count") or 0),
        "non_blocking_shadow_pending_count": int(
            status.get("non_blocking_shadow_pending_count") or 0
        ),
        "bulk_retire_eligible_count": int(
            status.get("bulk_retire_eligible_count") or 0
        ),
        "recent_pending_count": int(status.get("recent_pending_count") or 0),
        "stale_processing_count": int(status.get("stale_processing_count") or 0),
        "condition_event_pending_count": int(
            status.get("condition_event_pending_count") or 0
        ),
        "condition_event_blocking_pending_count": int(
            status.get("condition_event_blocking_pending_count") or 0
        ),
        "operator_actions": list(status.get("operator_actions") or []),
    }


def _status_counts(connection: sqlite3.Connection) -> dict[str, int]:
    counts = {
        "PENDING": 0,
        "PROCESSING": 0,
        "APPLIED": 0,
        "SKIPPED": 0,
        "ERROR": 0,
        "DEAD_LETTER": 0,
    }
    for row in connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM projection_outbox
        GROUP BY status
        """
    ).fetchall():
        status = str(row["status"]).upper()
        if status in counts:
            counts[status] = int(row["count"])
    return counts


def _pending_bounds(connection: sqlite3.Connection) -> dict[str, str | None]:
    row = connection.execute(
        """
        SELECT
            MIN(COALESCE(available_at, created_at)) AS oldest_pending_at,
            MAX(COALESCE(available_at, created_at)) AS newest_pending_at
        FROM projection_outbox
        WHERE status = 'PENDING'
        """
    ).fetchone()
    return {
        "oldest_pending_at": None if row is None else row["oldest_pending_at"],
        "newest_pending_at": None if row is None else row["newest_pending_at"],
    }


def _grouped_status_counts(
    connection: sqlite3.Connection,
    column_name: str,
) -> dict[str, dict[str, int]]:
    if column_name not in {"projection_name", "event_type"}:
        raise ValueError(f"unsupported grouping column: {column_name}")
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for row in connection.execute(
        f"""
        SELECT {column_name} AS key, status, COUNT(*) AS count
        FROM projection_outbox
        GROUP BY {column_name}, status
        """
    ).fetchall():
        key = str(row["key"] or "UNKNOWN")
        grouped[key][str(row["status"]).lower() + "_count"] = int(row["count"])
        grouped[key]["total_count"] += int(row["count"])
    return {key: dict(counter) for key, counter in grouped.items()}


def _count_pending_since(connection: sqlite3.Connection, cutoff: str) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM projection_outbox
            WHERE status = 'PENDING'
                AND julianday(COALESCE(available_at, created_at)) >= julianday(?)
            """,
            (cutoff,),
        ).fetchone()["count"]
    )


def _count_eligible_pending(connection: sqlite3.Connection, now_wire: str) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM projection_outbox
            WHERE status = 'PENDING'
                AND (
                    available_at IS NULL
                    OR julianday(available_at) <= julianday(?)
                )
            """,
            (now_wire,),
        ).fetchone()["count"]
    )


def _count_stale_processing(connection: sqlite3.Connection, cutoff: str) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM projection_outbox
            WHERE status = 'PROCESSING'
                AND locked_at IS NOT NULL
                AND julianday(locked_at) <= julianday(?)
            """,
            (cutoff,),
        ).fetchone()["count"]
    )


def _count_condition_event_pending(
    connection: sqlite3.Connection,
    *,
    recent_cutoff: str | None = None,
) -> int:
    params: list[Any] = []
    recent_clause = ""
    if recent_cutoff is not None:
        recent_clause = (
            "AND julianday(COALESCE(available_at, created_at)) >= julianday(?)"
        )
        params.append(recent_cutoff)
    return int(
        connection.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM projection_outbox
            WHERE status = 'PENDING'
                AND event_type = 'condition_event'
                {recent_clause}
            """,
            params,
        ).fetchone()["count"]
    )


def _live_ingest_detected(connection: sqlite3.Connection, cutoff: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM gateway_events
        WHERE julianday(received_at) >= julianday(?)
        LIMIT 1
        """,
        (cutoff,),
    ).fetchone()
    return row is not None


def _backlog_buckets(
    connection: sqlite3.Connection,
    *,
    now_wire: str,
    sample_limit: int,
) -> list[ProjectionOutboxBacklogBucket]:
    rows = connection.execute(
        """
        SELECT
            projection_name,
            event_type,
            status,
            COUNT(*) AS count,
            MIN(COALESCE(available_at, created_at)) AS oldest_at,
            MAX(COALESCE(available_at, created_at)) AS newest_at,
            MAX(attempts) AS max_attempts
        FROM projection_outbox
        WHERE status IN ('PENDING', 'PROCESSING', 'ERROR', 'DEAD_LETTER')
        GROUP BY projection_name, event_type, status
        ORDER BY count DESC, projection_name, event_type, status
        LIMIT 50
        """
    ).fetchall()
    buckets: list[ProjectionOutboxBacklogBucket] = []
    bounded_sample_limit = min(max(int(sample_limit), 0), 10)
    for row in rows:
        projection_name = str(row["projection_name"])
        event_type = str(row["event_type"])
        status = str(row["status"])
        sample_event_ids = _sample_event_ids(
            connection,
            projection_name=projection_name,
            event_type=event_type,
            status=status,
            limit=bounded_sample_limit,
        )
        key = f"{projection_name}:{event_type}:{status}"
        buckets.append(
            ProjectionOutboxBacklogBucket(
                key=key,
                projection_name=projection_name,
                event_type=event_type,
                status=status,
                count=int(row["count"]),
                oldest_age_sec=_age_seconds(row["oldest_at"], now_wire=now_wire),
                newest_age_sec=_age_seconds(row["newest_at"], now_wire=now_wire),
                max_attempts=int(row["max_attempts"] or 0),
                sample_event_ids=tuple(sample_event_ids),
            )
        )
    return buckets


def _sample_event_ids(
    connection: sqlite3.Connection,
    *,
    projection_name: str,
    event_type: str,
    status: str,
    limit: int,
) -> list[str]:
    if limit <= 0:
        return []
    return [
        str(row["event_id"])
        for row in connection.execute(
            """
            SELECT event_id
            FROM projection_outbox
            WHERE projection_name = ?
                AND event_type = ?
                AND status = ?
            ORDER BY COALESCE(available_at, created_at), event_id
            LIMIT ?
            """,
            (projection_name, event_type, status, limit),
        ).fetchall()
    ]


def _latest_run(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    latest_run = payload.get("latest_run")
    if isinstance(latest_run, Mapping):
        return latest_run
    if "status" in payload:
        return payload
    return None


def _age_seconds(value: Any, *, now_wire: str) -> float | None:
    if value is None:
        return None
    try:
        now = parse_timestamp(now_wire, "now")
        timestamp = parse_timestamp(value, "timestamp")
    except ValueError:
        return None
    return max((now - timestamp).total_seconds(), 0.0)


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
