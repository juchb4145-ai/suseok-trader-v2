from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, new_message_id, parse_timestamp, utc_now

from services.config import Settings, load_settings
from services.runtime.live_sim_lifecycle_consumer import (
    enqueue_live_sim_lifecycle_event,
    get_live_sim_lifecycle_consumer_status,
    is_live_sim_lifecycle_event,
    process_live_sim_lifecycle_inline,
)


@dataclass(frozen=True, kw_only=True)
class LiveSimLifecycleRoutingDecision:
    decision_id: str
    event_id: str
    event_rowid: int
    event_type: str
    would_defer_inline: bool
    effective_defer_inline: bool
    inline_fallback: bool
    ordered_backlog_blocked: bool
    cutover_enabled: bool
    dry_run_enabled: bool
    global_kill_switch: bool
    consumer_enabled: bool
    worker_enabled: bool
    worker_healthy: bool
    pending_count: int
    processing_count: int
    dead_letter_count: int
    reason_codes: tuple[str, ...] = field(default_factory=tuple)
    evidence: Mapping[str, Any] = field(default_factory=dict)
    created_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "event_id": self.event_id,
            "event_rowid": self.event_rowid,
            "event_type": self.event_type,
            "would_defer_inline": self.would_defer_inline,
            "effective_defer_inline": self.effective_defer_inline,
            "inline_fallback": self.inline_fallback,
            "ordered_backlog_blocked": self.ordered_backlog_blocked,
            "cutover_enabled": self.cutover_enabled,
            "dry_run_enabled": self.dry_run_enabled,
            "global_kill_switch": self.global_kill_switch,
            "consumer_enabled": self.consumer_enabled,
            "worker_enabled": self.worker_enabled,
            "worker_healthy": self.worker_healthy,
            "pending_count": self.pending_count,
            "processing_count": self.processing_count,
            "dead_letter_count": self.dead_letter_count,
            "reason_codes": list(self.reason_codes),
            "evidence": dict(self.evidence),
            "created_at": self.created_at,
            "no_order_commands_created": True,
            "live_real_allowed": False,
        }


def route_live_sim_lifecycle_gateway_event(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    if not is_live_sim_lifecycle_event(event.event_type):
        return {"status": "IGNORED_EVENT_TYPE", "event_id": event.event_id}
    enqueue = enqueue_live_sim_lifecycle_event(connection, event)
    inbox_row = _inbox_row(connection, event.event_id)
    if inbox_row is not None and str(inbox_row["status"]) == "APPLIED":
        result = process_live_sim_lifecycle_inline(
            connection,
            event,
            settings=resolved_settings,
        )
        existing = _routing_decision_for_event(connection, event.event_id)
        if existing is not None:
            result["routing"] = existing.to_dict()
        return result

    decision = decide_live_sim_lifecycle_routing(
        connection,
        event,
        settings=resolved_settings,
    )
    if decision.effective_defer_inline:
        return {
            "status": "DEFERRED_TO_DURABLE_WORKER",
            "event_id": event.event_id,
            "enqueue_status": enqueue["status"],
            "inline_compatibility": False,
            "routing": decision.to_dict(),
        }
    if decision.ordered_backlog_blocked:
        return {
            "status": "BLOCKED_ORDERED_BACKLOG",
            "event_id": event.event_id,
            "enqueue_status": enqueue["status"],
            "inline_compatibility": False,
            "routing": decision.to_dict(),
        }
    if not decision.inline_fallback:
        return {
            "status": "BLOCKED_INLINE_FALLBACK_DISABLED",
            "event_id": event.event_id,
            "enqueue_status": enqueue["status"],
            "inline_compatibility": False,
            "routing": decision.to_dict(),
        }
    result = process_live_sim_lifecycle_inline(
        connection,
        event,
        settings=resolved_settings,
    )
    result["routing"] = decision.to_dict()
    return result


def decide_live_sim_lifecycle_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings | None = None,
) -> LiveSimLifecycleRoutingDecision:
    resolved_settings = settings or load_settings()
    existing = _routing_decision_for_event(connection, event.event_id)
    if existing is not None:
        return existing
    inbox_row = _inbox_row(connection, event.event_id)
    if inbox_row is None:
        raise ValueError(f"lifecycle inbox row missing for routing: {event.event_id}")
    consumer_status = get_live_sim_lifecycle_consumer_status(
        connection,
        settings=resolved_settings,
    )
    latest_run = consumer_status.get("latest_worker_run")
    latest_run = latest_run if isinstance(latest_run, Mapping) else None
    worker_health = _worker_health(latest_run, settings=resolved_settings)
    oldest = consumer_status.get("oldest_unresolved")
    oldest = oldest if isinstance(oldest, Mapping) else None
    current_is_earliest = oldest is None or oldest.get("event_id") == event.event_id
    unresolved_count = sum(
        int(consumer_status.get(name) or 0)
        for name in ("pending_count", "processing_count", "dead_letter_count")
    )
    reasons: list[str] = []
    if not resolved_settings.live_sim_lifecycle_consumer_enabled:
        reasons.append("LIFECYCLE_CONSUMER_DISABLED")
    if not resolved_settings.live_sim_lifecycle_worker_enabled:
        reasons.append("LIFECYCLE_WORKER_DISABLED")
    if (
        resolved_settings.live_sim_lifecycle_require_worker_health
        and not worker_health["healthy"]
    ):
        reasons.append("LIFECYCLE_WORKER_UNHEALTHY")
    if int(consumer_status.get("dead_letter_count") or 0):
        reasons.append("LIFECYCLE_DEAD_LETTER_PRESENT")
    if int(consumer_status.get("stale_processing_count") or 0):
        reasons.append("LIFECYCLE_STALE_PROCESSING")
    if int(consumer_status.get("missing_inbox_count") or 0):
        reasons.append("LIFECYCLE_INBOX_GAP")
    if int(consumer_status.get("applied_without_result_count") or 0):
        reasons.append("LIFECYCLE_APPLIED_RESULT_GAP")
    if unresolved_count > resolved_settings.live_sim_lifecycle_max_unresolved_count:
        reasons.append("LIFECYCLE_BACKLOG_LIMIT_EXCEEDED")
    if str(inbox_row["status"]) not in {"PENDING", "PROCESSING"}:
        reasons.append("LIFECYCLE_CURRENT_INBOX_NOT_PENDING")
    if not current_is_earliest:
        reasons.append("LIFECYCLE_PRIOR_EVENT_UNRESOLVED")

    readiness_reasons = {
        "LIFECYCLE_CONSUMER_DISABLED",
        "LIFECYCLE_WORKER_DISABLED",
        "LIFECYCLE_WORKER_UNHEALTHY",
        "LIFECYCLE_DEAD_LETTER_PRESENT",
        "LIFECYCLE_STALE_PROCESSING",
        "LIFECYCLE_INBOX_GAP",
        "LIFECYCLE_APPLIED_RESULT_GAP",
        "LIFECYCLE_BACKLOG_LIMIT_EXCEEDED",
        "LIFECYCLE_CURRENT_INBOX_NOT_PENDING",
    }
    ready = not any(reason in readiness_reasons for reason in reasons)
    would_defer = ready
    effective_defer = (
        ready
        and resolved_settings.live_sim_lifecycle_cutover_enabled
        and not resolved_settings.live_sim_lifecycle_global_kill_switch
    )
    if not resolved_settings.live_sim_lifecycle_cutover_enabled:
        reasons.append("LIFECYCLE_CUTOVER_DISABLED")
    if resolved_settings.live_sim_lifecycle_global_kill_switch:
        reasons.append("LIFECYCLE_GLOBAL_KILL_SWITCH")
    if effective_defer:
        reasons.append("LIFECYCLE_EFFECTIVE_DEFER_TO_WORKER")
    ordered_backlog_blocked = not effective_defer and not current_is_earliest
    inline_fallback = (
        not effective_defer
        and current_is_earliest
        and resolved_settings.live_sim_lifecycle_inline_fallback_enabled
    )
    if ordered_backlog_blocked:
        reasons.append("LIFECYCLE_OUT_OF_ORDER_INLINE_BLOCKED")
    elif inline_fallback:
        reasons.append("LIFECYCLE_INLINE_FALLBACK")
    elif not effective_defer:
        reasons.append("LIFECYCLE_INLINE_FALLBACK_DISABLED")

    now = datetime_to_wire(utc_now())
    decision = LiveSimLifecycleRoutingDecision(
        decision_id=new_message_id("live_sim_lifecycle_routing"),
        event_id=event.event_id,
        event_rowid=int(inbox_row["event_rowid"]),
        event_type=event.event_type.strip().lower(),
        would_defer_inline=would_defer,
        effective_defer_inline=effective_defer,
        inline_fallback=inline_fallback,
        ordered_backlog_blocked=ordered_backlog_blocked,
        cutover_enabled=resolved_settings.live_sim_lifecycle_cutover_enabled,
        dry_run_enabled=resolved_settings.live_sim_lifecycle_cutover_dry_run_enabled,
        global_kill_switch=resolved_settings.live_sim_lifecycle_global_kill_switch,
        consumer_enabled=resolved_settings.live_sim_lifecycle_consumer_enabled,
        worker_enabled=resolved_settings.live_sim_lifecycle_worker_enabled,
        worker_healthy=bool(worker_health["healthy"]),
        pending_count=int(consumer_status.get("pending_count") or 0),
        processing_count=int(consumer_status.get("processing_count") or 0),
        dead_letter_count=int(consumer_status.get("dead_letter_count") or 0),
        reason_codes=tuple(dict.fromkeys(reasons)),
        evidence={
            "enqueue_status": str(inbox_row["status"]),
            "current_is_earliest_unresolved": current_is_earliest,
            "oldest_unresolved": dict(oldest or {}),
            "unresolved_count": unresolved_count,
            "max_unresolved_count": (
                resolved_settings.live_sim_lifecycle_max_unresolved_count
            ),
            "worker_health": worker_health,
            "missing_inbox_count": int(
                consumer_status.get("missing_inbox_count") or 0
            ),
            "applied_without_result_count": int(
                consumer_status.get("applied_without_result_count") or 0
            ),
            "stale_processing_count": int(
                consumer_status.get("stale_processing_count") or 0
            ),
        },
        created_at=now,
    )
    _persist_routing_decision(connection, decision)
    return decision


def build_live_sim_lifecycle_cutover_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    consumer = get_live_sim_lifecycle_consumer_status(
        connection,
        settings=resolved_settings,
    )
    latest = get_latest_live_sim_lifecycle_routing_status(
        connection,
        settings=resolved_settings,
    )
    effective_count = int(
        connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM live_sim_lifecycle_routing_decisions
            WHERE effective_defer_inline = 1
            """
        ).fetchone()["count"]
    )
    worker_health = _worker_health(
        consumer.get("latest_worker_run")
        if isinstance(consumer.get("latest_worker_run"), Mapping)
        else None,
        settings=resolved_settings,
    )
    cutover_failures = [
        reason
        for reason in consumer.get("reason_codes") or []
        if reason
        in {
            "LIFECYCLE_DEAD_LETTER_PRESENT",
            "LIFECYCLE_INBOX_GAP",
            "LIFECYCLE_APPLIED_RESULT_GAP",
            "LIFECYCLE_STALE_PROCESSING",
        }
    ]
    status = "FAIL" if cutover_failures else "PASS"
    if not (
        resolved_settings.live_sim_lifecycle_cutover_enabled
        and not resolved_settings.live_sim_lifecycle_global_kill_switch
        and resolved_settings.live_sim_lifecycle_consumer_enabled
        and resolved_settings.live_sim_lifecycle_worker_enabled
        and worker_health["healthy"]
    ):
        status = "WARN" if status != "FAIL" else status
    return {
        **consumer,
        "status": status,
        "cutover_status": status,
        "cutover_dry_run_enabled": (
            resolved_settings.live_sim_lifecycle_cutover_dry_run_enabled
        ),
        "cutover_enabled": resolved_settings.live_sim_lifecycle_cutover_enabled,
        "global_kill_switch": resolved_settings.live_sim_lifecycle_global_kill_switch,
        "inline_fallback_enabled": (
            resolved_settings.live_sim_lifecycle_inline_fallback_enabled
        ),
        "worker_health": worker_health,
        "effective_defer_count": effective_count,
        "latest_routing": latest,
        "request_path_removed": False,
        "raw_append_and_durable_enqueue_only": False,
    }


def get_latest_live_sim_lifecycle_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT *
        FROM live_sim_lifecycle_routing_decisions
        ORDER BY created_at DESC, decision_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        resolved_settings = settings or load_settings()
        return {
            "status": "NOT_RUN",
            "cutover_enabled": resolved_settings.live_sim_lifecycle_cutover_enabled,
            "global_kill_switch": (
                resolved_settings.live_sim_lifecycle_global_kill_switch
            ),
            "effective_defer_inline": False,
            "reason_codes": ["LIFECYCLE_ROUTING_NOT_RUN"],
            "no_order_commands_created": True,
            "live_real_allowed": False,
        }
    return _row_to_decision(row).to_dict()


def list_live_sim_lifecycle_routing_decisions(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM live_sim_lifecycle_routing_decisions
        ORDER BY created_at DESC, decision_id DESC
        LIMIT ?
        """,
        (min(max(int(limit), 1), 500),),
    ).fetchall()
    return [_row_to_decision(row).to_dict() for row in rows]


def _worker_health(
    latest_run: Mapping[str, Any] | None,
    *,
    settings: Settings,
) -> dict[str, Any]:
    if latest_run is None:
        return {
            "healthy": False,
            "reason": "WORKER_RUN_MISSING",
            "age_sec": None,
            "latest_status": None,
        }
    completed_at = latest_run.get("completed_at")
    try:
        age_sec = max(
            (utc_now() - parse_timestamp(completed_at, "completed_at")).total_seconds(),
            0.0,
        )
    except (TypeError, ValueError):
        return {
            "healthy": False,
            "reason": "WORKER_RUN_TIMESTAMP_INVALID",
            "age_sec": None,
            "latest_status": latest_run.get("status"),
        }
    latest_status = str(latest_run.get("status") or "")
    healthy = (
        latest_status in {"IDLE", "COMPLETED"}
        and age_sec <= settings.live_sim_lifecycle_worker_health_max_age_sec
    )
    return {
        "healthy": healthy,
        "reason": "PASS" if healthy else "WORKER_RUN_STALE_OR_ERROR",
        "age_sec": round(age_sec, 3),
        "max_age_sec": settings.live_sim_lifecycle_worker_health_max_age_sec,
        "latest_status": latest_status,
        "completed_at": completed_at,
    }


def _persist_routing_decision(
    connection: sqlite3.Connection,
    decision: LiveSimLifecycleRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO live_sim_lifecycle_routing_decisions (
            decision_id,
            event_id,
            event_rowid,
            event_type,
            would_defer_inline,
            effective_defer_inline,
            inline_fallback,
            ordered_backlog_blocked,
            cutover_enabled,
            dry_run_enabled,
            global_kill_switch,
            consumer_enabled,
            worker_enabled,
            worker_healthy,
            pending_count,
            processing_count,
            dead_letter_count,
            reason_codes_json,
            evidence_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            decision.decision_id,
            decision.event_id,
            decision.event_rowid,
            decision.event_type,
            int(decision.would_defer_inline),
            int(decision.effective_defer_inline),
            int(decision.inline_fallback),
            int(decision.ordered_backlog_blocked),
            int(decision.cutover_enabled),
            int(decision.dry_run_enabled),
            int(decision.global_kill_switch),
            int(decision.consumer_enabled),
            int(decision.worker_enabled),
            int(decision.worker_healthy),
            decision.pending_count,
            decision.processing_count,
            decision.dead_letter_count,
            _json_dumps(list(decision.reason_codes)),
            _json_dumps(dict(decision.evidence)),
            decision.created_at,
        ),
    )
    connection.commit()


def _routing_decision_for_event(
    connection: sqlite3.Connection,
    event_id: str,
) -> LiveSimLifecycleRoutingDecision | None:
    row = connection.execute(
        "SELECT * FROM live_sim_lifecycle_routing_decisions WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return None if row is None else _row_to_decision(row)


def _inbox_row(connection: sqlite3.Connection, event_id: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM live_sim_lifecycle_inbox WHERE event_id = ?",
        (event_id,),
    ).fetchone()


def _row_to_decision(row: sqlite3.Row) -> LiveSimLifecycleRoutingDecision:
    return LiveSimLifecycleRoutingDecision(
        decision_id=str(row["decision_id"]),
        event_id=str(row["event_id"]),
        event_rowid=int(row["event_rowid"]),
        event_type=str(row["event_type"]),
        would_defer_inline=bool(row["would_defer_inline"]),
        effective_defer_inline=bool(row["effective_defer_inline"]),
        inline_fallback=bool(row["inline_fallback"]),
        ordered_backlog_blocked=bool(row["ordered_backlog_blocked"]),
        cutover_enabled=bool(row["cutover_enabled"]),
        dry_run_enabled=bool(row["dry_run_enabled"]),
        global_kill_switch=bool(row["global_kill_switch"]),
        consumer_enabled=bool(row["consumer_enabled"]),
        worker_enabled=bool(row["worker_enabled"]),
        worker_healthy=bool(row["worker_healthy"]),
        pending_count=int(row["pending_count"]),
        processing_count=int(row["processing_count"]),
        dead_letter_count=int(row["dead_letter_count"]),
        reason_codes=tuple(_json_list(row["reason_codes_json"])),
        evidence=_json_object(row["evidence_json"]),
        created_at=str(row["created_at"]),
    )


def _json_object(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _json_list(value: Any) -> list[str]:
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError):
        return []
    return [str(item) for item in parsed] if isinstance(parsed, list) else []


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
