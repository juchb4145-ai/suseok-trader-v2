from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_reference_service import market_symbols_payload_has_symbols
from services.runtime.market_reference_projection_reconcile import (
    get_latest_market_reference_projection_reconcile,
)

PROJECTION_NAME_MARKET_REFERENCE = "market_reference"
MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON = (
    "MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_IN_PR13"
)


@dataclass(frozen=True, kw_only=True)
class MarketReferenceAppendOnlyRoutingDecision:
    event_id: str
    event_type: str
    dry_run_enabled: bool
    cutover_enabled: bool
    latest_reconcile_status: str | None
    latest_reconcile_run_id: str | None
    latest_reconcile_created_at: str | None
    latest_reconcile_age_sec: float | None
    append_only_ready: bool
    outbox_job_present: bool
    outbox_status: str | None
    would_skip_inline: bool
    effective_skip_inline: bool
    blocked_reason_codes: Sequence[str]
    decided_at: str
    evidence: Mapping[str, Any] = field(default_factory=dict)
    reconcile_required: bool = True
    worker_apply_enabled: bool = False
    membership_count: int = 0
    min_membership_count: int = 0
    global_kill_switch: bool = True
    legacy_effective_skip_guard: bool = True
    skip_budget_limit: int = 0
    skip_budget_used: int = 0
    skip_budget_remaining: int = 0
    outbox_pending_count: int = 0
    outbox_processing_count: int = 0
    outbox_error_count: int = 0
    outbox_dead_letter_count: int = 0
    rollback_required: bool = False
    rollback_reason_codes: Sequence[str] = field(default_factory=tuple)
    controller_status: str = "WARN"
    projection_name: str = PROJECTION_NAME_MARKET_REFERENCE
    no_trading_side_effects: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "projection_name": self.projection_name,
            "dry_run_enabled": self.dry_run_enabled,
            "cutover_enabled": self.cutover_enabled,
            "reconcile_required": self.reconcile_required,
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_run_id": self.latest_reconcile_run_id,
            "latest_reconcile_created_at": self.latest_reconcile_created_at,
            "latest_reconcile_age_sec": self.latest_reconcile_age_sec,
            "append_only_ready": self.append_only_ready,
            "outbox_job_present": self.outbox_job_present,
            "outbox_status": self.outbox_status,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "worker_apply_enabled": self.worker_apply_enabled,
            "membership_count": self.membership_count,
            "min_membership_count": self.min_membership_count,
            "global_kill_switch": self.global_kill_switch,
            "legacy_effective_skip_guard": self.legacy_effective_skip_guard,
            "skip_budget_limit": self.skip_budget_limit,
            "skip_budget_used": self.skip_budget_used,
            "skip_budget_remaining": self.skip_budget_remaining,
            "outbox_pending_count": self.outbox_pending_count,
            "outbox_processing_count": self.outbox_processing_count,
            "outbox_error_count": self.outbox_error_count,
            "outbox_dead_letter_count": self.outbox_dead_letter_count,
            "rollback_required": self.rollback_required,
            "rollback_reason_codes": list(self.rollback_reason_codes),
            "controller_status": self.controller_status,
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
            "read_only": True,
            "no_trading_side_effects": self.no_trading_side_effects,
        }


def decide_market_reference_append_only_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketReferenceAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    decided_at = datetime_to_wire(utc_now())
    dry_run_enabled = bool(settings.gateway_market_reference_append_only_dry_run_enabled)
    cutover_enabled = bool(settings.gateway_market_reference_append_only_cutover_enabled)
    global_kill_switch = bool(
        settings.gateway_market_reference_append_only_global_kill_switch
    )
    legacy_effective_skip_guard = bool(
        settings.gateway_market_reference_append_only_effective_skip_disabled_in_pr13
    )
    skip_budget_limit = int(
        settings.gateway_market_reference_append_only_max_skip_per_minute
    )
    skip_budget_used = _market_reference_skip_budget_used(connection)
    skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    reconcile_required = bool(
        settings.gateway_market_reference_append_only_require_reconcile_pass
    )
    latest_reconcile = get_latest_market_reference_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    latest_status = _string_or_none(
        latest_run.get("status") if isinstance(latest_run, Mapping) else None
    )
    latest_run_id = _string_or_none(
        latest_run.get("run_id") if isinstance(latest_run, Mapping) else None
    )
    latest_created_at = _string_or_none(
        latest_run.get("created_at") if isinstance(latest_run, Mapping) else None
    )
    latest_age_sec = _age_seconds(latest_created_at) if latest_created_at else None
    append_only_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    membership_count = _membership_count(connection)
    min_membership_count = int(
        settings.gateway_market_reference_append_only_min_membership_count
    )
    outbox_job = _market_reference_outbox_job(connection, event.event_id)
    outbox_job_present = outbox_job is not None
    normalized_outbox_status = _normalize_outbox_status(
        outbox_status or (outbox_job["status"] if outbox_job is not None else None)
    )
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_reference_apply_enabled
    )
    source_event_status = _gateway_event_status(connection, event.event_id)
    outbox_counts = _market_reference_outbox_counts(connection)
    effective_skip_health = _market_reference_effective_skip_health(connection)
    rollback_reason_codes = _market_reference_rollback_reason_codes(
        outbox_counts=outbox_counts,
        effective_skip_health=effective_skip_health,
        settings=settings,
    )

    reason_codes: list[str] = []
    would_skip_inline = False
    if event_type != "market_symbols":
        reason_codes.append("NOT_MARKET_SYMBOLS_EVENT")
    elif not dry_run_enabled:
        reason_codes.append("DRY_RUN_DISABLED")
    elif not _outbox_is_ready(
        outbox_status=normalized_outbox_status,
        outbox_job_present=outbox_job_present,
    ):
        reason_codes.append("MARKET_REFERENCE_OUTBOX_JOB_MISSING_OR_NOT_READY")
    elif reconcile_required and latest_run is None:
        reason_codes.append("MARKET_REFERENCE_RECONCILE_MISSING")
    elif reconcile_required and (latest_status != "PASS" or not append_only_ready):
        reason_codes.append("MARKET_REFERENCE_RECONCILE_NOT_PASS")
    elif reconcile_required and latest_age_sec is None:
        reason_codes.append("MARKET_REFERENCE_RECONCILE_FRESHNESS_UNKNOWN")
    elif (
        reconcile_required
        and latest_age_sec
        > settings.gateway_market_reference_append_only_reconcile_max_age_sec
    ):
        reason_codes.append("MARKET_REFERENCE_RECONCILE_STALE")
    elif source_event_status != "ACCEPTED":
        reason_codes.append("SOURCE_GATEWAY_EVENT_NOT_ACCEPTED")
    elif not market_symbols_payload_has_symbols(event.payload):
        reason_codes.append("MARKET_REFERENCE_NO_SYMBOLS")
    elif membership_count < min_membership_count:
        reason_codes.append("MARKET_REFERENCE_MEMBERSHIP_COUNT_BELOW_MIN")
    else:
        would_skip_inline = True
        reason_codes.append("DRY_RUN_WOULD_SKIP_INLINE")

    effective_skip_inline = False
    if would_skip_inline:
        if not cutover_enabled:
            reason_codes.append("MARKET_REFERENCE_CUTOVER_DISABLED")
        elif global_kill_switch:
            reason_codes.append("MARKET_REFERENCE_GLOBAL_KILL_SWITCH")
        elif legacy_effective_skip_guard:
            reason_codes.append(MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON)
        elif not worker_apply_enabled:
            reason_codes.append("MARKET_REFERENCE_WORKER_APPLY_NOT_ENABLED")
        elif latest_status != "PASS" or not append_only_ready:
            reason_codes.append("MARKET_REFERENCE_RECONCILE_NOT_PASS")
        elif latest_age_sec is None:
            reason_codes.append("MARKET_REFERENCE_RECONCILE_FRESHNESS_UNKNOWN")
        elif (
            latest_age_sec
            > settings.gateway_market_reference_append_only_reconcile_max_age_sec
        ):
            reason_codes.append("MARKET_REFERENCE_RECONCILE_STALE")
        elif rollback_reason_codes:
            reason_codes.append("MARKET_REFERENCE_INLINE_ROLLBACK_REQUIRED")
            reason_codes.extend(rollback_reason_codes)
        elif skip_budget_limit <= 0:
            reason_codes.append("MARKET_REFERENCE_SKIP_BUDGET_EXHAUSTED")
        else:
            reserved_budget_used = _reserve_market_reference_skip_budget(
                connection,
                event_id=event.event_id,
                limit=skip_budget_limit,
            )
            if reserved_budget_used is None:
                reason_codes.append("MARKET_REFERENCE_SKIP_BUDGET_EXHAUSTED")
                skip_budget_used = _market_reference_skip_budget_used(connection)
            else:
                effective_skip_inline = True
                skip_budget_used = reserved_budget_used
                reason_codes.append("MARKET_REFERENCE_LIMITED_CUTOVER_EFFECTIVE_SKIP")
            skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    elif cutover_enabled and legacy_effective_skip_guard:
        reason_codes.append(MARKET_REFERENCE_EFFECTIVE_SKIP_DISABLED_REASON)

    controller_status = _market_reference_controller_status(
        dry_run_enabled=dry_run_enabled,
        cutover_enabled=cutover_enabled,
        global_kill_switch=global_kill_switch,
        legacy_effective_skip_guard=legacy_effective_skip_guard,
        skip_budget_limit=skip_budget_limit,
        worker_apply_enabled=worker_apply_enabled,
        latest_status=latest_status,
        append_only_ready=append_only_ready,
        latest_age_sec=latest_age_sec,
        membership_count=membership_count,
        min_membership_count=min_membership_count,
        rollback_reason_codes=rollback_reason_codes,
        settings=settings,
    )

    evidence = {
        "pr": "PR-14",
        "cutover_status": "LIMITED_CUTOVER" if effective_skip_inline else "INLINE_FALLBACK",
        "inline_projection_remains_enabled": not effective_skip_inline,
        "effective_skip_disabled_in_pr13": legacy_effective_skip_guard,
        "fallback_inline_projection_expected": not effective_skip_inline,
        "outbox_enqueue_status": _normalize_outbox_status(outbox_status),
        "outbox_job_status": None if outbox_job is None else outbox_job["status"],
        "outbox_counts": outbox_counts,
        "effective_skip_health": effective_skip_health,
        "source_event_status": source_event_status,
        "latest_reconcile": (
            None
            if not isinstance(latest_run, Mapping)
            else {
                "run_id": latest_run.get("run_id"),
                "status": latest_run.get("status"),
                "append_only_ready": bool(latest_run.get("append_only_ready")),
                "created_at": latest_run.get("created_at"),
            }
        ),
        "worker_apply_enabled": worker_apply_enabled,
        "membership_count": membership_count,
        "min_membership_count": min_membership_count,
        "global_kill_switch": global_kill_switch,
        "skip_budget_limit": skip_budget_limit,
        "skip_budget_used": skip_budget_used,
        "skip_budget_remaining": skip_budget_remaining,
        "rollback_required": bool(rollback_reason_codes),
        "rollback_reason_codes": list(rollback_reason_codes),
        "controller_status": controller_status,
        "market_data_controller_unaffected": True,
        "live_real_order_behavior_unchanged": True,
        "no_trading_side_effects": True,
    }
    decision = MarketReferenceAppendOnlyRoutingDecision(
        event_id=event.event_id,
        event_type=event_type,
        dry_run_enabled=dry_run_enabled,
        cutover_enabled=cutover_enabled,
        reconcile_required=reconcile_required,
        latest_reconcile_status=latest_status,
        latest_reconcile_run_id=latest_run_id,
        latest_reconcile_created_at=latest_created_at,
        latest_reconcile_age_sec=latest_age_sec,
        append_only_ready=append_only_ready,
        outbox_job_present=outbox_job_present,
        outbox_status=normalized_outbox_status,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=effective_skip_inline,
        worker_apply_enabled=worker_apply_enabled,
        membership_count=membership_count,
        min_membership_count=min_membership_count,
        global_kill_switch=global_kill_switch,
        legacy_effective_skip_guard=legacy_effective_skip_guard,
        skip_budget_limit=skip_budget_limit,
        skip_budget_used=skip_budget_used,
        skip_budget_remaining=skip_budget_remaining,
        outbox_pending_count=outbox_counts["pending_count"],
        outbox_processing_count=outbox_counts["processing_count"],
        outbox_error_count=outbox_counts["error_count"],
        outbox_dead_letter_count=outbox_counts["dead_letter_count"],
        rollback_required=bool(rollback_reason_codes),
        rollback_reason_codes=tuple(rollback_reason_codes),
        controller_status=controller_status,
        blocked_reason_codes=tuple(dict.fromkeys(reason_codes)),
        evidence=evidence,
        decided_at=decided_at,
    )
    _persist_market_reference_routing_decision(connection, decision)
    return decision


def get_latest_market_reference_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    rows = list_market_reference_append_only_routing_decisions(connection, limit=500)
    latest_reconcile = get_latest_market_reference_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    latest_status = _string_or_none(
        latest_run.get("status") if isinstance(latest_run, Mapping) else None
    )
    latest_created_at = _string_or_none(
        latest_run.get("created_at") if isinstance(latest_run, Mapping) else None
    )
    latest_age_sec = _age_seconds(latest_created_at) if latest_created_at else None
    append_only_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    effective_skip_count = _count_decisions(connection, "effective_skip_inline")
    would_skip_count = _count_decisions(connection, "would_skip_inline")
    outbox_counts = _market_reference_outbox_counts(connection)
    effective_skip_health = _market_reference_effective_skip_health(connection)
    rollback_reason_codes = _market_reference_rollback_reason_codes(
        outbox_counts=outbox_counts,
        effective_skip_health=effective_skip_health,
        settings=resolved_settings,
    )
    membership_count = _membership_count(connection)
    min_membership_count = int(
        resolved_settings.gateway_market_reference_append_only_min_membership_count
    )
    skip_budget_limit = int(
        resolved_settings.gateway_market_reference_append_only_max_skip_per_minute
    )
    skip_budget_used = _market_reference_skip_budget_used(connection)
    skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    controller_status = _market_reference_controller_status(
        dry_run_enabled=bool(
            resolved_settings.gateway_market_reference_append_only_dry_run_enabled
        ),
        cutover_enabled=bool(
            resolved_settings.gateway_market_reference_append_only_cutover_enabled
        ),
        global_kill_switch=bool(
            resolved_settings.gateway_market_reference_append_only_global_kill_switch
        ),
        legacy_effective_skip_guard=bool(
            resolved_settings.gateway_market_reference_append_only_effective_skip_disabled_in_pr13
        ),
        skip_budget_limit=skip_budget_limit,
        worker_apply_enabled=bool(
            resolved_settings.projection_outbox_apply_projection_enabled
            and resolved_settings.projection_outbox_market_reference_apply_enabled
        ),
        latest_status=latest_status,
        append_only_ready=append_only_ready,
        latest_age_sec=latest_age_sec,
        membership_count=membership_count,
        min_membership_count=min_membership_count,
        rollback_reason_codes=rollback_reason_codes,
        settings=resolved_settings,
    )
    failures = list(rollback_reason_codes)
    warnings = [
        "PR-14 market_reference limited cutover is feature-flagged",
        "market_data controller unaffected",
        "LIVE_REAL/order behavior unchanged",
        "rollback: enable global kill switch or disable market_reference cutover",
    ]
    if isinstance(latest_run, Mapping) and latest_run.get("status") == "FAIL":
        failures.append("MARKET_REFERENCE_RECONCILE_FAIL")
    if not bool(resolved_settings.gateway_market_reference_append_only_dry_run_enabled):
        warnings.append("market_reference append-only dry-run disabled")
    if not bool(resolved_settings.gateway_market_reference_append_only_cutover_enabled):
        warnings.append("market_reference limited cutover disabled")
    if bool(resolved_settings.gateway_market_reference_append_only_global_kill_switch):
        warnings.append("market_reference global kill switch enabled")
    if bool(
        resolved_settings.gateway_market_reference_append_only_effective_skip_disabled_in_pr13
    ):
        warnings.append("legacy PR-13 effective-skip guard enabled")
    if skip_budget_limit <= 0:
        warnings.append("market_reference skip budget disabled")
    blocked_reason_counts = _blocked_reason_code_counts(rows)
    return {
        "pr": "PR-14",
        "status": controller_status,
        "dry_run_enabled": bool(
            resolved_settings.gateway_market_reference_append_only_dry_run_enabled
        ),
        "cutover_enabled": bool(
            resolved_settings.gateway_market_reference_append_only_cutover_enabled
        ),
        "global_kill_switch": bool(
            resolved_settings.gateway_market_reference_append_only_global_kill_switch
        ),
        "effective_skip_disabled_in_pr13": bool(
            resolved_settings.gateway_market_reference_append_only_effective_skip_disabled_in_pr13
        ),
        "skip_budget_limit": skip_budget_limit,
        "skip_budget_used_current_minute": skip_budget_used,
        "skip_budget_remaining_current_minute": skip_budget_remaining,
        "reconcile_required": bool(
            resolved_settings.gateway_market_reference_append_only_require_reconcile_pass
        ),
        "reconcile_max_age_sec": int(
            resolved_settings.gateway_market_reference_append_only_reconcile_max_age_sec
        ),
        "total_decision_count": _count_all_decisions(connection),
        "would_skip_inline_count": would_skip_count,
        "effective_skip_inline_count": effective_skip_count,
        "blocked_reason_code_counts": blocked_reason_counts,
        "latest_decision": rows[0] if rows else None,
        "latest_reconcile": latest_reconcile,
        "latest_reconcile_status": (
            latest_run.get("status") if isinstance(latest_run, Mapping) else None
        ),
        "latest_reconcile_age_sec": latest_age_sec,
        "append_only_ready": append_only_ready,
        "worker_apply_enabled": bool(
            resolved_settings.projection_outbox_apply_projection_enabled
            and resolved_settings.projection_outbox_market_reference_apply_enabled
        ),
        "membership_count": membership_count,
        "min_membership_count": min_membership_count,
        "membership_count_ready": membership_count >= min_membership_count,
        "outbox": outbox_counts,
        "effective_skip_health": effective_skip_health,
        "rollback_required": bool(rollback_reason_codes),
        "rollback_reason_codes": list(rollback_reason_codes),
        "rollback_hint": (
            "set GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH=true "
            "or GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED=false"
        ),
        "effective_skip_allowed_now": bool(
            controller_status == "PASS" and skip_budget_remaining > 0
        ),
        "failures": sorted(set(failures)),
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_reference_append_only_routing_decisions(
    connection: sqlite3.Connection,
    limit: int = 100,
) -> list[dict[str, Any]]:
    bounded_limit = min(max(int(limit), 1), 500)
    rows = connection.execute(
        """
        SELECT *
        FROM market_reference_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT ?
        """,
        (bounded_limit,),
    ).fetchall()
    return [_routing_row_to_dict(row) for row in rows]


def build_market_reference_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    latest_event = connection.execute(
        """
        SELECT event_id, event_ts, received_at
        FROM gateway_events
        WHERE status = 'ACCEPTED' AND event_type = 'market_symbols'
        ORDER BY rowid DESC
        LIMIT 1
        """
    ).fetchone()
    latest_reconcile = get_latest_market_reference_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    routing_status = get_latest_market_reference_append_only_routing_status(
        connection,
        settings=resolved_settings,
    )
    outbox_counts = _market_reference_outbox_counts(connection)
    latest_outbox_job = (
        None
        if latest_event is None
        else _market_reference_outbox_job(connection, str(latest_event["event_id"]))
    )
    latest_outbox_payload = _market_reference_outbox_job_payload(latest_outbox_job)
    membership_count = _membership_count(connection)
    missing_membership_count = int(
        latest_run.get("missing_membership_count") or 0
        if isinstance(latest_run, Mapping)
        else 0
    )
    return {
        "pr": "PR-14",
        "membership_count": membership_count,
        "latest_market_symbols_event_id": (
            None if latest_event is None else latest_event["event_id"]
        ),
        "latest_market_symbols_event_ts": (
            None if latest_event is None else latest_event["event_ts"]
        ),
        "latest_reconcile_status": (
            latest_run.get("status") if isinstance(latest_run, Mapping) else None
        ),
        "latest_reconcile_run_id": (
            latest_run.get("run_id") if isinstance(latest_run, Mapping) else None
        ),
        "append_only_ready": bool(
            latest_run.get("append_only_ready")
            if isinstance(latest_run, Mapping)
            else False
        ),
        "missing_membership_count": missing_membership_count,
        "outbox": outbox_counts,
        "latest_outbox_job": latest_outbox_payload,
        "append_only_routing": routing_status,
        "append_only_dry_run_would_skip_count": int(
            routing_status.get("would_skip_inline_count") or 0
        ),
        "effective_skip_inline_count": int(
            routing_status.get("effective_skip_inline_count") or 0
        ),
        "warnings": [
            "PR-14 market_reference limited cutover is feature-flagged",
            "market_data controller unaffected",
            "LIVE_REAL/order behavior unchanged",
            "rollback: enable global kill switch or disable market_reference cutover",
        ],
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _persist_market_reference_routing_decision(
    connection: sqlite3.Connection,
    decision: MarketReferenceAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_reference_projection_routing_decisions (
            event_id,
            event_type,
            projection_name,
            dry_run_enabled,
            cutover_enabled,
            reconcile_required,
            latest_reconcile_run_id,
            latest_reconcile_status,
            latest_reconcile_created_at,
            latest_reconcile_age_sec,
            append_only_ready,
            outbox_status,
            outbox_job_present,
            would_skip_inline,
            effective_skip_inline,
            worker_apply_enabled,
            membership_count,
            min_membership_count,
            blocked_reason_codes_json,
            evidence_json,
            decided_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, projection_name) DO UPDATE SET
            event_type = excluded.event_type,
            dry_run_enabled = excluded.dry_run_enabled,
            cutover_enabled = excluded.cutover_enabled,
            reconcile_required = excluded.reconcile_required,
            latest_reconcile_run_id = excluded.latest_reconcile_run_id,
            latest_reconcile_status = excluded.latest_reconcile_status,
            latest_reconcile_created_at = excluded.latest_reconcile_created_at,
            latest_reconcile_age_sec = excluded.latest_reconcile_age_sec,
            append_only_ready = excluded.append_only_ready,
            outbox_status = excluded.outbox_status,
            outbox_job_present = excluded.outbox_job_present,
            would_skip_inline = excluded.would_skip_inline,
            effective_skip_inline = excluded.effective_skip_inline,
            worker_apply_enabled = excluded.worker_apply_enabled,
            membership_count = excluded.membership_count,
            min_membership_count = excluded.min_membership_count,
            blocked_reason_codes_json = excluded.blocked_reason_codes_json,
            evidence_json = excluded.evidence_json,
            decided_at = excluded.decided_at
        """,
        (
            decision.event_id,
            decision.event_type,
            decision.projection_name,
            int(decision.dry_run_enabled),
            int(decision.cutover_enabled),
            int(decision.reconcile_required),
            decision.latest_reconcile_run_id,
            decision.latest_reconcile_status,
            decision.latest_reconcile_created_at,
            decision.latest_reconcile_age_sec,
            int(decision.append_only_ready),
            decision.outbox_status,
            int(decision.outbox_job_present),
            int(decision.would_skip_inline),
            int(decision.effective_skip_inline),
            int(decision.worker_apply_enabled),
            decision.membership_count,
            decision.min_membership_count,
            json.dumps(
                list(decision.blocked_reason_codes),
                ensure_ascii=False,
                sort_keys=True,
            ),
            canonical_json(decision.evidence),
            decision.decided_at,
        ),
    )
    connection.commit()


def _market_reference_outbox_job(
    connection: sqlite3.Connection,
    event_id: str,
) -> Mapping[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, updated_at, created_at, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_reference' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _market_reference_outbox_job_payload(
    row: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    payload["metadata"] = _json_object(payload.pop("metadata_json", "{}"))
    return payload


def _market_reference_outbox_counts(connection: sqlite3.Connection) -> dict[str, int]:
    counts = {
        "job_count": 0,
        "pending_count": 0,
        "processing_count": 0,
        "applied_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "dead_letter_count": 0,
    }
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM projection_outbox
        WHERE projection_name = 'market_reference'
        GROUP BY status
        """
    ).fetchall()
    for row in rows:
        status = str(row["status"]).lower()
        count = int(row["count"])
        counts["job_count"] += count
        key = f"{status}_count"
        if key in counts:
            counts[key] = count
    return counts


def _market_reference_effective_skip_health(
    connection: sqlite3.Connection,
) -> dict[str, int]:
    row = connection.execute(
        """
        WITH latest_market_symbols AS (
            SELECT event_id
            FROM gateway_events
            WHERE status = 'ACCEPTED' AND event_type = 'market_symbols'
            ORDER BY rowid DESC
            LIMIT 1
        )
        SELECT
            COUNT(*) AS effective_skip_count,
            SUM(
                CASE
                    WHEN o.outbox_id IS NULL OR o.status != 'APPLIED' THEN 1
                    ELSE 0
                END
            ) AS pending_worker_count,
            SUM(
                CASE
                    WHEN o.status IN ('ERROR', 'DEAD_LETTER') THEN 1
                    ELSE 0
                END
            ) AS worker_error_count,
            SUM(
                CASE
                    WHEN o.status = 'APPLIED'
                        AND COALESCE(
                            json_extract(
                                o.metadata_json,
                                '$.last_worker_evidence.apply_result'
                            ),
                            ''
                        ) != 'APPLIED_BY_WORKER'
                    THEN 1
                    ELSE 0
                END
            ) AS worker_apply_evidence_missing_count,
            SUM(
                CASE
                    WHEN d.event_id = (
                        SELECT event_id FROM latest_market_symbols
                    ) AND NOT EXISTS (
                        SELECT 1
                        FROM market_symbol_memberships AS m
                        WHERE m.event_id = d.event_id
                    ) THEN 1
                    ELSE 0
                END
            ) AS artifact_missing_count
        FROM market_reference_projection_routing_decisions AS d
        LEFT JOIN projection_outbox AS o
            ON o.event_id = d.event_id
            AND o.projection_name = 'market_reference'
        WHERE d.effective_skip_inline = 1
        """
    ).fetchone()
    return {
        "effective_skip_count": int(row["effective_skip_count"] or 0),
        "pending_worker_count": int(row["pending_worker_count"] or 0),
        "worker_error_count": int(row["worker_error_count"] or 0),
        "worker_apply_evidence_missing_count": int(
            row["worker_apply_evidence_missing_count"] or 0
        ),
        "artifact_missing_count": int(row["artifact_missing_count"] or 0),
    }


def _market_reference_rollback_reason_codes(
    *,
    outbox_counts: Mapping[str, int],
    effective_skip_health: Mapping[str, int],
    settings: Settings,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if int(outbox_counts.get("error_count") or 0) > 0:
        reasons.append("MARKET_REFERENCE_OUTBOX_ERROR")
    if int(outbox_counts.get("dead_letter_count") or 0) > 0:
        reasons.append("MARKET_REFERENCE_OUTBOX_DEAD_LETTER")
    if int(outbox_counts.get("processing_count") or 0) > 0:
        reasons.append("MARKET_REFERENCE_OUTBOX_PROCESSING_ACTIVE")
    if int(outbox_counts.get("pending_count") or 0) > int(
        settings.gateway_market_reference_append_only_max_pending_within_sla
    ):
        reasons.append("MARKET_REFERENCE_OUTBOX_PENDING_EXCEEDED")
    if int(effective_skip_health.get("pending_worker_count") or 0) > 0:
        reasons.append("MARKET_REFERENCE_EFFECTIVE_SKIP_PENDING_WORKER")
    if int(effective_skip_health.get("worker_error_count") or 0) > 0:
        reasons.append("MARKET_REFERENCE_EFFECTIVE_SKIP_WORKER_ERROR")
    if int(
        effective_skip_health.get("worker_apply_evidence_missing_count") or 0
    ) > 0:
        reasons.append("MARKET_REFERENCE_EFFECTIVE_SKIP_WORKER_EVIDENCE_MISSING")
    if int(effective_skip_health.get("artifact_missing_count") or 0) > 0:
        reasons.append("MARKET_REFERENCE_EFFECTIVE_SKIP_ARTIFACT_MISSING")
    return tuple(sorted(set(reasons)))


def _market_reference_controller_status(
    *,
    dry_run_enabled: bool,
    cutover_enabled: bool,
    global_kill_switch: bool,
    legacy_effective_skip_guard: bool,
    skip_budget_limit: int,
    worker_apply_enabled: bool,
    latest_status: str | None,
    append_only_ready: bool,
    latest_age_sec: float | None,
    membership_count: int,
    min_membership_count: int,
    rollback_reason_codes: Sequence[str],
    settings: Settings,
) -> str:
    if rollback_reason_codes:
        return "FAIL"
    gates_ready = bool(
        dry_run_enabled
        and cutover_enabled
        and not global_kill_switch
        and not legacy_effective_skip_guard
        and skip_budget_limit > 0
        and worker_apply_enabled
        and latest_status == "PASS"
        and append_only_ready
        and latest_age_sec is not None
        and latest_age_sec
        <= settings.gateway_market_reference_append_only_reconcile_max_age_sec
        and membership_count >= min_membership_count
    )
    if gates_ready:
        return "PASS"
    active_cutover_requested = bool(
        cutover_enabled and not global_kill_switch and not legacy_effective_skip_guard
    )
    return "FAIL" if active_cutover_requested else "WARN"


def _market_reference_skip_budget_used(connection: sqlite3.Connection) -> int:
    minute_bucket = _market_reference_budget_minute_bucket()
    row = connection.execute(
        """
        SELECT used_count
        FROM market_reference_append_only_budget_state
        WHERE budget_name = 'market_reference_global'
            AND minute_bucket = ?
        """,
        (minute_bucket,),
    ).fetchone()
    return 0 if row is None else int(row["used_count"])


def _reserve_market_reference_skip_budget(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    limit: int,
) -> int | None:
    if limit <= 0:
        return None
    minute_bucket = _market_reference_budget_minute_bucket()
    cursor = connection.execute(
        """
        INSERT INTO market_reference_append_only_budget_state (
            budget_name,
            minute_bucket,
            used_count,
            last_event_id,
            updated_at
        )
        VALUES ('market_reference_global', ?, 1, ?, ?)
        ON CONFLICT(budget_name) DO UPDATE SET
            minute_bucket = excluded.minute_bucket,
            used_count = CASE
                WHEN market_reference_append_only_budget_state.minute_bucket
                    = excluded.minute_bucket
                THEN market_reference_append_only_budget_state.used_count + 1
                ELSE 1
            END,
            last_event_id = excluded.last_event_id,
            updated_at = excluded.updated_at
        WHERE market_reference_append_only_budget_state.minute_bucket
                != excluded.minute_bucket
            OR market_reference_append_only_budget_state.used_count < ?
        """,
        (minute_bucket, event_id, datetime_to_wire(utc_now()), limit),
    )
    connection.commit()
    if cursor.rowcount != 1:
        return None
    return _market_reference_skip_budget_used(connection)


def _market_reference_budget_minute_bucket() -> str:
    return datetime_to_wire(utc_now().replace(second=0, microsecond=0))


def _outbox_is_ready(
    *,
    outbox_status: str | None,
    outbox_job_present: bool,
) -> bool:
    if not outbox_job_present:
        return False
    normalized = _normalize_outbox_status(outbox_status)
    return normalized in {
        None,
        "ENQUEUED",
        "DUPLICATE",
        "PENDING",
        "PROCESSING",
        "APPLIED",
        "SKIPPED",
    }


def _count_all_decisions(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_reference_projection_routing_decisions"
    ).fetchone()
    return 0 if row is None else int(row["count"])


def _count_decisions(connection: sqlite3.Connection, column_name: str) -> int:
    if column_name not in {"would_skip_inline", "effective_skip_inline"}:
        raise ValueError(f"unsupported routing decision count column: {column_name}")
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM market_reference_projection_routing_decisions
        WHERE {column_name} = 1
        """
    ).fetchone()
    return 0 if row is None else int(row["count"])


def _blocked_reason_code_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        for code in row.get("blocked_reason_codes") or []:
            counter[str(code)] += 1
    return dict(sorted(counter.items()))


def _membership_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_symbol_memberships"
    ).fetchone()
    return 0 if row is None else int(row["count"])


def _gateway_event_status(
    connection: sqlite3.Connection,
    event_id: str,
) -> str | None:
    row = connection.execute(
        """
        SELECT status
        FROM gateway_events
        WHERE event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else _string_or_none(row["status"])


def _routing_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key in (
        "dry_run_enabled",
        "cutover_enabled",
        "reconcile_required",
        "append_only_ready",
        "outbox_job_present",
        "would_skip_inline",
        "effective_skip_inline",
        "worker_apply_enabled",
    ):
        payload[key] = bool(payload[key])
    payload["blocked_reason_codes"] = _json_array(
        payload.pop("blocked_reason_codes_json", "[]")
    )
    payload["evidence"] = _json_object(payload.pop("evidence_json", "{}"))
    payload["read_only"] = True
    payload["no_trading_side_effects"] = True
    return payload


def _normalize_outbox_status(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value).strip().upper()


def _string_or_none(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _age_seconds(value: str) -> float | None:
    try:
        parsed = parse_timestamp(value, "created_at")
    except Exception:
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value in (None, ""):
        return {}
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _json_array(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    try:
        loaded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    return list(loaded) if isinstance(loaded, list) else []
