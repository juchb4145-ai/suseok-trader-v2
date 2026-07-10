from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, TradingMode, TradingProfile
from services.market_scan_service import inspect_market_scan_event
from services.runtime.market_scan_projection_reconcile import (
    get_latest_market_scan_projection_reconcile,
)

PROJECTION_NAME_MARKET_SCAN = "market_scan"
MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_REASON = (
    "MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_IN_PR20"
)


@dataclass(frozen=True, kw_only=True)
class MarketScanAppendOnlyRoutingDecision:
    event_id: str
    event_type: str
    dry_run_enabled: bool
    cutover_enabled: bool
    global_kill_switch: bool
    reconcile_required: bool
    latest_reconcile_run_id: str | None
    latest_reconcile_status: str | None
    latest_reconcile_created_at: str | None
    latest_reconcile_age_sec: float | None
    append_only_ready: bool
    outbox_status: str | None
    outbox_job_present: bool
    parser_verified: bool
    data_usable: bool
    market_data_dependency_ready: bool
    worker_apply_enabled: bool
    observe_safe: bool
    event_age_sec: float | None
    event_future_skew_sec: float | None
    skip_budget_limit: int
    skip_budget_used: int
    skip_budget_remaining: int
    rollback_required: bool
    controller_status: str
    would_skip_inline: bool
    effective_skip_inline: bool
    effective_skip_disabled_in_pr20: bool
    blocked_reason_codes: Sequence[str] = field(default_factory=tuple)
    evidence: Mapping[str, Any] = field(default_factory=dict)
    decided_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "projection_name": PROJECTION_NAME_MARKET_SCAN,
            "dry_run_enabled": self.dry_run_enabled,
            "cutover_enabled": self.cutover_enabled,
            "global_kill_switch": self.global_kill_switch,
            "reconcile_required": self.reconcile_required,
            "latest_reconcile_run_id": self.latest_reconcile_run_id,
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_created_at": self.latest_reconcile_created_at,
            "latest_reconcile_age_sec": self.latest_reconcile_age_sec,
            "append_only_ready": self.append_only_ready,
            "outbox_status": self.outbox_status,
            "outbox_job_present": self.outbox_job_present,
            "parser_verified": self.parser_verified,
            "data_usable": self.data_usable,
            "market_data_dependency_ready": self.market_data_dependency_ready,
            "worker_apply_enabled": self.worker_apply_enabled,
            "observe_safe": self.observe_safe,
            "event_age_sec": self.event_age_sec,
            "event_future_skew_sec": self.event_future_skew_sec,
            "skip_budget_limit": self.skip_budget_limit,
            "skip_budget_used": self.skip_budget_used,
            "skip_budget_remaining": self.skip_budget_remaining,
            "rollback_required": self.rollback_required,
            "controller_status": self.controller_status,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "effective_skip_disabled_in_pr20": (
                self.effective_skip_disabled_in_pr20
            ),
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
        }


def decide_market_scan_append_only_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketScanAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    decided_at = datetime_to_wire(utc_now())
    readiness = inspect_market_scan_event(event, settings=settings)
    latest = get_latest_market_scan_projection_reconcile(connection)
    latest_run = _mapping(latest.get("latest_run"))
    latest_status = _optional_text(latest_run.get("status"))
    latest_created_at = _optional_text(latest_run.get("created_at"))
    latest_age_sec = _age_seconds(latest_created_at)
    append_only_ready = bool(latest_run.get("append_only_ready"))
    current_outbox = _outbox_job(connection, event.event_id)
    normalized_outbox_status = _optional_text(
        (current_outbox or {}).get("status") if current_outbox else outbox_status
    )
    outbox_counts = _outbox_counts(connection)
    observe_safe = bool(
        settings.trading_profile is TradingProfile.OBSERVE
        and settings.trading_mode is TradingMode.OBSERVE
        and not settings.trading_allow_live_sim
        and not settings.trading_allow_live_real
    )
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_scan_apply_enabled
    )
    dependency = _market_data_dependency_status(
        connection,
        event_id=event.event_id,
        expected_rows=readiness.row_count,
        settings=settings,
    )
    prior_event_id = _prior_scan_event_id(connection, event.event_id)
    reconcile_covers_prior_event = bool(
        prior_event_id
        and latest_run.get("latest_event_id") == prior_event_id
        and latest_status == "PASS"
        and append_only_ready
    )
    event_age_sec, event_future_skew_sec = _timestamp_age_and_future_skew(event.ts)
    cutover_enabled = bool(settings.gateway_market_scan_append_only_cutover_enabled)
    global_kill_switch = bool(
        settings.gateway_market_scan_append_only_global_kill_switch
    )
    legacy_effective_skip_guard = bool(
        settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20
    )
    skip_budget_limit = int(
        settings.gateway_market_scan_append_only_max_skip_per_minute
    )
    skip_budget_used = _skip_budget_used(connection)
    skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    effective_skip_health = _effective_skip_health(
        connection,
        exclude_event_id=event.event_id,
    )
    rollback_reason_codes = _rollback_reason_codes(
        outbox_counts=outbox_counts,
        effective_skip_health=effective_skip_health,
        settings=settings,
    )

    reasons: list[str] = []
    if not settings.gateway_market_scan_append_only_dry_run_enabled:
        reasons.append("MARKET_SCAN_DRY_RUN_DISABLED")
    if event_type != "tr_response" or not readiness.recognized:
        reasons.append("MARKET_SCAN_EVENT_NOT_ELIGIBLE")
    if not settings.market_scan_enabled:
        reasons.append("MARKET_SCAN_SERVICE_DISABLED")
    if not observe_safe:
        reasons.append("MARKET_SCAN_OBSERVE_SAFE_REQUIRED")
    if not worker_apply_enabled:
        reasons.append("MARKET_SCAN_WORKER_APPLY_DISABLED")
    if not settings.gateway_market_scan_append_only_require_reconcile_pass:
        reasons.append("MARKET_SCAN_RECONCILE_GUARD_DISABLED")
    if not settings.gateway_market_scan_append_only_require_prior_event_reconcile:
        reasons.append("MARKET_SCAN_PRIOR_RECONCILE_GUARD_DISABLED")
    if latest_status != "PASS" or not append_only_ready:
        reasons.append("MARKET_SCAN_RECONCILE_NOT_READY")
    if latest_age_sec is None or (
        latest_age_sec > settings.gateway_market_scan_append_only_reconcile_max_age_sec
    ):
        reasons.append("MARKET_SCAN_RECONCILE_STALE")
    if not reconcile_covers_prior_event:
        reasons.append("MARKET_SCAN_PRIOR_EVENT_NOT_RECONCILED")
    if not settings.gateway_market_scan_append_only_require_parser_verified:
        reasons.append("MARKET_SCAN_PARSER_GUARD_DISABLED")
    if not readiness.parser_verified:
        reasons.append("MARKET_SCAN_PARSER_UNVERIFIED")
    if not readiness.data_usable:
        reasons.append("MARKET_SCAN_DATA_UNUSABLE")
    if event_age_sec is None or (
        event_age_sec > settings.gateway_market_scan_append_only_max_event_age_sec
    ):
        reasons.append("MARKET_SCAN_EVENT_STALE")
    if event_future_skew_sec is None or (
        event_future_skew_sec
        > settings.gateway_market_scan_append_only_max_future_skew_sec
    ):
        reasons.append("MARKET_SCAN_EVENT_FUTURE_SKEW")
    if not settings.gateway_market_scan_append_only_require_market_data_dependency:
        reasons.append("MARKET_SCAN_MARKET_DATA_DEPENDENCY_GUARD_DISABLED")
    if not dependency["ready"]:
        reasons.append("MARKET_SCAN_MARKET_DATA_DEPENDENCY_NOT_READY")
    if not settings.gateway_market_scan_append_only_require_worker_closure:
        reasons.append("MARKET_SCAN_WORKER_CLOSURE_GUARD_DISABLED")
    if not settings.gateway_market_scan_append_only_fail_closed_on_worker_error:
        reasons.append("MARKET_SCAN_WORKER_ERROR_FAIL_CLOSED_DISABLED")
    if current_outbox is None:
        reasons.append("MARKET_SCAN_OUTBOX_JOB_MISSING")
    elif str(current_outbox["status"]).upper() not in {"PENDING", "PROCESSING"}:
        reasons.append("MARKET_SCAN_OUTBOX_NOT_ENQUEUED")
    if outbox_counts["error_count"] > 0:
        reasons.append("MARKET_SCAN_OUTBOX_ERROR_PRESENT")
    if outbox_counts["dead_letter_count"] > 0:
        reasons.append("MARKET_SCAN_OUTBOX_DEAD_LETTER_PRESENT")
    if (
        outbox_counts["pending_count"] + outbox_counts["processing_count"]
        > settings.gateway_market_scan_append_only_max_pending_within_sla
    ):
        reasons.append("MARKET_SCAN_OUTBOX_BACKLOG_NOT_READY")
    if rollback_reason_codes:
        reasons.append("MARKET_SCAN_INLINE_ROLLBACK_REQUIRED")
        reasons.extend(rollback_reason_codes)

    would_skip_inline = not reasons
    blocked_reason_codes = list(reasons)
    effective_skip_inline = False
    if would_skip_inline:
        blocked_reason_codes.append("DRY_RUN_WOULD_SKIP_INLINE")
        if not cutover_enabled:
            blocked_reason_codes.append("MARKET_SCAN_CUTOVER_DISABLED")
        if global_kill_switch:
            blocked_reason_codes.append("MARKET_SCAN_GLOBAL_KILL_SWITCH")
        if legacy_effective_skip_guard:
            blocked_reason_codes.append(MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_REASON)
        if skip_budget_limit <= 0:
            blocked_reason_codes.append("MARKET_SCAN_SKIP_BUDGET_EXHAUSTED")
        if (
            cutover_enabled
            and not global_kill_switch
            and not legacy_effective_skip_guard
            and skip_budget_limit > 0
        ):
            reserved_budget_used = _reserve_skip_budget(
                connection,
                event_id=event.event_id,
                limit=skip_budget_limit,
            )
            if reserved_budget_used is None:
                blocked_reason_codes.append("MARKET_SCAN_SKIP_BUDGET_EXHAUSTED")
                skip_budget_used = _skip_budget_used(connection)
            else:
                effective_skip_inline = True
                skip_budget_used = reserved_budget_used
                blocked_reason_codes.append(
                    "MARKET_SCAN_LIMITED_CUTOVER_EFFECTIVE_SKIP"
                )
            skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    elif legacy_effective_skip_guard:
        blocked_reason_codes.append(MARKET_SCAN_EFFECTIVE_SKIP_DISABLED_REASON)
    controller_status = _controller_status(
        settings=settings,
        observe_safe=observe_safe,
        worker_apply_enabled=worker_apply_enabled,
        latest_status=latest_status,
        append_only_ready=append_only_ready,
        latest_age_sec=latest_age_sec,
        reconcile_covers_prior_event=reconcile_covers_prior_event,
        event_age_sec=event_age_sec,
        event_future_skew_sec=event_future_skew_sec,
        rollback_reason_codes=rollback_reason_codes,
    )
    decision = MarketScanAppendOnlyRoutingDecision(
        event_id=event.event_id,
        event_type=event_type,
        dry_run_enabled=settings.gateway_market_scan_append_only_dry_run_enabled,
        cutover_enabled=cutover_enabled,
        global_kill_switch=global_kill_switch,
        reconcile_required=(
            settings.gateway_market_scan_append_only_require_reconcile_pass
        ),
        latest_reconcile_run_id=_optional_text(latest_run.get("run_id")),
        latest_reconcile_status=latest_status,
        latest_reconcile_created_at=latest_created_at,
        latest_reconcile_age_sec=latest_age_sec,
        append_only_ready=append_only_ready,
        outbox_status=normalized_outbox_status,
        outbox_job_present=current_outbox is not None,
        parser_verified=readiness.parser_verified,
        data_usable=readiness.data_usable,
        market_data_dependency_ready=bool(dependency["ready"]),
        worker_apply_enabled=worker_apply_enabled,
        observe_safe=observe_safe,
        event_age_sec=event_age_sec,
        event_future_skew_sec=event_future_skew_sec,
        skip_budget_limit=skip_budget_limit,
        skip_budget_used=skip_budget_used,
        skip_budget_remaining=skip_budget_remaining,
        rollback_required=bool(rollback_reason_codes),
        controller_status=controller_status,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=effective_skip_inline,
        effective_skip_disabled_in_pr20=(
            settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20
        ),
        blocked_reason_codes=tuple(dict.fromkeys(blocked_reason_codes)),
        evidence={
            "pr": "PR-21",
            "readiness": readiness.to_dict(),
            "market_data_dependency": dependency,
            "prior_event_id": prior_event_id,
            "reconcile_covers_prior_event": reconcile_covers_prior_event,
            "outbox_counts": outbox_counts,
            "effective_skip_health": effective_skip_health,
            "rollback_reason_codes": list(rollback_reason_codes),
            "event_age_sec": event_age_sec,
            "event_future_skew_sec": event_future_skew_sec,
            "skip_budget_limit": skip_budget_limit,
            "skip_budget_used": skip_budget_used,
            "skip_budget_remaining": skip_budget_remaining,
            "inline_market_scan_path_retained": not effective_skip_inline,
            "nxt_may_validate_only_explicit_venue_neutral_scan_inputs": True,
            "no_order_side_effects": True,
            "no_trading_side_effects": True,
        },
        decided_at=decided_at,
    )
    _persist_decision(connection, decision)
    return decision


def get_latest_market_scan_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, Any]:
    decisions = list_market_scan_append_only_routing_decisions(connection, limit=100)
    latest = decisions[0] if decisions else None
    reconcile = get_latest_market_scan_projection_reconcile(connection)
    outbox = _outbox_counts(connection)
    effective_skip_health = _effective_skip_health(connection)
    rollback_reason_codes = _rollback_reason_codes(
        outbox_counts=outbox,
        effective_skip_health=effective_skip_health,
        settings=settings,
    )
    skip_budget_limit = int(
        settings.gateway_market_scan_append_only_max_skip_per_minute
    )
    skip_budget_used = _skip_budget_used(connection)
    controller_status = str((latest or {}).get("controller_status") or "WARN").upper()
    if rollback_reason_codes:
        controller_status = "FAIL"
    return {
        "status": controller_status,
        "controller_status": controller_status,
        "dry_run_enabled": settings.gateway_market_scan_append_only_dry_run_enabled,
        "cutover_enabled": settings.gateway_market_scan_append_only_cutover_enabled,
        "global_kill_switch": (
            settings.gateway_market_scan_append_only_global_kill_switch
        ),
        "effective_skip_disabled_in_pr20": (
            settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20
        ),
        "worker_apply_enabled": bool(
            settings.projection_outbox_apply_projection_enabled
            and settings.projection_outbox_market_scan_apply_enabled
        ),
        "skip_budget_limit": skip_budget_limit,
        "skip_budget_used": skip_budget_used,
        "skip_budget_remaining": max(skip_budget_limit - skip_budget_used, 0),
        "decision_count": _count_decisions(connection),
        "would_skip_inline_count": _count_decisions(
            connection, field_name="would_skip_inline"
        ),
        "effective_skip_inline_count": _count_decisions(
            connection, field_name="effective_skip_inline"
        ),
        "latest_decision": latest,
        "latest_reconcile": reconcile,
        "outbox": outbox,
        "effective_skip_health": effective_skip_health,
        "rollback_required": bool(rollback_reason_codes),
        "rollback_reason_codes": list(rollback_reason_codes),
        "rollback_hint": (
            "set GATEWAY_MARKET_SCAN_APPEND_ONLY_GLOBAL_KILL_SWITCH=true or "
            "GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED=false"
        ),
        "inline_market_scan_path_retained": True,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_scan_append_only_routing_decisions(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT * FROM market_scan_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC LIMIT ?
        """,
        (min(max(int(limit), 1), 500),),
    ).fetchall()
    return [_decision_row(row) for row in rows]


def _persist_decision(
    connection: sqlite3.Connection,
    decision: MarketScanAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_scan_projection_routing_decisions (
            event_id, event_type, projection_name, dry_run_enabled,
            cutover_enabled, global_kill_switch, reconcile_required,
            latest_reconcile_run_id, latest_reconcile_status,
            latest_reconcile_created_at, latest_reconcile_age_sec, append_only_ready,
            outbox_status, outbox_job_present, parser_verified, data_usable,
            market_data_dependency_ready, worker_apply_enabled, observe_safe,
            event_age_sec, event_future_skew_sec, skip_budget_limit,
            skip_budget_used, skip_budget_remaining, rollback_required,
            controller_status,
            would_skip_inline, effective_skip_inline,
            effective_skip_disabled_in_pr20, blocked_reason_codes_json,
            evidence_json, decided_at
        ) VALUES (
            ?, ?, 'market_scan', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(event_id, projection_name) DO UPDATE SET
            dry_run_enabled = excluded.dry_run_enabled,
            cutover_enabled = excluded.cutover_enabled,
            global_kill_switch = excluded.global_kill_switch,
            reconcile_required = excluded.reconcile_required,
            latest_reconcile_run_id = excluded.latest_reconcile_run_id,
            latest_reconcile_status = excluded.latest_reconcile_status,
            latest_reconcile_created_at = excluded.latest_reconcile_created_at,
            latest_reconcile_age_sec = excluded.latest_reconcile_age_sec,
            append_only_ready = excluded.append_only_ready,
            outbox_status = excluded.outbox_status,
            outbox_job_present = excluded.outbox_job_present,
            parser_verified = excluded.parser_verified,
            data_usable = excluded.data_usable,
            market_data_dependency_ready = excluded.market_data_dependency_ready,
            worker_apply_enabled = excluded.worker_apply_enabled,
            observe_safe = excluded.observe_safe,
            event_age_sec = excluded.event_age_sec,
            event_future_skew_sec = excluded.event_future_skew_sec,
            skip_budget_limit = excluded.skip_budget_limit,
            skip_budget_used = excluded.skip_budget_used,
            skip_budget_remaining = excluded.skip_budget_remaining,
            rollback_required = excluded.rollback_required,
            controller_status = excluded.controller_status,
            would_skip_inline = excluded.would_skip_inline,
            effective_skip_inline = excluded.effective_skip_inline,
            effective_skip_disabled_in_pr20 = excluded.effective_skip_disabled_in_pr20,
            blocked_reason_codes_json = excluded.blocked_reason_codes_json,
            evidence_json = excluded.evidence_json,
            decided_at = excluded.decided_at
        """,
        (
            decision.event_id,
            decision.event_type,
            int(decision.dry_run_enabled),
            int(decision.cutover_enabled),
            int(decision.global_kill_switch),
            int(decision.reconcile_required),
            decision.latest_reconcile_run_id,
            decision.latest_reconcile_status,
            decision.latest_reconcile_created_at,
            decision.latest_reconcile_age_sec,
            int(decision.append_only_ready),
            decision.outbox_status,
            int(decision.outbox_job_present),
            int(decision.parser_verified),
            int(decision.data_usable),
            int(decision.market_data_dependency_ready),
            int(decision.worker_apply_enabled),
            int(decision.observe_safe),
            decision.event_age_sec,
            decision.event_future_skew_sec,
            decision.skip_budget_limit,
            decision.skip_budget_used,
            decision.skip_budget_remaining,
            int(decision.rollback_required),
            decision.controller_status,
            int(decision.would_skip_inline),
            int(decision.effective_skip_inline),
            int(decision.effective_skip_disabled_in_pr20),
            json.dumps(
                list(decision.blocked_reason_codes),
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            canonical_json(dict(decision.evidence)),
            decision.decided_at,
        ),
    )
    connection.commit()


def _market_data_dependency_status(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    expected_rows: int,
    settings: Settings,
) -> dict[str, Any]:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_tr_snapshots WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    artifact_count = int(row["count"] if row else 0)
    sibling = connection.execute(
        """
        SELECT status FROM projection_outbox
        WHERE projection_name = 'market_data' AND event_id = ?
        """,
        (event_id,),
    ).fetchone()
    sibling_status = None if sibling is None else str(sibling["status"]).upper()
    artifact_ready = expected_rows > 0 and artifact_count >= expected_rows
    ordered_worker_ready = bool(
        sibling_status in {"PENDING", "PROCESSING", "APPLIED"}
        and settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_data_apply_enabled
    )
    return {
        "ready": artifact_ready or ordered_worker_ready,
        "artifact_count": artifact_count,
        "expected_rows": expected_rows,
        "sibling_outbox_status": sibling_status,
        "ordered_worker_ready": ordered_worker_ready,
    }


def _outbox_job(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT * FROM projection_outbox
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else {key: row[key] for key in row.keys()}


def _outbox_counts(connection: sqlite3.Connection) -> dict[str, int]:
    result = {
        "pending_count": 0,
        "processing_count": 0,
        "applied_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "dead_letter_count": 0,
    }
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count FROM projection_outbox
        WHERE projection_name = 'market_scan' GROUP BY status
        """
    ).fetchall()
    for row in rows:
        key = f"{str(row['status']).lower()}_count"
        if key in result:
            result[key] = int(row["count"])
    return result


def _prior_scan_event_id(connection: sqlite3.Connection, event_id: str) -> str | None:
    current = connection.execute(
        "SELECT rowid AS event_rowid FROM gateway_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if current is None:
        return None
    row = connection.execute(
        """
        SELECT event_id FROM gateway_events
        WHERE status = 'ACCEPTED' AND lower(event_type) = 'tr_response'
          AND rowid < ?
          AND (
                lower(COALESCE(json_extract(payload_json, '$.request_id'), ''))
                    LIKE 'market_scan:%'
             OR lower(COALESCE(json_extract(payload_json, '$.request_name'), ''))
                    LIKE 'market_scan_%'
          )
        ORDER BY rowid DESC LIMIT 1
        """,
        (int(current["event_rowid"]),),
    ).fetchone()
    return None if row is None else str(row["event_id"])


def _decision_row(row: sqlite3.Row) -> dict[str, Any]:
    data = {key: row[key] for key in row.keys()}
    for key in (
        "dry_run_enabled",
        "cutover_enabled",
        "global_kill_switch",
        "reconcile_required",
        "append_only_ready",
        "outbox_job_present",
        "parser_verified",
        "data_usable",
        "market_data_dependency_ready",
        "worker_apply_enabled",
        "observe_safe",
        "rollback_required",
        "would_skip_inline",
        "effective_skip_inline",
        "effective_skip_disabled_in_pr20",
    ):
        data[key] = bool(data[key])
    data["blocked_reason_codes"] = json.loads(data.pop("blocked_reason_codes_json"))
    data["evidence"] = json.loads(data.pop("evidence_json"))
    return data


def _count_decisions(
    connection: sqlite3.Connection,
    *,
    field_name: str | None = None,
) -> int:
    if field_name is None:
        row = connection.execute(
            "SELECT COUNT(*) AS count FROM market_scan_projection_routing_decisions"
        ).fetchone()
    elif field_name in {"would_skip_inline", "effective_skip_inline"}:
        row = connection.execute(
            f"SELECT COUNT(*) AS count FROM market_scan_projection_routing_decisions "
            f"WHERE {field_name} = 1"
        ).fetchone()
    else:
        raise ValueError("unsupported field_name")
    return int(row["count"] if row else 0)


def _effective_skip_health(
    connection: sqlite3.Connection,
    *,
    exclude_event_id: str | None = None,
) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            SUM(CASE WHEN outbox.status IS NULL OR outbox.status <> 'APPLIED'
                     THEN 1 ELSE 0 END) AS pending_worker_count,
            SUM(CASE WHEN outbox.status IN ('ERROR', 'DEAD_LETTER')
                     THEN 1 ELSE 0 END) AS worker_error_count,
            SUM(CASE WHEN COALESCE(
                        json_extract(
                            outbox.metadata_json,
                            '$.last_worker_evidence.apply_result'
                        ),
                        ''
                     ) <> 'APPLIED_BY_WORKER'
                     THEN 1 ELSE 0 END) AS worker_apply_evidence_missing_count,
            SUM(CASE WHEN NOT EXISTS (
                        SELECT 1 FROM market_scan_snapshots AS snapshot
                        WHERE snapshot.source_event_id = decision.event_id
                     ) THEN 1 ELSE 0 END) AS artifact_missing_count,
            SUM(CASE WHEN NOT EXISTS (
                        SELECT 1 FROM market_scan_snapshots AS snapshot
                        WHERE snapshot.source_event_id = decision.event_id
                          AND snapshot.generated_by LIKE 'projection_outbox_worker:%'
                     ) THEN 1 ELSE 0 END) AS worker_lineage_missing_count
        FROM market_scan_projection_routing_decisions AS decision
        LEFT JOIN projection_outbox AS outbox
          ON outbox.projection_name = 'market_scan'
         AND outbox.event_id = decision.event_id
        WHERE decision.effective_skip_inline = 1
          AND (? IS NULL OR decision.event_id <> ?)
        """,
        (exclude_event_id, exclude_event_id),
    ).fetchone()
    return {
        "pending_worker_count": int(row["pending_worker_count"] or 0),
        "worker_error_count": int(row["worker_error_count"] or 0),
        "worker_apply_evidence_missing_count": int(
            row["worker_apply_evidence_missing_count"] or 0
        ),
        "artifact_missing_count": int(row["artifact_missing_count"] or 0),
        "worker_lineage_missing_count": int(
            row["worker_lineage_missing_count"] or 0
        ),
    }


def _rollback_reason_codes(
    *,
    outbox_counts: Mapping[str, int],
    effective_skip_health: Mapping[str, int],
    settings: Settings,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if int(outbox_counts.get("error_count") or 0) > 0:
        reasons.append("MARKET_SCAN_OUTBOX_ERROR")
    if int(outbox_counts.get("dead_letter_count") or 0) > 0:
        reasons.append("MARKET_SCAN_OUTBOX_DEAD_LETTER")
    if int(outbox_counts.get("processing_count") or 0) > 0:
        reasons.append("MARKET_SCAN_OUTBOX_PROCESSING_ACTIVE")
    if int(outbox_counts.get("pending_count") or 0) > int(
        settings.gateway_market_scan_append_only_max_pending_within_sla
    ):
        reasons.append("MARKET_SCAN_OUTBOX_PENDING_EXCEEDED")
    if int(effective_skip_health.get("pending_worker_count") or 0) > 0:
        reasons.append("MARKET_SCAN_EFFECTIVE_SKIP_PENDING_WORKER")
    if int(effective_skip_health.get("worker_error_count") or 0) > 0:
        reasons.append("MARKET_SCAN_EFFECTIVE_SKIP_WORKER_ERROR")
    if int(
        effective_skip_health.get("worker_apply_evidence_missing_count") or 0
    ) > 0:
        reasons.append("MARKET_SCAN_EFFECTIVE_SKIP_WORKER_EVIDENCE_MISSING")
    if int(effective_skip_health.get("artifact_missing_count") or 0) > 0:
        reasons.append("MARKET_SCAN_EFFECTIVE_SKIP_ARTIFACT_MISSING")
    if int(effective_skip_health.get("worker_lineage_missing_count") or 0) > 0:
        reasons.append("MARKET_SCAN_EFFECTIVE_SKIP_WORKER_LINEAGE_MISSING")
    return tuple(sorted(set(reasons)))


def _controller_status(
    *,
    settings: Settings,
    observe_safe: bool,
    worker_apply_enabled: bool,
    latest_status: str | None,
    append_only_ready: bool,
    latest_age_sec: float | None,
    reconcile_covers_prior_event: bool,
    event_age_sec: float | None,
    event_future_skew_sec: float | None,
    rollback_reason_codes: Sequence[str],
) -> str:
    if rollback_reason_codes:
        return "FAIL"
    ready = bool(
        settings.gateway_market_scan_append_only_dry_run_enabled
        and settings.gateway_market_scan_append_only_cutover_enabled
        and not settings.gateway_market_scan_append_only_global_kill_switch
        and not settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20
        and settings.gateway_market_scan_append_only_max_skip_per_minute > 0
        and settings.gateway_market_scan_append_only_require_reconcile_pass
        and settings.gateway_market_scan_append_only_require_prior_event_reconcile
        and settings.gateway_market_scan_append_only_require_parser_verified
        and settings.gateway_market_scan_append_only_require_market_data_dependency
        and settings.gateway_market_scan_append_only_require_worker_closure
        and settings.gateway_market_scan_append_only_fail_closed_on_worker_error
        and observe_safe
        and worker_apply_enabled
        and latest_status == "PASS"
        and append_only_ready
        and latest_age_sec is not None
        and latest_age_sec
        <= settings.gateway_market_scan_append_only_reconcile_max_age_sec
        and reconcile_covers_prior_event
        and event_age_sec is not None
        and event_age_sec <= settings.gateway_market_scan_append_only_max_event_age_sec
        and event_future_skew_sec is not None
        and event_future_skew_sec
        <= settings.gateway_market_scan_append_only_max_future_skew_sec
    )
    if ready:
        return "PASS"
    cutover_requested = bool(
        settings.gateway_market_scan_append_only_cutover_enabled
        and not settings.gateway_market_scan_append_only_global_kill_switch
        and not settings.gateway_market_scan_append_only_effective_skip_disabled_in_pr20
    )
    return "FAIL" if cutover_requested else "WARN"


def _skip_budget_used(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT used_count FROM market_scan_append_only_budget_state
        WHERE budget_name = 'market_scan_global' AND minute_bucket = ?
        """,
        (_budget_minute_bucket(),),
    ).fetchone()
    return 0 if row is None else int(row["used_count"])


def _reserve_skip_budget(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    limit: int,
) -> int | None:
    existing = connection.execute(
        """
        SELECT effective_skip_inline
        FROM market_scan_projection_routing_decisions
        WHERE projection_name = 'market_scan' AND event_id = ?
        """,
        (event_id,),
    ).fetchone()
    if existing is not None and bool(existing["effective_skip_inline"]):
        return _skip_budget_used(connection)
    minute_bucket = _budget_minute_bucket()
    now = datetime_to_wire(utc_now())
    try:
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.execute(
            """
            INSERT INTO market_scan_append_only_budget_state (
                budget_name, minute_bucket, used_count, last_event_id, updated_at
            ) VALUES ('market_scan_global', ?, 1, ?, ?)
            ON CONFLICT(budget_name) DO UPDATE SET
                minute_bucket = excluded.minute_bucket,
                used_count = CASE
                    WHEN market_scan_append_only_budget_state.minute_bucket
                         = excluded.minute_bucket
                    THEN market_scan_append_only_budget_state.used_count + 1
                    ELSE 1
                END,
                last_event_id = excluded.last_event_id,
                updated_at = excluded.updated_at
            WHERE market_scan_append_only_budget_state.minute_bucket
                  <> excluded.minute_bucket
               OR market_scan_append_only_budget_state.used_count < ?
            """,
            (minute_bucket, event_id, now, int(limit)),
        )
        if cursor.rowcount != 1:
            connection.rollback()
            return None
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return _skip_budget_used(connection)


def _budget_minute_bucket() -> str:
    return datetime_to_wire(utc_now().replace(second=0, microsecond=0))


def _timestamp_age_and_future_skew(value: object) -> tuple[float | None, float | None]:
    try:
        delta = (utc_now() - parse_timestamp(value, "timestamp")).total_seconds()
    except Exception:
        return None, None
    return max(delta, 0.0), max(-delta, 0.0)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _age_seconds(value: object) -> float | None:
    if value is None:
        return None
    try:
        return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)
    except Exception:
        return None
