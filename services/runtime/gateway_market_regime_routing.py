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
from services.market_context_service import get_market_context_status
from services.runtime.market_regime_projection_reconcile import (
    get_latest_market_regime_projection_reconcile,
)

PROJECTION_NAME_MARKET_REGIME = "market_regime"
MARKET_REGIME_EFFECTIVE_SKIP_DISABLED_REASON = "MARKET_REGIME_EFFECTIVE_SKIP_DISABLED_IN_PR18"


@dataclass(frozen=True, kw_only=True)
class MarketRegimeAppendOnlyRoutingDecision:
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
    index_artifact_present: bool
    context_ready: bool
    worker_apply_enabled: bool
    index_routing_ready: bool
    skip_budget_limit: int
    skip_budget_used: int
    skip_budget_remaining: int
    would_skip_inline: bool
    effective_skip_inline: bool
    effective_skip_disabled_in_pr18: bool
    rollback_required: bool
    controller_status: str
    blocked_reason_codes: tuple[str, ...]
    evidence: Mapping[str, Any] = field(default_factory=dict)
    decided_at: str = ""
    projection_name: str = PROJECTION_NAME_MARKET_REGIME
    no_trading_side_effects: bool = True
    observe_safe: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "projection_name": self.projection_name,
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
            "index_artifact_present": self.index_artifact_present,
            "context_ready": self.context_ready,
            "worker_apply_enabled": self.worker_apply_enabled,
            "index_routing_ready": self.index_routing_ready,
            "skip_budget_limit": self.skip_budget_limit,
            "skip_budget_used": self.skip_budget_used,
            "skip_budget_remaining": self.skip_budget_remaining,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "effective_skip_disabled_in_pr18": self.effective_skip_disabled_in_pr18,
            "rollback_required": self.rollback_required,
            "controller_status": self.controller_status,
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
            "read_only": True,
            "no_trading_side_effects": self.no_trading_side_effects,
            "observe_safe": self.observe_safe,
        }


def decide_market_regime_append_only_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketRegimeAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    decided_at = datetime_to_wire(utc_now())
    dry_run_enabled = bool(settings.gateway_market_regime_append_only_dry_run_enabled)
    cutover_enabled = bool(settings.gateway_market_regime_append_only_cutover_enabled)
    global_kill_switch = bool(settings.gateway_market_regime_append_only_global_kill_switch)
    effective_guard = bool(
        settings.gateway_market_regime_append_only_effective_skip_disabled_in_pr18
    )
    observe_safe = _is_observe_safe(settings)
    skip_budget_limit = int(settings.gateway_market_regime_append_only_max_skip_per_minute)
    skip_budget_used = _skip_budget_used(connection)
    skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)

    latest = get_latest_market_regime_projection_reconcile(connection)
    latest_run = _mapping(latest.get("latest_run"))
    latest_created_at = _optional_text(latest_run.get("created_at"))
    latest_age_sec = _age_seconds(latest_created_at)
    latest_status = _optional_text(latest_run.get("status"))
    append_only_ready = bool(latest_run.get("append_only_ready"))
    reconcile_required = bool(settings.gateway_market_regime_append_only_require_reconcile_pass)
    prior_event_id = _prior_accepted_market_index_event_id(
        connection,
        current_event_id=event.event_id,
    )
    reconcile_covers_prior_event = bool(
        prior_event_id
        and _optional_text(latest_run.get("latest_event_id")) == prior_event_id
        and latest_run.get("latest_event_covered") is True
    )

    outbox = _outbox_job(connection, event.event_id)
    normalized_outbox_status = _normalize_status(
        (outbox.get("status") if outbox else None) or outbox_status
    )
    index_artifact_present = _market_index_sample_exists(connection, event.event_id)
    source_status = _gateway_event_status(connection, event.event_id)
    context_status = get_market_context_status(connection, settings=settings)
    context_ready = bool(context_status.get("status") == "PASS")
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_regime_apply_enabled
    )
    index_routing = _market_index_routing_guard(connection, event.event_id)
    index_routing_ready = bool(index_routing.get("ready"))
    outbox_counts = _outbox_counts(connection)
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
    if event_type != "market_index_tick":
        reasons.append("NOT_MARKET_INDEX_TICK")
    if not observe_safe:
        reasons.append("MARKET_REGIME_CORE_NOT_OBSERVE_SAFE")
    if not dry_run_enabled:
        reasons.append("DRY_RUN_DISABLED")
    if source_status != "ACCEPTED":
        reasons.append("SOURCE_GATEWAY_EVENT_NOT_ACCEPTED")
    if outbox is None or normalized_outbox_status not in {
        "ENQUEUED",
        "PENDING",
        "PROCESSING",
        "APPLIED",
    }:
        reasons.append("MARKET_REGIME_OUTBOX_JOB_MISSING_OR_NOT_READY")
    if not index_artifact_present:
        reasons.append("MARKET_REGIME_INDEX_DEPENDENCY_MISSING")
    if not worker_apply_enabled:
        reasons.append("MARKET_REGIME_WORKER_APPLY_DISABLED")
    if not settings.market_regime_enabled:
        reasons.append("MARKET_REGIME_DISABLED")
    if not reconcile_required:
        reasons.append("MARKET_REGIME_RECONCILE_GUARD_DISABLED")
    if latest_status != "PASS":
        reasons.append("MARKET_REGIME_RECONCILE_NOT_PASS")
    if latest_age_sec is None or (
        latest_age_sec > settings.gateway_market_regime_append_only_reconcile_max_age_sec
    ):
        reasons.append("MARKET_REGIME_RECONCILE_STALE")
    if not append_only_ready:
        reasons.append("MARKET_REGIME_APPEND_ONLY_NOT_READY")
    if not settings.gateway_market_regime_append_only_require_prior_event_reconcile:
        reasons.append("MARKET_REGIME_PRIOR_EVENT_RECONCILE_GUARD_DISABLED")
    elif not reconcile_covers_prior_event:
        reasons.append("MARKET_REGIME_RECONCILE_PRIOR_EVENT_NOT_COVERED")
    if not settings.gateway_market_regime_append_only_require_index_routing_guard:
        reasons.append("MARKET_REGIME_INDEX_ROUTING_GUARD_DISABLED")
    elif not index_routing_ready:
        reasons.append("MARKET_REGIME_INDEX_ROUTING_NOT_READY")
        reasons.extend(str(value) for value in index_routing.get("reason_codes") or [])
    if not settings.gateway_market_regime_append_only_require_worker_context_refresh:
        reasons.append("MARKET_REGIME_WORKER_CONTEXT_REFRESH_GUARD_DISABLED")
    if not settings.gateway_market_regime_append_only_fail_closed_on_context_refresh_error:
        reasons.append("MARKET_REGIME_CONTEXT_REFRESH_FAIL_CLOSED_DISABLED")
    if not context_ready:
        reasons.append("MARKET_REGIME_CONTEXT_NOT_READY")
    if rollback_reason_codes:
        reasons.append("MARKET_REGIME_INLINE_ROLLBACK_REQUIRED")
        reasons.extend(rollback_reason_codes)

    would_skip_inline = not reasons
    blocked_reason_codes = [*reasons]
    effective_skip_inline = False
    if would_skip_inline:
        blocked_reason_codes.append("DRY_RUN_WOULD_SKIP_INLINE")
        if not cutover_enabled:
            blocked_reason_codes.append("MARKET_REGIME_CUTOVER_DISABLED")
        elif global_kill_switch:
            blocked_reason_codes.append("MARKET_REGIME_GLOBAL_KILL_SWITCH")
        elif effective_guard:
            blocked_reason_codes.append(MARKET_REGIME_EFFECTIVE_SKIP_DISABLED_REASON)
        elif skip_budget_limit <= 0:
            blocked_reason_codes.append("MARKET_REGIME_SKIP_BUDGET_EXHAUSTED")
        else:
            reserved_budget_used = _reserve_skip_budget(
                connection,
                event_id=event.event_id,
                limit=skip_budget_limit,
            )
            if reserved_budget_used is None:
                blocked_reason_codes.append("MARKET_REGIME_SKIP_BUDGET_EXHAUSTED")
                skip_budget_used = _skip_budget_used(connection)
            else:
                effective_skip_inline = True
                skip_budget_used = reserved_budget_used
                blocked_reason_codes.append("MARKET_REGIME_LIMITED_CUTOVER_EFFECTIVE_SKIP")
            skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    elif cutover_enabled and effective_guard:
        blocked_reason_codes.append(MARKET_REGIME_EFFECTIVE_SKIP_DISABLED_REASON)

    controller_status = _controller_status(
        settings=settings,
        observe_safe=observe_safe,
        worker_apply_enabled=worker_apply_enabled,
        latest_status=latest_status,
        append_only_ready=append_only_ready,
        latest_age_sec=latest_age_sec,
        context_ready=context_ready,
        index_routing_ready=index_routing_ready,
        reconcile_covers_prior_event=reconcile_covers_prior_event,
        rollback_reason_codes=rollback_reason_codes,
    )
    evidence = {
        "pr": "PR-19",
        "source_status": source_status,
        "context_status": context_status,
        "outbox": outbox,
        "outbox_enqueue_status": _normalize_status(outbox_status),
        "outbox_counts": outbox_counts,
        "latest_reconcile": latest_run,
        "prior_event_id": prior_event_id,
        "reconcile_covers_prior_event": reconcile_covers_prior_event,
        "index_routing": index_routing,
        "effective_skip_health": effective_skip_health,
        "rollback_reason_codes": list(rollback_reason_codes),
        "skip_budget_limit": skip_budget_limit,
        "skip_budget_used": skip_budget_used,
        "skip_budget_remaining": skip_budget_remaining,
        "inline_market_regime_path_retained": not effective_skip_inline,
        "market_context_deferred_to_worker": effective_skip_inline,
        "nxt_is_not_valid_market_regime_evidence": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    decision = MarketRegimeAppendOnlyRoutingDecision(
        event_id=event.event_id,
        event_type=event_type,
        dry_run_enabled=dry_run_enabled,
        cutover_enabled=cutover_enabled,
        global_kill_switch=global_kill_switch,
        reconcile_required=reconcile_required,
        latest_reconcile_run_id=_optional_text(latest_run.get("run_id")),
        latest_reconcile_status=latest_status,
        latest_reconcile_created_at=latest_created_at,
        latest_reconcile_age_sec=latest_age_sec,
        append_only_ready=append_only_ready,
        outbox_status=normalized_outbox_status,
        outbox_job_present=outbox is not None,
        index_artifact_present=index_artifact_present,
        context_ready=context_ready,
        worker_apply_enabled=worker_apply_enabled,
        index_routing_ready=index_routing_ready,
        skip_budget_limit=skip_budget_limit,
        skip_budget_used=skip_budget_used,
        skip_budget_remaining=skip_budget_remaining,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=effective_skip_inline,
        effective_skip_disabled_in_pr18=effective_guard,
        rollback_required=bool(rollback_reason_codes),
        controller_status=controller_status,
        blocked_reason_codes=tuple(dict.fromkeys(blocked_reason_codes)),
        evidence=evidence,
        decided_at=decided_at,
        observe_safe=observe_safe,
    )
    _persist_decision(connection, decision)
    return decision


def get_latest_market_regime_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, Any]:
    rows = list_market_regime_append_only_routing_decisions(connection, limit=100)
    latest_reconcile = get_latest_market_regime_projection_reconcile(connection)
    latest_run = _mapping(latest_reconcile.get("latest_run"))
    outbox_counts = _outbox_counts(connection)
    effective_skip_health = _effective_skip_health(connection)
    rollback_reason_codes = _rollback_reason_codes(
        outbox_counts=outbox_counts,
        effective_skip_health=effective_skip_health,
        settings=settings,
    )
    skip_budget_limit = int(settings.gateway_market_regime_append_only_max_skip_per_minute)
    skip_budget_used = _skip_budget_used(connection)
    latest = rows[0] if rows else None
    controller_status = str((latest or {}).get("controller_status") or "WARN").upper()
    return {
        "status": controller_status,
        "controller_status": controller_status,
        "dry_run_enabled": settings.gateway_market_regime_append_only_dry_run_enabled,
        "cutover_enabled": settings.gateway_market_regime_append_only_cutover_enabled,
        "global_kill_switch": (settings.gateway_market_regime_append_only_global_kill_switch),
        "effective_skip_disabled_in_pr18": (
            settings.gateway_market_regime_append_only_effective_skip_disabled_in_pr18
        ),
        "reconcile_required": (settings.gateway_market_regime_append_only_require_reconcile_pass),
        "prior_event_reconcile_required": (
            settings.gateway_market_regime_append_only_require_prior_event_reconcile
        ),
        "index_routing_guard_required": (
            settings.gateway_market_regime_append_only_require_index_routing_guard
        ),
        "worker_context_refresh_required": (
            settings.gateway_market_regime_append_only_require_worker_context_refresh
        ),
        "context_refresh_fail_closed": (
            settings.gateway_market_regime_append_only_fail_closed_on_context_refresh_error
        ),
        "worker_apply_enabled": bool(
            settings.projection_outbox_apply_projection_enabled
            and settings.projection_outbox_market_regime_apply_enabled
        ),
        "skip_budget_limit": skip_budget_limit,
        "skip_budget_used": skip_budget_used,
        "skip_budget_remaining": max(skip_budget_limit - skip_budget_used, 0),
        "decision_count": _count_decisions(connection),
        "would_skip_inline_count": _count_decisions(
            connection,
            field_name="would_skip_inline",
        ),
        "effective_skip_inline_count": _count_decisions(
            connection,
            field_name="effective_skip_inline",
        ),
        "latest_decision": latest,
        "latest_reconcile": latest_reconcile,
        "latest_reconcile_status": _optional_text(latest_run.get("status")),
        "append_only_ready": bool(latest_run.get("append_only_ready")),
        "outbox": outbox_counts,
        "effective_skip_health": effective_skip_health,
        "rollback_required": bool(rollback_reason_codes),
        "rollback_reason_codes": list(rollback_reason_codes),
        "rollback_hint": (
            "set GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH=true "
            "or GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED=false"
        ),
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_regime_append_only_routing_decisions(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT * FROM market_regime_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT ?
        """,
        (min(max(int(limit), 1), 500),),
    ).fetchall()
    return [_decision_row_to_dict(row) for row in rows]


def _persist_decision(
    connection: sqlite3.Connection,
    decision: MarketRegimeAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_regime_projection_routing_decisions (
            event_id, event_type, projection_name, dry_run_enabled,
            cutover_enabled, global_kill_switch, reconcile_required,
            latest_reconcile_run_id, latest_reconcile_status,
            latest_reconcile_created_at, latest_reconcile_age_sec,
            append_only_ready, outbox_status, outbox_job_present,
            index_artifact_present, context_ready, worker_apply_enabled,
            skip_budget_limit, skip_budget_used, skip_budget_remaining,
            observe_safe, index_routing_ready, rollback_required,
            controller_status, would_skip_inline, effective_skip_inline,
            effective_skip_disabled_in_pr18, blocked_reason_codes_json,
            evidence_json, decided_at
        ) VALUES (
            :event_id, :event_type, :projection_name, :dry_run_enabled,
            :cutover_enabled, :global_kill_switch, :reconcile_required,
            :latest_reconcile_run_id, :latest_reconcile_status,
            :latest_reconcile_created_at, :latest_reconcile_age_sec,
            :append_only_ready, :outbox_status, :outbox_job_present,
            :index_artifact_present, :context_ready, :worker_apply_enabled,
            :skip_budget_limit, :skip_budget_used, :skip_budget_remaining,
            :observe_safe, :index_routing_ready, :rollback_required,
            :controller_status, :would_skip_inline, :effective_skip_inline,
            :effective_skip_disabled_in_pr18, :blocked_reason_codes_json,
            :evidence_json, :decided_at
        )
        ON CONFLICT(event_id, projection_name) DO UPDATE SET
            event_type = excluded.event_type,
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
            index_artifact_present = excluded.index_artifact_present,
            context_ready = excluded.context_ready,
            worker_apply_enabled = excluded.worker_apply_enabled,
            skip_budget_limit = excluded.skip_budget_limit,
            skip_budget_used = excluded.skip_budget_used,
            skip_budget_remaining = excluded.skip_budget_remaining,
            observe_safe = excluded.observe_safe,
            index_routing_ready = excluded.index_routing_ready,
            rollback_required = excluded.rollback_required,
            controller_status = excluded.controller_status,
            would_skip_inline = excluded.would_skip_inline,
            effective_skip_inline = excluded.effective_skip_inline,
            effective_skip_disabled_in_pr18 = excluded.effective_skip_disabled_in_pr18,
            blocked_reason_codes_json = excluded.blocked_reason_codes_json,
            evidence_json = excluded.evidence_json,
            decided_at = excluded.decided_at
        """,
        {
            "event_id": decision.event_id,
            "event_type": decision.event_type,
            "projection_name": decision.projection_name,
            "dry_run_enabled": int(decision.dry_run_enabled),
            "cutover_enabled": int(decision.cutover_enabled),
            "global_kill_switch": int(decision.global_kill_switch),
            "reconcile_required": int(decision.reconcile_required),
            "latest_reconcile_run_id": decision.latest_reconcile_run_id,
            "latest_reconcile_status": decision.latest_reconcile_status,
            "latest_reconcile_created_at": decision.latest_reconcile_created_at,
            "latest_reconcile_age_sec": decision.latest_reconcile_age_sec,
            "append_only_ready": int(decision.append_only_ready),
            "outbox_status": decision.outbox_status,
            "outbox_job_present": int(decision.outbox_job_present),
            "index_artifact_present": int(decision.index_artifact_present),
            "context_ready": int(decision.context_ready),
            "worker_apply_enabled": int(decision.worker_apply_enabled),
            "skip_budget_limit": decision.skip_budget_limit,
            "skip_budget_used": decision.skip_budget_used,
            "skip_budget_remaining": decision.skip_budget_remaining,
            "observe_safe": int(decision.observe_safe),
            "index_routing_ready": int(decision.index_routing_ready),
            "rollback_required": int(decision.rollback_required),
            "controller_status": decision.controller_status,
            "would_skip_inline": int(decision.would_skip_inline),
            "effective_skip_inline": int(decision.effective_skip_inline),
            "effective_skip_disabled_in_pr18": int(decision.effective_skip_disabled_in_pr18),
            "blocked_reason_codes_json": json.dumps(
                list(decision.blocked_reason_codes),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            "evidence_json": canonical_json(decision.evidence),
            "decided_at": decision.decided_at,
        },
    )
    connection.commit()


def _market_index_routing_guard(
    connection: sqlite3.Connection,
    event_id: str,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT parser_status, parser_verified, data_source, data_usable, evidence_json
        FROM market_index_projection_routing_decisions
        WHERE event_id = ? AND projection_name = 'market_index'
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    if row is None:
        return {
            "ready": False,
            "reason_codes": ["MARKET_REGIME_INDEX_ROUTING_DECISION_MISSING"],
        }
    evidence = _json_object(row["evidence_json"])
    gateway_health = _mapping(evidence.get("gateway_health"))
    reasons: list[str] = []
    if not bool(row["parser_verified"]):
        reasons.append("MARKET_REGIME_INDEX_PARSER_NOT_VERIFIED")
    if not bool(row["data_usable"]):
        reasons.append("MARKET_REGIME_INDEX_DATA_NOT_USABLE")
    if str(row["data_source"] or "").upper() != "REALTIME":
        reasons.append("MARKET_REGIME_INDEX_SOURCE_NOT_REALTIME")
    if evidence.get("observe_safe") is not True:
        reasons.append("MARKET_REGIME_INDEX_ROUTING_NOT_OBSERVE_SAFE")
    if evidence.get("event_market_weekday") is not True:
        reasons.append("MARKET_REGIME_INDEX_EVENT_NON_TRADING_DAY")
    if str(evidence.get("event_market_session") or "") != "REGULAR":
        reasons.append("MARKET_REGIME_INDEX_EVENT_OUTSIDE_KRX_SESSION")
    if evidence.get("gateway_health_required") is not True:
        reasons.append("MARKET_REGIME_INDEX_GATEWAY_HEALTH_GUARD_DISABLED")
    if gateway_health.get("ready") is not True:
        reasons.append("MARKET_REGIME_INDEX_GATEWAY_HEALTH_NOT_READY")
    if not _within_limit(
        evidence.get("event_age_sec"),
        evidence.get("max_event_age_sec"),
    ):
        reasons.append("MARKET_REGIME_INDEX_EVENT_STALE")
    if not _within_limit(
        evidence.get("event_future_skew_sec"),
        evidence.get("max_future_skew_sec"),
    ):
        reasons.append("MARKET_REGIME_INDEX_EVENT_FUTURE_SKEW")
    return {
        "ready": not reasons,
        "parser_status": row["parser_status"],
        "parser_verified": bool(row["parser_verified"]),
        "data_source": row["data_source"],
        "data_usable": bool(row["data_usable"]),
        "gateway_health": gateway_health,
        "event_market_session": evidence.get("event_market_session"),
        "event_market_weekday": evidence.get("event_market_weekday"),
        "event_age_sec": evidence.get("event_age_sec"),
        "event_future_skew_sec": evidence.get("event_future_skew_sec"),
        "reason_codes": reasons,
    }


def _prior_accepted_market_index_event_id(
    connection: sqlite3.Connection,
    *,
    current_event_id: str,
) -> str | None:
    current = connection.execute(
        "SELECT rowid FROM gateway_events WHERE event_id = ? LIMIT 1",
        (current_event_id,),
    ).fetchone()
    if current is None:
        return None
    row = connection.execute(
        """
        SELECT event_id
        FROM gateway_events
        WHERE status = 'ACCEPTED'
          AND event_type = 'market_index_tick'
          AND rowid < ?
        ORDER BY rowid DESC
        LIMIT 1
        """,
        (int(current["rowid"]),),
    ).fetchone()
    return None if row is None else str(row["event_id"])


def _outbox_counts(connection: sqlite3.Connection) -> dict[str, int]:
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
        WHERE projection_name = 'market_regime'
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


def _effective_skip_health(
    connection: sqlite3.Connection,
    *,
    exclude_event_id: str | None = None,
) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS effective_skip_count,
            SUM(
                CASE
                    WHEN outbox.outbox_id IS NULL
                      OR outbox.status IN ('PENDING', 'PROCESSING')
                    THEN 1 ELSE 0
                END
            ) AS pending_worker_count,
            SUM(
                CASE
                    WHEN outbox.status IN ('ERROR', 'DEAD_LETTER', 'SKIPPED')
                    THEN 1 ELSE 0
                END
            ) AS worker_error_count,
            SUM(
                CASE
                    WHEN outbox.status = 'APPLIED'
                     AND COALESCE(
                        json_extract(
                            outbox.metadata_json,
                            '$.last_worker_evidence.apply_result'
                        ),
                        ''
                     ) != 'APPLIED_BY_WORKER'
                    THEN 1 ELSE 0
                END
            ) AS worker_apply_evidence_missing_count,
            SUM(
                CASE WHEN NOT EXISTS (
                    SELECT 1 FROM market_regime_snapshots AS snapshot
                    WHERE snapshot.source_event_id = decision.event_id
                ) THEN 1 ELSE 0 END
            ) AS regime_snapshot_missing_count,
            SUM(
                CASE WHEN (
                    SELECT COUNT(DISTINCT context.market)
                    FROM market_context_snapshots AS context
                    WHERE context.source_event_id = decision.event_id
                ) < 2 THEN 1 ELSE 0 END
            ) AS context_pair_missing_count
        FROM market_regime_projection_routing_decisions AS decision
        LEFT JOIN projection_outbox AS outbox
          ON outbox.event_id = decision.event_id
         AND outbox.projection_name = 'market_regime'
        WHERE decision.effective_skip_inline = 1
          AND (? IS NULL OR decision.event_id != ?)
        """,
        (exclude_event_id, exclude_event_id),
    ).fetchone()
    return {
        "effective_skip_count": int(row["effective_skip_count"] or 0),
        "pending_worker_count": int(row["pending_worker_count"] or 0),
        "worker_error_count": int(row["worker_error_count"] or 0),
        "worker_apply_evidence_missing_count": int(row["worker_apply_evidence_missing_count"] or 0),
        "regime_snapshot_missing_count": int(row["regime_snapshot_missing_count"] or 0),
        "context_pair_missing_count": int(row["context_pair_missing_count"] or 0),
    }


def _rollback_reason_codes(
    *,
    outbox_counts: Mapping[str, int],
    effective_skip_health: Mapping[str, int],
    settings: Settings,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if int(outbox_counts.get("error_count") or 0):
        reasons.append("MARKET_REGIME_OUTBOX_ERROR")
    if int(outbox_counts.get("dead_letter_count") or 0):
        reasons.append("MARKET_REGIME_OUTBOX_DEAD_LETTER")
    if int(outbox_counts.get("skipped_count") or 0):
        reasons.append("MARKET_REGIME_OUTBOX_SKIPPED")
    if int(outbox_counts.get("processing_count") or 0):
        reasons.append("MARKET_REGIME_OUTBOX_PROCESSING_ACTIVE")
    if int(outbox_counts.get("pending_count") or 0) > int(
        settings.gateway_market_regime_append_only_max_pending_within_sla
    ):
        reasons.append("MARKET_REGIME_OUTBOX_PENDING_EXCEEDED")
    for key, reason in (
        ("pending_worker_count", "MARKET_REGIME_EFFECTIVE_SKIP_PENDING_WORKER"),
        ("worker_error_count", "MARKET_REGIME_EFFECTIVE_SKIP_WORKER_ERROR"),
        (
            "worker_apply_evidence_missing_count",
            "MARKET_REGIME_EFFECTIVE_SKIP_WORKER_EVIDENCE_MISSING",
        ),
        (
            "regime_snapshot_missing_count",
            "MARKET_REGIME_EFFECTIVE_SKIP_SNAPSHOT_MISSING",
        ),
        (
            "context_pair_missing_count",
            "MARKET_REGIME_EFFECTIVE_SKIP_CONTEXT_PAIR_MISSING",
        ),
    ):
        if int(effective_skip_health.get(key) or 0):
            reasons.append(reason)
    return tuple(sorted(set(reasons)))


def _controller_status(
    *,
    settings: Settings,
    observe_safe: bool,
    worker_apply_enabled: bool,
    latest_status: str | None,
    append_only_ready: bool,
    latest_age_sec: float | None,
    context_ready: bool,
    index_routing_ready: bool,
    reconcile_covers_prior_event: bool,
    rollback_reason_codes: Sequence[str],
) -> str:
    active_cutover_requested = bool(
        settings.gateway_market_regime_append_only_cutover_enabled
        and not settings.gateway_market_regime_append_only_global_kill_switch
        and not settings.gateway_market_regime_append_only_effective_skip_disabled_in_pr18
    )
    gates_ready = bool(
        settings.gateway_market_regime_append_only_dry_run_enabled
        and active_cutover_requested
        and observe_safe
        and worker_apply_enabled
        and settings.market_regime_enabled
        and settings.gateway_market_regime_append_only_require_reconcile_pass
        and settings.gateway_market_regime_append_only_require_prior_event_reconcile
        and settings.gateway_market_regime_append_only_require_index_routing_guard
        and settings.gateway_market_regime_append_only_require_worker_context_refresh
        and settings.gateway_market_regime_append_only_fail_closed_on_context_refresh_error
        and latest_status == "PASS"
        and append_only_ready
        and latest_age_sec is not None
        and latest_age_sec <= settings.gateway_market_regime_append_only_reconcile_max_age_sec
        and context_ready
        and index_routing_ready
        and reconcile_covers_prior_event
        and settings.gateway_market_regime_append_only_max_skip_per_minute > 0
        and not rollback_reason_codes
    )
    if gates_ready:
        return "PASS"
    return "FAIL" if active_cutover_requested else "WARN"


def _reserve_skip_budget(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    limit: int,
) -> int | None:
    if limit <= 0:
        return None
    minute_bucket = _budget_minute_bucket()
    current = connection.execute(
        """
        SELECT used_count, last_event_id
        FROM market_regime_append_only_budget_state
        WHERE budget_name = 'market_regime_global' AND minute_bucket = ?
        """,
        (minute_bucket,),
    ).fetchone()
    if current is not None and str(current["last_event_id"] or "") == event_id:
        return int(current["used_count"])
    cursor = connection.execute(
        """
        INSERT INTO market_regime_append_only_budget_state (
            budget_name, minute_bucket, used_count, last_event_id, updated_at
        )
        VALUES ('market_regime_global', ?, 1, ?, ?)
        ON CONFLICT(budget_name) DO UPDATE SET
            minute_bucket = excluded.minute_bucket,
            used_count = CASE
                WHEN market_regime_append_only_budget_state.minute_bucket
                    = excluded.minute_bucket
                THEN market_regime_append_only_budget_state.used_count + 1
                ELSE 1
            END,
            last_event_id = excluded.last_event_id,
            updated_at = excluded.updated_at
        WHERE market_regime_append_only_budget_state.minute_bucket
                != excluded.minute_bucket
           OR market_regime_append_only_budget_state.used_count < ?
        """,
        (minute_bucket, event_id, datetime_to_wire(utc_now()), limit),
    )
    connection.commit()
    if cursor.rowcount != 1:
        return None
    return _skip_budget_used(connection)


def _skip_budget_used(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT used_count
        FROM market_regime_append_only_budget_state
        WHERE budget_name = 'market_regime_global' AND minute_bucket = ?
        """,
        (_budget_minute_bucket(),),
    ).fetchone()
    return 0 if row is None else int(row["used_count"])


def _budget_minute_bucket() -> str:
    return datetime_to_wire(utc_now().replace(second=0, microsecond=0))


def _count_decisions(
    connection: sqlite3.Connection,
    *,
    field_name: str | None = None,
) -> int:
    if field_name is None:
        sql = "SELECT COUNT(*) AS count FROM market_regime_projection_routing_decisions"
    elif field_name in {"would_skip_inline", "effective_skip_inline"}:
        sql = (
            "SELECT COUNT(*) AS count FROM market_regime_projection_routing_decisions "
            f"WHERE {field_name} = 1"
        )
    else:
        raise ValueError("unsupported decision field")
    return int(connection.execute(sql).fetchone()["count"])


def _outbox_job(connection: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, attempts, updated_at, metadata_json
        FROM projection_outbox
        WHERE projection_name = ? AND event_id = ?
        LIMIT 1
        """,
        (PROJECTION_NAME_MARKET_REGIME, event_id),
    ).fetchone()
    return None if row is None else _row_to_dict(row)


def _market_index_sample_exists(connection: sqlite3.Connection, event_id: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM market_index_tick_samples WHERE event_id = ? LIMIT 1",
            (event_id,),
        ).fetchone()
        is not None
    )


def _gateway_event_status(connection: sqlite3.Connection, event_id: str) -> str | None:
    row = connection.execute(
        "SELECT status FROM gateway_events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    return None if row is None else str(row["status"])


def _is_observe_safe(settings: Settings) -> bool:
    return bool(
        settings.trading_profile is TradingProfile.OBSERVE
        and settings.trading_mode is TradingMode.OBSERVE
        and not settings.trading_allow_live_sim
        and not settings.trading_allow_live_real
    )


def _within_limit(value: object, limit: object) -> bool:
    try:
        return float(value) >= 0 and float(limit) >= 0 and float(value) <= float(limit)
    except (TypeError, ValueError):
        return False


def _age_seconds(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max((utc_now() - parse_timestamp(value, "created_at")).total_seconds(), 0.0)
    except ValueError:
        return None


def _decision_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    data = _row_to_dict(row)
    data["blocked_reason_codes"] = json.loads(data.pop("blocked_reason_codes_json"))
    data["evidence"] = json.loads(data.pop("evidence_json"))
    for key in (
        "dry_run_enabled",
        "cutover_enabled",
        "global_kill_switch",
        "reconcile_required",
        "append_only_ready",
        "outbox_job_present",
        "index_artifact_present",
        "context_ready",
        "worker_apply_enabled",
        "observe_safe",
        "index_routing_ready",
        "rollback_required",
        "would_skip_inline",
        "effective_skip_inline",
        "effective_skip_disabled_in_pr18",
    ):
        data[key] = bool(data[key])
    return data


def _normalize_status(value: object) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        loaded = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}


def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}
