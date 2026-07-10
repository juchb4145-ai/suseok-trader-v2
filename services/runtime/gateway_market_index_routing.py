from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import (
    MARKET_TIMEZONE,
    datetime_to_wire,
    parse_timestamp,
    utc_now,
)
from storage.gateway_command_store import canonical_json

from services.config import Settings, TradingMode, TradingProfile, load_settings
from services.market_data_service import market_session_for_tick
from services.market_index_service import (
    MARKET_INDEX_SOURCE_REALTIME,
    classify_market_index_data_source,
    market_index_parser_status,
    market_index_parser_verified,
    market_index_payload_usability,
)
from services.runtime.market_index_projection_reconcile import (
    get_latest_market_index_projection_reconcile,
)

PROJECTION_NAME_MARKET_INDEX = "market_index"
MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_REASON = (
    "MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_IN_PR15"
)


@dataclass(frozen=True, kw_only=True)
class MarketIndexAppendOnlyRoutingDecision:
    event_id: str
    event_type: str
    dry_run_enabled: bool
    cutover_enabled: bool
    reconcile_required: bool
    data_usable_required: bool
    parser_verified_required: bool
    latest_reconcile_status: str | None
    latest_reconcile_run_id: str | None
    latest_reconcile_created_at: str | None
    latest_reconcile_age_sec: float | None
    append_only_ready: bool
    outbox_job_present: bool
    outbox_status: str | None
    parser_status: str
    parser_verified: bool
    data_source: str
    data_usable: bool
    would_skip_inline: bool
    effective_skip_inline: bool
    worker_apply_enabled: bool
    blocked_reason_codes: Sequence[str]
    decided_at: str
    evidence: Mapping[str, Any] = field(default_factory=dict)
    global_kill_switch: bool = True
    legacy_effective_skip_guard: bool = True
    skip_budget_limit: int = 0
    skip_budget_used: int = 0
    skip_budget_remaining: int = 0
    outbox_pending_count: int = 0
    outbox_processing_count: int = 0
    outbox_error_count: int = 0
    outbox_dead_letter_count: int = 0
    regime_continuity_required: bool = True
    regime_continuity_ready: bool = False
    rollback_required: bool = False
    rollback_reason_codes: Sequence[str] = field(default_factory=tuple)
    controller_status: str = "WARN"
    projection_name: str = PROJECTION_NAME_MARKET_INDEX
    no_trading_side_effects: bool = True
    observe_safe: bool = True
    gateway_health_required: bool = True
    gateway_health_ready: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "projection_name": self.projection_name,
            "dry_run_enabled": self.dry_run_enabled,
            "cutover_enabled": self.cutover_enabled,
            "reconcile_required": self.reconcile_required,
            "data_usable_required": self.data_usable_required,
            "parser_verified_required": self.parser_verified_required,
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_run_id": self.latest_reconcile_run_id,
            "latest_reconcile_created_at": self.latest_reconcile_created_at,
            "latest_reconcile_age_sec": self.latest_reconcile_age_sec,
            "append_only_ready": self.append_only_ready,
            "outbox_job_present": self.outbox_job_present,
            "outbox_status": self.outbox_status,
            "parser_status": self.parser_status,
            "parser_verified": self.parser_verified,
            "data_source": self.data_source,
            "data_usable": self.data_usable,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "worker_apply_enabled": self.worker_apply_enabled,
            "global_kill_switch": self.global_kill_switch,
            "legacy_effective_skip_guard": self.legacy_effective_skip_guard,
            "skip_budget_limit": self.skip_budget_limit,
            "skip_budget_used": self.skip_budget_used,
            "skip_budget_remaining": self.skip_budget_remaining,
            "outbox_pending_count": self.outbox_pending_count,
            "outbox_processing_count": self.outbox_processing_count,
            "outbox_error_count": self.outbox_error_count,
            "outbox_dead_letter_count": self.outbox_dead_letter_count,
            "regime_continuity_required": self.regime_continuity_required,
            "regime_continuity_ready": self.regime_continuity_ready,
            "rollback_required": self.rollback_required,
            "rollback_reason_codes": list(self.rollback_reason_codes),
            "controller_status": self.controller_status,
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
            "read_only": True,
            "no_trading_side_effects": self.no_trading_side_effects,
            "observe_safe": self.observe_safe,
            "gateway_health_required": self.gateway_health_required,
            "gateway_health_ready": self.gateway_health_ready,
        }


def decide_market_index_append_only_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketIndexAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    decided_at = datetime_to_wire(utc_now())
    dry_run_enabled = bool(settings.gateway_market_index_append_only_dry_run_enabled)
    cutover_enabled = bool(settings.gateway_market_index_append_only_cutover_enabled)
    global_kill_switch = bool(
        settings.gateway_market_index_append_only_global_kill_switch
    )
    legacy_effective_skip_guard = bool(
        settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15
    )
    skip_budget_limit = int(
        settings.gateway_market_index_append_only_max_skip_per_minute
    )
    observe_safe = _is_observe_safe(settings)
    skip_budget_used = _skip_budget_used(connection)
    skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    latest = get_latest_market_index_projection_reconcile(connection)
    latest_run = latest.get("latest_run")
    latest_status = _mapping_string(latest_run, "status")
    latest_run_id = _mapping_string(latest_run, "run_id")
    latest_created_at = _mapping_string(latest_run, "created_at")
    latest_age_sec = _age_seconds(latest_created_at)
    append_only_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    outbox_job = _outbox_job(connection, event.event_id)
    normalized_outbox_status = _normalize_outbox_status(
        (outbox_job.get("status") if outbox_job else None) or outbox_status
    )
    parser_status = market_index_parser_status(event.payload)
    parser_verified = market_index_parser_verified(event.payload)
    data_source = classify_market_index_data_source(event.payload)
    usability = market_index_payload_usability(event.payload)
    data_usable = bool(usability.get("data_usable"))
    event_now = utc_now()
    trade_time = _usability_timestamp(usability, "trade_time", fallback=event.ts)
    tick_ts = _usability_timestamp(usability, "tick_ts", fallback=event.ts)
    temporal_deltas = {
        "envelope": (event_now - event.ts).total_seconds(),
        "trade_time": (event_now - trade_time).total_seconds(),
        "tick_ts": (event_now - tick_ts).total_seconds(),
    }
    temporal_age_sec = {
        key: max(value, 0.0) for key, value in temporal_deltas.items()
    }
    temporal_future_skew_sec = {
        key: max(-value, 0.0) for key, value in temporal_deltas.items()
    }
    event_age_sec = max(temporal_age_sec.values())
    event_future_skew_sec = max(temporal_future_skew_sec.values())
    event_market_session = market_session_for_tick(trade_time, "KRX")
    event_market_weekday = trade_time.astimezone(MARKET_TIMEZONE).weekday() < 5
    gateway_health_required = bool(
        settings.gateway_market_index_append_only_require_fresh_gateway_health
    )
    gateway_health = _gateway_market_index_health(
        connection,
        max_age_sec=int(
            settings.gateway_market_index_append_only_gateway_health_max_age_sec
        ),
        max_future_skew_sec=int(
            settings.gateway_market_index_append_only_max_future_skew_sec
        ),
    )
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_index_apply_enabled
    )
    reconcile_required = bool(
        settings.gateway_market_index_append_only_require_reconcile_pass
    )
    data_usable_required = bool(
        settings.gateway_market_index_append_only_require_data_usable
    )
    parser_verified_required = bool(
        settings.gateway_market_index_append_only_require_parser_verified
    )
    regime_continuity_required = bool(
        settings.gateway_market_index_append_only_require_worker_regime_refresh
    )
    regime_refresh_fail_closed = bool(
        settings.gateway_market_index_append_only_fail_closed_on_regime_refresh_error
    )
    source_status = _gateway_event_status(connection, event.event_id)
    outbox_counts = _outbox_counts(connection)
    effective_skip_health = _effective_skip_health(connection)
    preapplied_continuity = _preapplied_event_continuity(
        connection,
        event_id=event.event_id,
    )
    rollback_reason_codes = _rollback_reason_codes(
        outbox_counts=outbox_counts,
        effective_skip_health=effective_skip_health,
        settings=settings,
    )
    regime_continuity_ready = not any(
        reason.startswith(("MARKET_INDEX_REGIME_", "MARKET_REGIME_"))
        for reason in rollback_reason_codes
    )

    reasons: list[str] = []
    would_skip_inline = False
    if event_type != "market_index_tick":
        reasons.append("NOT_MARKET_INDEX_TICK")
    elif not observe_safe:
        reasons.append("MARKET_INDEX_CORE_NOT_OBSERVE_SAFE")
    elif event_age_sec > settings.gateway_market_index_append_only_max_event_age_sec:
        reasons.append("MARKET_INDEX_EVENT_STALE")
    elif (
        event_future_skew_sec
        > settings.gateway_market_index_append_only_max_future_skew_sec
    ):
        reasons.append("MARKET_INDEX_EVENT_FUTURE_SKEW")
    elif not event_market_weekday:
        reasons.append("MARKET_INDEX_EVENT_NON_TRADING_DAY")
    elif event_market_session != "REGULAR":
        reasons.append("MARKET_INDEX_EVENT_OUTSIDE_KRX_SESSION")
    elif not gateway_health_required:
        reasons.append("MARKET_INDEX_GATEWAY_HEALTH_GUARD_DISABLED")
    elif not gateway_health["ready"]:
        reasons.append("MARKET_INDEX_GATEWAY_HEALTH_NOT_READY")
    elif not dry_run_enabled:
        reasons.append("DRY_RUN_DISABLED")
    elif source_status != "ACCEPTED":
        reasons.append("SOURCE_GATEWAY_EVENT_NOT_ACCEPTED")
    elif outbox_job is None or normalized_outbox_status not in {
        "ENQUEUED",
        "PENDING",
        "PROCESSING",
        "APPLIED",
    }:
        reasons.append("MARKET_INDEX_OUTBOX_JOB_MISSING_OR_NOT_READY")
    elif (
        normalized_outbox_status == "APPLIED"
        and not preapplied_continuity["ready"]
    ):
        reasons.append("MARKET_INDEX_PREAPPLIED_CONTINUITY_NOT_READY")
    elif not worker_apply_enabled:
        reasons.append("MARKET_INDEX_WORKER_APPLY_DISABLED")
    elif not reconcile_required:
        reasons.append("MARKET_INDEX_RECONCILE_GUARD_DISABLED")
    elif latest_run is None:
        reasons.append("MARKET_INDEX_RECONCILE_MISSING")
    elif latest_status != "PASS" or not append_only_ready:
        reasons.append("MARKET_INDEX_RECONCILE_NOT_PASS")
    elif latest_age_sec is None:
        reasons.append("MARKET_INDEX_RECONCILE_FRESHNESS_UNKNOWN")
    elif (
        latest_age_sec is not None
        and latest_age_sec
        > settings.gateway_market_index_append_only_reconcile_max_age_sec
    ):
        reasons.append("MARKET_INDEX_RECONCILE_STALE")
    elif not data_usable_required:
        reasons.append("MARKET_INDEX_DATA_USABILITY_GUARD_DISABLED")
    elif not data_usable:
        reasons.append("MARKET_INDEX_DATA_NOT_USABLE")
    elif not parser_verified_required:
        reasons.append("MARKET_INDEX_PARSER_VERIFICATION_GUARD_DISABLED")
    elif not parser_verified:
        reasons.append("MARKET_INDEX_PARSER_NOT_VERIFIED")
    elif data_source != MARKET_INDEX_SOURCE_REALTIME:
        reasons.append("MARKET_INDEX_SOURCE_NOT_SUPPORTED_FOR_CUTOVER")
    elif not regime_continuity_required:
        reasons.append("MARKET_INDEX_REGIME_CONTINUITY_GUARD_DISABLED")
    elif not settings.market_regime_enabled:
        reasons.append("MARKET_INDEX_REGIME_DISABLED")
    elif not regime_refresh_fail_closed:
        reasons.append("MARKET_INDEX_REGIME_REFRESH_FAIL_CLOSED_DISABLED")
    else:
        would_skip_inline = True
        reasons.append("DRY_RUN_WOULD_SKIP_INLINE")

    effective_skip_inline = False
    if would_skip_inline:
        if not cutover_enabled:
            reasons.append("MARKET_INDEX_CUTOVER_DISABLED")
        elif global_kill_switch:
            reasons.append("MARKET_INDEX_GLOBAL_KILL_SWITCH")
        elif legacy_effective_skip_guard:
            reasons.append(MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_REASON)
        elif not worker_apply_enabled:
            reasons.append("MARKET_INDEX_WORKER_APPLY_DISABLED")
        elif rollback_reason_codes:
            reasons.append("MARKET_INDEX_INLINE_ROLLBACK_REQUIRED")
            reasons.extend(rollback_reason_codes)
        elif skip_budget_limit <= 0:
            reasons.append("MARKET_INDEX_SKIP_BUDGET_EXHAUSTED")
        else:
            reserved_budget_used = _reserve_skip_budget(
                connection,
                event_id=event.event_id,
                limit=skip_budget_limit,
            )
            if reserved_budget_used is None:
                reasons.append("MARKET_INDEX_SKIP_BUDGET_EXHAUSTED")
                skip_budget_used = _skip_budget_used(connection)
            else:
                effective_skip_inline = True
                skip_budget_used = reserved_budget_used
                reasons.append("MARKET_INDEX_LIMITED_CUTOVER_EFFECTIVE_SKIP")
            skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    elif cutover_enabled and legacy_effective_skip_guard:
        reasons.append(MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_REASON)
    if (
        legacy_effective_skip_guard
        and (cutover_enabled or would_skip_inline)
        and MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_REASON not in reasons
    ):
        reasons.append(MARKET_INDEX_EFFECTIVE_SKIP_DISABLED_REASON)

    controller_status = _controller_status(
        dry_run_enabled=dry_run_enabled,
        observe_safe=observe_safe,
        gateway_health_required=gateway_health_required,
        gateway_health_ready=bool(gateway_health["ready"]),
        cutover_enabled=cutover_enabled,
        global_kill_switch=global_kill_switch,
        legacy_effective_skip_guard=legacy_effective_skip_guard,
        skip_budget_limit=skip_budget_limit,
        worker_apply_enabled=worker_apply_enabled,
        latest_status=latest_status,
        append_only_ready=append_only_ready,
        latest_age_sec=latest_age_sec,
        parser_verified_required=parser_verified_required,
        reconcile_required=reconcile_required,
        data_usable_required=data_usable_required,
        regime_continuity_required=regime_continuity_required,
        regime_refresh_fail_closed=regime_refresh_fail_closed,
        rollback_reason_codes=rollback_reason_codes,
        settings=settings,
    )
    evidence = {
        "pr": "PR-16",
        "observe_safe": observe_safe,
        "event_age_sec": event_age_sec,
        "event_future_skew_sec": event_future_skew_sec,
        "temporal_age_sec": temporal_age_sec,
        "temporal_future_skew_sec": temporal_future_skew_sec,
        "payload_trade_time": datetime_to_wire(trade_time),
        "payload_tick_ts": datetime_to_wire(tick_ts),
        "event_market_session": event_market_session,
        "event_market_weekday": event_market_weekday,
        "max_event_age_sec": int(
            settings.gateway_market_index_append_only_max_event_age_sec
        ),
        "max_future_skew_sec": int(
            settings.gateway_market_index_append_only_max_future_skew_sec
        ),
        "gateway_health_required": gateway_health_required,
        "gateway_health": gateway_health,
        "cutover_status": "LIMITED" if effective_skip_inline else "INLINE_FALLBACK",
        "inline_market_index_projection_skipped": effective_skip_inline,
        "market_regime_deferred_to_worker": effective_skip_inline,
        "effective_skip_disabled_in_pr15": legacy_effective_skip_guard,
        "payload_usability": usability,
        "source_gateway_event_status": source_status,
        "outbox_enqueue_status": _normalize_outbox_status(outbox_status),
        "latest_reconcile": latest_run,
        "global_kill_switch": global_kill_switch,
        "skip_budget_limit": skip_budget_limit,
        "skip_budget_used": skip_budget_used,
        "skip_budget_remaining": skip_budget_remaining,
        "outbox_counts": outbox_counts,
        "effective_skip_health": effective_skip_health,
        "preapplied_event_continuity": preapplied_continuity,
        "rollback_required": bool(rollback_reason_codes),
        "rollback_reason_codes": list(rollback_reason_codes),
        "regime_continuity_required": regime_continuity_required,
        "regime_refresh_fail_closed": regime_refresh_fail_closed,
        "regime_continuity_ready": regime_continuity_ready,
        "controller_status": controller_status,
        "parser_confidence_separate_from_data_usability": True,
        "tr_bootstrap_adapter_status": "IMPLEMENTED",
        "tr_bootstrap_enabled": settings.market_index_tr_bootstrap_enabled,
        "tr_bootstrap_parser_status": (
            settings.market_index_tr_bootstrap_parser_status
        ),
        "nxt_is_not_valid_market_index_evidence": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
    }
    decision = MarketIndexAppendOnlyRoutingDecision(
        event_id=event.event_id,
        event_type=event_type,
        dry_run_enabled=dry_run_enabled,
        cutover_enabled=cutover_enabled,
        reconcile_required=reconcile_required,
        data_usable_required=data_usable_required,
        parser_verified_required=parser_verified_required,
        latest_reconcile_status=latest_status,
        latest_reconcile_run_id=latest_run_id,
        latest_reconcile_created_at=latest_created_at,
        latest_reconcile_age_sec=latest_age_sec,
        append_only_ready=append_only_ready,
        outbox_job_present=outbox_job is not None,
        outbox_status=normalized_outbox_status,
        parser_status=parser_status,
        parser_verified=parser_verified,
        data_source=data_source,
        data_usable=data_usable,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=effective_skip_inline,
        worker_apply_enabled=worker_apply_enabled,
        global_kill_switch=global_kill_switch,
        legacy_effective_skip_guard=legacy_effective_skip_guard,
        skip_budget_limit=skip_budget_limit,
        skip_budget_used=skip_budget_used,
        skip_budget_remaining=skip_budget_remaining,
        outbox_pending_count=outbox_counts["pending_count"],
        outbox_processing_count=outbox_counts["processing_count"],
        outbox_error_count=outbox_counts["error_count"],
        outbox_dead_letter_count=outbox_counts["dead_letter_count"],
        regime_continuity_required=regime_continuity_required,
        regime_continuity_ready=regime_continuity_ready,
        rollback_required=bool(rollback_reason_codes),
        rollback_reason_codes=tuple(rollback_reason_codes),
        controller_status=controller_status,
        observe_safe=observe_safe,
        gateway_health_required=gateway_health_required,
        gateway_health_ready=bool(gateway_health["ready"]),
        blocked_reason_codes=tuple(dict.fromkeys(reasons)),
        evidence=evidence,
        decided_at=decided_at,
    )
    _persist_decision(connection, decision)
    return decision


def get_latest_market_index_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    rows = list_market_index_append_only_routing_decisions(connection, limit=500)
    latest_reconcile = get_latest_market_index_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    latest_status = _mapping_string(latest_run, "status")
    latest_created_at = _mapping_string(latest_run, "created_at")
    latest_age_sec = _age_seconds(latest_created_at)
    append_only_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    effective_count = _count_decisions(connection, "effective_skip_inline")
    would_count = _count_decisions(connection, "would_skip_inline")
    outbox_counts = _outbox_counts(connection)
    effective_skip_health = _effective_skip_health(connection)
    gateway_health = _gateway_market_index_health(
        connection,
        max_age_sec=int(
            resolved_settings.gateway_market_index_append_only_gateway_health_max_age_sec
        ),
        max_future_skew_sec=int(
            resolved_settings.gateway_market_index_append_only_max_future_skew_sec
        ),
    )
    rollback_reason_codes = _rollback_reason_codes(
        outbox_counts=outbox_counts,
        effective_skip_health=effective_skip_health,
        settings=resolved_settings,
    )
    skip_budget_limit = int(
        resolved_settings.gateway_market_index_append_only_max_skip_per_minute
    )
    skip_budget_used = _skip_budget_used(connection)
    skip_budget_remaining = max(skip_budget_limit - skip_budget_used, 0)
    controller_status = _controller_status(
        dry_run_enabled=bool(
            resolved_settings.gateway_market_index_append_only_dry_run_enabled
        ),
        observe_safe=_is_observe_safe(resolved_settings),
        gateway_health_required=bool(
            resolved_settings.gateway_market_index_append_only_require_fresh_gateway_health
        ),
        gateway_health_ready=bool(gateway_health["ready"]),
        cutover_enabled=bool(
            resolved_settings.gateway_market_index_append_only_cutover_enabled
        ),
        global_kill_switch=bool(
            resolved_settings.gateway_market_index_append_only_global_kill_switch
        ),
        legacy_effective_skip_guard=bool(
            resolved_settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15
        ),
        skip_budget_limit=skip_budget_limit,
        worker_apply_enabled=bool(
            resolved_settings.projection_outbox_apply_projection_enabled
            and resolved_settings.projection_outbox_market_index_apply_enabled
        ),
        latest_status=latest_status,
        append_only_ready=append_only_ready,
        latest_age_sec=latest_age_sec,
        parser_verified_required=bool(
            resolved_settings.gateway_market_index_append_only_require_parser_verified
        ),
        reconcile_required=bool(
            resolved_settings.gateway_market_index_append_only_require_reconcile_pass
        ),
        data_usable_required=bool(
            resolved_settings.gateway_market_index_append_only_require_data_usable
        ),
        regime_continuity_required=bool(
            resolved_settings.gateway_market_index_append_only_require_worker_regime_refresh
        ),
        regime_refresh_fail_closed=bool(
            resolved_settings.gateway_market_index_append_only_fail_closed_on_regime_refresh_error
        ),
        rollback_reason_codes=rollback_reason_codes,
        settings=resolved_settings,
    )
    failures = list(rollback_reason_codes)
    if isinstance(latest_run, Mapping) and latest_run.get("status") == "FAIL":
        failures.append("MARKET_INDEX_RECONCILE_FAIL")
    if effective_count > 0 and (
        not resolved_settings.gateway_market_index_append_only_cutover_enabled
        or resolved_settings.gateway_market_index_append_only_global_kill_switch
        or resolved_settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15
    ):
        failures.append("MARKET_INDEX_EFFECTIVE_SKIP_OUTSIDE_ENABLED_CUTOVER")
    warnings = [
        "PR-16 market_index limited cutover is feature-flagged",
        "rollback: enable global kill switch or disable market_index cutover",
        "TR bootstrap adapter is implemented but is not realtime cutover evidence",
        "NXT is not accepted as KRX market index evidence",
    ]
    if not resolved_settings.gateway_market_index_append_only_dry_run_enabled:
        warnings.append("market_index append-only dry-run disabled")
    if not resolved_settings.gateway_market_index_append_only_cutover_enabled:
        warnings.append("market_index limited cutover disabled")
    if resolved_settings.gateway_market_index_append_only_global_kill_switch:
        warnings.append("market_index global kill switch enabled")
    if resolved_settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15:
        warnings.append("legacy PR-15 effective-skip guard enabled")
    if skip_budget_limit <= 0:
        warnings.append("market_index skip budget disabled")
    return {
        "pr": "PR-16",
        "status": controller_status,
        "observe_safe": _is_observe_safe(resolved_settings),
        "gateway_health_required": bool(
            resolved_settings.gateway_market_index_append_only_require_fresh_gateway_health
        ),
        "gateway_health_ready": bool(gateway_health["ready"]),
        "gateway_health": gateway_health,
        "dry_run_enabled": bool(
            resolved_settings.gateway_market_index_append_only_dry_run_enabled
        ),
        "cutover_enabled": bool(
            resolved_settings.gateway_market_index_append_only_cutover_enabled
        ),
        "global_kill_switch": bool(
            resolved_settings.gateway_market_index_append_only_global_kill_switch
        ),
        "effective_skip_disabled_in_pr15": bool(
            resolved_settings.gateway_market_index_append_only_effective_skip_disabled_in_pr15
        ),
        "skip_budget_limit": skip_budget_limit,
        "skip_budget_used_current_minute": skip_budget_used,
        "skip_budget_remaining_current_minute": skip_budget_remaining,
        "reconcile_required": bool(
            resolved_settings.gateway_market_index_append_only_require_reconcile_pass
        ),
        "data_usable_required": bool(
            resolved_settings.gateway_market_index_append_only_require_data_usable
        ),
        "parser_verified_required": bool(
            resolved_settings.gateway_market_index_append_only_require_parser_verified
        ),
        "reconcile_max_age_sec": int(
            resolved_settings.gateway_market_index_append_only_reconcile_max_age_sec
        ),
        "worker_apply_enabled": bool(
            resolved_settings.projection_outbox_apply_projection_enabled
            and resolved_settings.projection_outbox_market_index_apply_enabled
        ),
        "total_decision_count": _count_all_decisions(connection),
        "would_skip_inline_count": would_count,
        "effective_skip_inline_count": effective_count,
        "blocked_reason_code_counts": _blocked_reason_counts(rows),
        "latest_decision": rows[0] if rows else None,
        "latest_reconcile": latest_reconcile,
        "latest_reconcile_status": (
            latest_run.get("status") if isinstance(latest_run, Mapping) else None
        ),
        "latest_reconcile_age_sec": latest_age_sec,
        "append_only_ready": append_only_ready,
        "outbox": outbox_counts,
        "effective_skip_health": effective_skip_health,
        "regime_continuity_required": bool(
            resolved_settings.gateway_market_index_append_only_require_worker_regime_refresh
        ),
        "regime_refresh_fail_closed": bool(
            resolved_settings.gateway_market_index_append_only_fail_closed_on_regime_refresh_error
        ),
        "regime_continuity_ready": not any(
            reason.startswith(("MARKET_INDEX_REGIME_", "MARKET_REGIME_"))
            for reason in rollback_reason_codes
        ),
        "rollback_required": bool(rollback_reason_codes),
        "rollback_reason_codes": list(rollback_reason_codes),
        "rollback_hint": (
            "set GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH=true "
            "or GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED=false"
        ),
        "effective_skip_allowed_now": bool(
            controller_status == "PASS" and skip_budget_remaining > 0
        ),
        "parser_confidence_separate_from_data_usability": True,
        "tr_bootstrap_adapter_status": "IMPLEMENTED",
        "tr_bootstrap_enabled": resolved_settings.market_index_tr_bootstrap_enabled,
        "tr_bootstrap_parser_status": (
            resolved_settings.market_index_tr_bootstrap_parser_status
        ),
        "failures": sorted(set(failures)),
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_index_append_only_routing_decisions(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM market_index_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT ?
        """,
        (min(max(int(limit), 1), 500),),
    ).fetchall()
    return [_row_to_dict(row) for row in rows]


def _persist_decision(
    connection: sqlite3.Connection,
    decision: MarketIndexAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_index_projection_routing_decisions (
            event_id, event_type, projection_name, dry_run_enabled, cutover_enabled,
            reconcile_required, data_usable_required, parser_verified_required,
            latest_reconcile_run_id, latest_reconcile_status,
            latest_reconcile_created_at, latest_reconcile_age_sec, append_only_ready,
            outbox_status, outbox_job_present, parser_status, parser_verified,
            data_source, data_usable, would_skip_inline, effective_skip_inline,
            worker_apply_enabled, blocked_reason_codes_json, evidence_json, decided_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, projection_name) DO UPDATE SET
            event_type = excluded.event_type,
            dry_run_enabled = excluded.dry_run_enabled,
            cutover_enabled = excluded.cutover_enabled,
            reconcile_required = excluded.reconcile_required,
            data_usable_required = excluded.data_usable_required,
            parser_verified_required = excluded.parser_verified_required,
            latest_reconcile_run_id = excluded.latest_reconcile_run_id,
            latest_reconcile_status = excluded.latest_reconcile_status,
            latest_reconcile_created_at = excluded.latest_reconcile_created_at,
            latest_reconcile_age_sec = excluded.latest_reconcile_age_sec,
            append_only_ready = excluded.append_only_ready,
            outbox_status = excluded.outbox_status,
            outbox_job_present = excluded.outbox_job_present,
            parser_status = excluded.parser_status,
            parser_verified = excluded.parser_verified,
            data_source = excluded.data_source,
            data_usable = excluded.data_usable,
            would_skip_inline = excluded.would_skip_inline,
            effective_skip_inline = excluded.effective_skip_inline,
            worker_apply_enabled = excluded.worker_apply_enabled,
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
            int(decision.data_usable_required),
            int(decision.parser_verified_required),
            decision.latest_reconcile_run_id,
            decision.latest_reconcile_status,
            decision.latest_reconcile_created_at,
            decision.latest_reconcile_age_sec,
            int(decision.append_only_ready),
            decision.outbox_status,
            int(decision.outbox_job_present),
            decision.parser_status,
            int(decision.parser_verified),
            decision.data_source,
            int(decision.data_usable),
            int(decision.would_skip_inline),
            int(decision.effective_skip_inline),
            int(decision.worker_apply_enabled),
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


def _outbox_counts(connection: sqlite3.Connection) -> dict[str, int]:
    counts = {
        "job_count": 0,
        "pending_count": 0,
        "processing_count": 0,
        "applied_count": 0,
        "skipped_count": 0,
        "error_count": 0,
        "dead_letter_count": 0,
        "regime_job_count": 0,
        "regime_pending_count": 0,
        "regime_processing_count": 0,
        "regime_applied_count": 0,
        "regime_skipped_count": 0,
        "regime_error_count": 0,
        "regime_dead_letter_count": 0,
    }
    rows = connection.execute(
        """
        SELECT projection_name, status, COUNT(*) AS count
        FROM projection_outbox
        WHERE projection_name IN ('market_index', 'market_regime')
        GROUP BY projection_name, status
        """
    ).fetchall()
    for row in rows:
        projection_name = str(row["projection_name"])
        status = str(row["status"]).lower()
        count = int(row["count"])
        prefix = "regime_" if projection_name == "market_regime" else ""
        counts[f"{prefix}job_count"] += count
        key = f"{prefix}{status}_count"
        if key in counts:
            counts[key] = count
    return counts


def _effective_skip_health(connection: sqlite3.Connection) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS effective_skip_count,
            SUM(
                CASE
                    WHEN index_outbox.outbox_id IS NULL
                        OR index_outbox.status IN ('PENDING', 'PROCESSING')
                        OR regime_outbox.outbox_id IS NULL
                        OR regime_outbox.status IN ('PENDING', 'PROCESSING')
                    THEN 1 ELSE 0
                END
            ) AS pending_worker_count,
            SUM(
                CASE
                    WHEN index_outbox.status IN ('ERROR', 'DEAD_LETTER', 'SKIPPED')
                        OR regime_outbox.status IN ('ERROR', 'DEAD_LETTER', 'SKIPPED')
                    THEN 1 ELSE 0
                END
            ) AS worker_error_count,
            SUM(
                CASE
                    WHEN index_outbox.status = 'APPLIED'
                        AND COALESCE(
                            json_extract(
                                index_outbox.metadata_json,
                                '$.last_worker_evidence.apply_result'
                            ),
                            ''
                        ) != 'APPLIED_BY_WORKER'
                    THEN 1 ELSE 0
                END
            ) AS worker_apply_evidence_missing_count,
            SUM(
                CASE WHEN NOT EXISTS (
                    SELECT 1 FROM market_index_tick_samples AS sample
                    WHERE sample.event_id = decision.event_id
                ) THEN 1 ELSE 0 END
            ) AS artifact_missing_count,
            SUM(
                CASE WHEN NOT EXISTS (
                    SELECT 1 FROM market_regime_snapshots AS snapshot
                    WHERE snapshot.source_event_id = decision.event_id
                ) THEN 1 ELSE 0 END
            ) AS regime_snapshot_missing_count,
            SUM(
                CASE
                    WHEN regime_outbox.outbox_id IS NULL
                        OR regime_outbox.status != 'APPLIED'
                    THEN 1 ELSE 0
                END
            ) AS regime_outbox_not_applied_count
        FROM market_index_projection_routing_decisions AS decision
        LEFT JOIN projection_outbox AS index_outbox
            ON index_outbox.event_id = decision.event_id
            AND index_outbox.projection_name = 'market_index'
        LEFT JOIN projection_outbox AS regime_outbox
            ON regime_outbox.event_id = decision.event_id
            AND regime_outbox.projection_name = 'market_regime'
        WHERE decision.effective_skip_inline = 1
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
        "regime_snapshot_missing_count": int(
            row["regime_snapshot_missing_count"] or 0
        ),
        "regime_outbox_not_applied_count": int(
            row["regime_outbox_not_applied_count"] or 0
        ),
    }


def _preapplied_event_continuity(
    connection: sqlite3.Connection,
    *,
    event_id: str,
) -> dict[str, Any]:
    index_job = connection.execute(
        """
        SELECT status, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_index' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    regime_job = connection.execute(
        """
        SELECT status
        FROM projection_outbox
        WHERE projection_name = 'market_regime' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    index_status = _normalize_outbox_status(
        None if index_job is None else index_job["status"]
    )
    regime_status = _normalize_outbox_status(
        None if regime_job is None else regime_job["status"]
    )
    metadata = _json_object(None if index_job is None else index_job["metadata_json"])
    worker_evidence = metadata.get("last_worker_evidence")
    apply_result = (
        str(worker_evidence.get("apply_result") or "").strip()
        if isinstance(worker_evidence, Mapping)
        else ""
    )
    artifact_present = connection.execute(
        "SELECT 1 FROM market_index_tick_samples WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone() is not None
    regime_snapshot_present = connection.execute(
        """
        SELECT 1
        FROM market_regime_snapshots
        WHERE source_event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone() is not None
    ready = bool(
        index_status == "APPLIED"
        and apply_result == "APPLIED_BY_WORKER"
        and artifact_present
        and regime_status == "APPLIED"
        and regime_snapshot_present
    )
    return {
        "required": index_status == "APPLIED",
        "ready": ready,
        "index_outbox_status": index_status,
        "index_apply_result": apply_result or None,
        "index_artifact_present": artifact_present,
        "regime_outbox_status": regime_status,
        "regime_snapshot_present": regime_snapshot_present,
    }


def _rollback_reason_codes(
    *,
    outbox_counts: Mapping[str, int],
    effective_skip_health: Mapping[str, int],
    settings: Settings,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if int(outbox_counts.get("error_count") or 0) > 0:
        reasons.append("MARKET_INDEX_OUTBOX_ERROR")
    if int(outbox_counts.get("dead_letter_count") or 0) > 0:
        reasons.append("MARKET_INDEX_OUTBOX_DEAD_LETTER")
    if int(outbox_counts.get("regime_error_count") or 0) > 0:
        reasons.append("MARKET_REGIME_OUTBOX_ERROR")
    if int(outbox_counts.get("regime_dead_letter_count") or 0) > 0:
        reasons.append("MARKET_REGIME_OUTBOX_DEAD_LETTER")
    if int(outbox_counts.get("processing_count") or 0) > 0:
        reasons.append("MARKET_INDEX_OUTBOX_PROCESSING_ACTIVE")
    if int(outbox_counts.get("pending_count") or 0) > int(
        settings.gateway_market_index_append_only_max_pending_within_sla
    ):
        reasons.append("MARKET_INDEX_OUTBOX_PENDING_EXCEEDED")
    if int(outbox_counts.get("regime_processing_count") or 0) > 0:
        reasons.append("MARKET_REGIME_OUTBOX_PROCESSING_ACTIVE")
    if int(outbox_counts.get("regime_pending_count") or 0) > int(
        settings.gateway_market_index_append_only_max_pending_within_sla
    ):
        reasons.append("MARKET_REGIME_OUTBOX_PENDING_EXCEEDED")
    if int(effective_skip_health.get("pending_worker_count") or 0) > 0:
        reasons.append("MARKET_INDEX_EFFECTIVE_SKIP_PENDING_WORKER")
    if int(effective_skip_health.get("worker_error_count") or 0) > 0:
        reasons.append("MARKET_INDEX_EFFECTIVE_SKIP_WORKER_ERROR")
    if int(
        effective_skip_health.get("worker_apply_evidence_missing_count") or 0
    ) > 0:
        reasons.append("MARKET_INDEX_EFFECTIVE_SKIP_WORKER_EVIDENCE_MISSING")
    if int(effective_skip_health.get("artifact_missing_count") or 0) > 0:
        reasons.append("MARKET_INDEX_EFFECTIVE_SKIP_ARTIFACT_MISSING")
    if int(effective_skip_health.get("regime_snapshot_missing_count") or 0) > 0:
        reasons.append("MARKET_INDEX_REGIME_SNAPSHOT_MISSING")
    if int(effective_skip_health.get("regime_outbox_not_applied_count") or 0) > 0:
        reasons.append("MARKET_INDEX_REGIME_OUTBOX_NOT_APPLIED")
    return tuple(sorted(set(reasons)))


def _controller_status(
    *,
    dry_run_enabled: bool,
    observe_safe: bool,
    gateway_health_required: bool,
    gateway_health_ready: bool,
    cutover_enabled: bool,
    global_kill_switch: bool,
    legacy_effective_skip_guard: bool,
    skip_budget_limit: int,
    worker_apply_enabled: bool,
    latest_status: str | None,
    append_only_ready: bool,
    latest_age_sec: float | None,
    parser_verified_required: bool,
    reconcile_required: bool,
    data_usable_required: bool,
    regime_continuity_required: bool,
    regime_refresh_fail_closed: bool,
    rollback_reason_codes: Sequence[str],
    settings: Settings,
) -> str:
    if rollback_reason_codes:
        return "FAIL"
    gates_ready = bool(
        dry_run_enabled
        and observe_safe
        and gateway_health_required
        and gateway_health_ready
        and cutover_enabled
        and not global_kill_switch
        and not legacy_effective_skip_guard
        and skip_budget_limit > 0
        and worker_apply_enabled
        and latest_status == "PASS"
        and append_only_ready
        and latest_age_sec is not None
        and latest_age_sec
        <= settings.gateway_market_index_append_only_reconcile_max_age_sec
        and reconcile_required
        and data_usable_required
        and parser_verified_required
        and regime_continuity_required
        and regime_refresh_fail_closed
        and settings.market_regime_enabled
    )
    if gates_ready:
        return "PASS"
    active_cutover_requested = bool(
        cutover_enabled and not global_kill_switch and not legacy_effective_skip_guard
    )
    return "FAIL" if active_cutover_requested else "WARN"


def _skip_budget_used(connection: sqlite3.Connection) -> int:
    minute_bucket = _budget_minute_bucket()
    row = connection.execute(
        """
        SELECT used_count
        FROM market_index_append_only_budget_state
        WHERE budget_name = 'market_index_global' AND minute_bucket = ?
        """,
        (minute_bucket,),
    ).fetchone()
    return 0 if row is None else int(row["used_count"])


def _is_observe_safe(settings: Settings) -> bool:
    return bool(
        settings.trading_profile == TradingProfile.OBSERVE
        and settings.trading_mode == TradingMode.OBSERVE
        and not settings.trading_allow_live_sim
        and not settings.trading_allow_live_real
    )


def _gateway_market_index_health(
    connection: sqlite3.Connection,
    *,
    max_age_sec: int,
    max_future_skew_sec: int,
) -> dict[str, Any]:
    keys = (
        "last_heartbeat_at",
        "market_index_realtime_enabled",
        "market_index_adapter_health",
        "parsed_market_index_tick_count",
        "latest_market_index_tick_at",
    )
    placeholders = ",".join("?" for _ in keys)
    values = {
        str(row["key"]): str(row["value"] or "")
        for row in connection.execute(
            f"SELECT key, value FROM gateway_status WHERE key IN ({placeholders})",
            keys,
        ).fetchall()
    }
    heartbeat_age_sec, heartbeat_future_skew_sec = _timestamp_age_and_future_skew(
        values.get("last_heartbeat_at")
    )
    latest_tick_age_sec, latest_tick_future_skew_sec = (
        _timestamp_age_and_future_skew(values.get("latest_market_index_tick_at"))
    )
    realtime_enabled = _status_bool(values.get("market_index_realtime_enabled"))
    adapter_health = str(values.get("market_index_adapter_health") or "UNKNOWN").upper()
    parsed_tick_count = _status_int(values.get("parsed_market_index_tick_count"))
    reason_codes: list[str] = []
    if heartbeat_age_sec is None:
        reason_codes.append("MARKET_INDEX_GATEWAY_HEARTBEAT_MISSING")
    elif heartbeat_age_sec > max_age_sec:
        reason_codes.append("MARKET_INDEX_GATEWAY_HEARTBEAT_STALE")
    if (
        heartbeat_future_skew_sec is not None
        and heartbeat_future_skew_sec > max_future_skew_sec
    ):
        reason_codes.append("MARKET_INDEX_GATEWAY_HEARTBEAT_FUTURE_SKEW")
    if not realtime_enabled:
        reason_codes.append("MARKET_INDEX_GATEWAY_REALTIME_DISABLED")
    if adapter_health != "CALLBACK_ACTIVE":
        reason_codes.append("MARKET_INDEX_GATEWAY_ADAPTER_NOT_CALLBACK_ACTIVE")
    if parsed_tick_count <= 0:
        reason_codes.append("MARKET_INDEX_GATEWAY_PARSED_TICK_MISSING")
    if latest_tick_age_sec is None:
        reason_codes.append("MARKET_INDEX_GATEWAY_LATEST_TICK_MISSING")
    elif latest_tick_age_sec > max_age_sec:
        reason_codes.append("MARKET_INDEX_GATEWAY_LATEST_TICK_STALE")
    if (
        latest_tick_future_skew_sec is not None
        and latest_tick_future_skew_sec > max_future_skew_sec
    ):
        reason_codes.append("MARKET_INDEX_GATEWAY_LATEST_TICK_FUTURE_SKEW")
    return {
        "ready": not reason_codes,
        "max_age_sec": int(max_age_sec),
        "max_future_skew_sec": int(max_future_skew_sec),
        "heartbeat_at": values.get("last_heartbeat_at") or None,
        "heartbeat_age_sec": heartbeat_age_sec,
        "heartbeat_future_skew_sec": heartbeat_future_skew_sec,
        "realtime_enabled": realtime_enabled,
        "adapter_health": adapter_health,
        "parsed_tick_count": parsed_tick_count,
        "latest_tick_at": values.get("latest_market_index_tick_at") or None,
        "latest_tick_age_sec": latest_tick_age_sec,
        "latest_tick_future_skew_sec": latest_tick_future_skew_sec,
        "reason_codes": reason_codes,
    }


def _status_bool(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _status_int(value: object) -> int:
    try:
        return int(str(value or "0").strip())
    except ValueError:
        return 0


def _timestamp_age_and_future_skew(
    value: object,
) -> tuple[float | None, float | None]:
    if value in (None, ""):
        return None, None
    try:
        parsed = parse_timestamp(str(value), "gateway_status_timestamp")
    except ValueError:
        return None, None
    delta_sec = (utc_now() - parsed).total_seconds()
    return max(delta_sec, 0.0), max(-delta_sec, 0.0)


def _reserve_skip_budget(
    connection: sqlite3.Connection,
    *,
    event_id: str,
    limit: int,
) -> int | None:
    if limit <= 0:
        return None
    minute_bucket = _budget_minute_bucket()
    cursor = connection.execute(
        """
        INSERT INTO market_index_append_only_budget_state (
            budget_name, minute_bucket, used_count, last_event_id, updated_at
        )
        VALUES ('market_index_global', ?, 1, ?, ?)
        ON CONFLICT(budget_name) DO UPDATE SET
            minute_bucket = excluded.minute_bucket,
            used_count = CASE
                WHEN market_index_append_only_budget_state.minute_bucket
                    = excluded.minute_bucket
                THEN market_index_append_only_budget_state.used_count + 1
                ELSE 1
            END,
            last_event_id = excluded.last_event_id,
            updated_at = excluded.updated_at
        WHERE market_index_append_only_budget_state.minute_bucket
                != excluded.minute_bucket
            OR market_index_append_only_budget_state.used_count < ?
        """,
        (minute_bucket, event_id, datetime_to_wire(utc_now()), limit),
    )
    connection.commit()
    if cursor.rowcount != 1:
        return None
    return _skip_budget_used(connection)


def _budget_minute_bucket() -> str:
    return datetime_to_wire(utc_now().replace(second=0, microsecond=0))


def _outbox_job(connection: sqlite3.Connection, event_id: str) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, created_at, updated_at, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_index' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _gateway_event_status(connection: sqlite3.Connection, event_id: str) -> str | None:
    row = connection.execute(
        "SELECT status FROM gateway_events WHERE event_id = ? LIMIT 1",
        (event_id,),
    ).fetchone()
    return None if row is None else str(row["status"])


def _normalize_outbox_status(value: object) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _mapping_string(value: object, key: str) -> str | None:
    if not isinstance(value, Mapping):
        return None
    text = str(value.get(key) or "").strip()
    return text or None


def _age_seconds(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        parsed = parse_timestamp(str(value), "timestamp")
    except ValueError:
        return None
    return max((utc_now() - parsed).total_seconds(), 0.0)


def _usability_timestamp(
    usability: Mapping[str, Any],
    key: str,
    *,
    fallback: datetime,
) -> datetime:
    value = usability.get(key)
    if value in (None, ""):
        return fallback
    try:
        return parse_timestamp(str(value), key)
    except ValueError:
        return fallback


def _count_all_decisions(connection: sqlite3.Connection) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS count FROM market_index_projection_routing_decisions"
        ).fetchone()["count"]
    )


def _count_decisions(connection: sqlite3.Connection, field_name: str) -> int:
    if field_name not in {"would_skip_inline", "effective_skip_inline"}:
        raise ValueError("unsupported decision field")
    return int(
        connection.execute(
            f"SELECT COUNT(*) AS count FROM market_index_projection_routing_decisions "
            f"WHERE {field_name} = 1"
        ).fetchone()["count"]
    )


def _blocked_reason_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        for reason in row.get("blocked_reason_codes") or []:
            counts[str(reason)] += 1
    return dict(sorted(counts.items()))


def _row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key in (
        "dry_run_enabled",
        "cutover_enabled",
        "reconcile_required",
        "data_usable_required",
        "parser_verified_required",
        "append_only_ready",
        "outbox_job_present",
        "parser_verified",
        "data_usable",
        "would_skip_inline",
        "effective_skip_inline",
        "worker_apply_enabled",
    ):
        payload[key] = bool(payload[key])
    payload["blocked_reason_codes"] = _json_array(
        payload.pop("blocked_reason_codes_json", "[]")
    )
    payload["evidence"] = _json_object(payload.pop("evidence_json", "{}"))
    return payload


def _json_array(value: object) -> list[Any]:
    try:
        loaded = json.loads(str(value or "[]"))
    except json.JSONDecodeError:
        return []
    return list(loaded) if isinstance(loaded, list) else []


def _json_object(value: object) -> dict[str, Any]:
    try:
        loaded = json.loads(str(value or "{}"))
    except json.JSONDecodeError:
        return {}
    return dict(loaded) if isinstance(loaded, Mapping) else {}
