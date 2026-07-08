from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import (
    datetime_to_wire,
    parse_timestamp,
    utc_now,
    validate_stock_code,
)
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.market_data_service import (
    MARKET_DATA_EVENT_TYPES,
    normalize_market_data_exchange,
)
from services.runtime.market_data_projection_reconcile import (
    get_latest_market_data_projection_reconcile,
)

PROJECTION_NAME_MARKET_DATA = "market_data"
PR6_EFFECTIVE_SKIP_DISABLED_REASON = "EFFECTIVE_SKIP_DISABLED_IN_PR6"
PR7_CUTOVER_SCOPE = "price_tick_only"
PR9_CUTOVER_SCOPE = "price_tick_and_tr_response"
PR7_ALLOWED_CUTOVER_EVENT_TYPE = "price_tick"
PR7_FORCED_INLINE_EVENT_TYPES = frozenset({"condition_event"})


@dataclass(frozen=True, kw_only=True)
class MarketDataAppendOnlyRoutingDecision:
    event_id: str
    event_type: str
    dry_run_enabled: bool
    cutover_enabled: bool
    reconcile_required: bool
    latest_reconcile_status: str | None
    latest_reconcile_run_id: str | None
    latest_reconcile_created_at: str | None
    latest_reconcile_age_sec: float | None
    append_only_ready: bool
    outbox_status: str | None
    outbox_job_present: bool
    would_skip_inline: bool
    effective_skip_inline: bool
    cutover_scope: str | None
    skip_budget_limit: int | None
    skip_budget_used: int | None
    skip_budget_remaining: int | None
    worker_apply_enabled: bool
    fallback_inline_projection_expected: bool
    blocked_reason_codes: Sequence[str]
    decided_at: str
    tr_response_rows_count: int | None = None
    tr_response_skip_budget_limit: int | None = None
    tr_response_skip_budget_used: int | None = None
    tr_response_skip_budget_remaining: int | None = None
    synthetic_child_guard_status: str | None = None
    worker_side_effect_ready: bool | None = None
    deferred_side_effect_required: bool | None = None
    projection_name: str = PROJECTION_NAME_MARKET_DATA
    post_apply_deferred_side_effects: Mapping[str, Any] = field(default_factory=dict)
    evidence: Mapping[str, Any] = field(default_factory=dict)

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
            "outbox_status": self.outbox_status,
            "outbox_job_present": self.outbox_job_present,
            "would_skip_inline": self.would_skip_inline,
            "effective_skip_inline": self.effective_skip_inline,
            "cutover_scope": self.cutover_scope,
            "skip_budget_limit": self.skip_budget_limit,
            "skip_budget_used": self.skip_budget_used,
            "skip_budget_remaining": self.skip_budget_remaining,
            "worker_apply_enabled": self.worker_apply_enabled,
            "fallback_inline_projection_expected": (
                self.fallback_inline_projection_expected
            ),
            "tr_response_rows_count": self.tr_response_rows_count,
            "tr_response_skip_budget_limit": self.tr_response_skip_budget_limit,
            "tr_response_skip_budget_used": self.tr_response_skip_budget_used,
            "tr_response_skip_budget_remaining": (
                self.tr_response_skip_budget_remaining
            ),
            "synthetic_child_guard_status": self.synthetic_child_guard_status,
            "worker_side_effect_ready": self.worker_side_effect_ready,
            "deferred_side_effect_required": self.deferred_side_effect_required,
            "post_apply_deferred_side_effects": dict(
                self.post_apply_deferred_side_effects
            ),
            "blocked_reason_codes": list(self.blocked_reason_codes),
            "evidence": dict(self.evidence),
            "decided_at": self.decided_at,
            "read_only": True,
            "no_trading_side_effects": True,
        }


def decide_market_data_projection_routing(
    connection: sqlite3.Connection,
    event: GatewayEvent,
    *,
    settings: Settings,
    outbox_status: str | None = None,
) -> MarketDataAppendOnlyRoutingDecision:
    event_type = event.event_type.strip().lower()
    tr_response_dry_run_enabled = bool(
        settings.gateway_market_data_append_only_tr_response_dry_run_enabled
    )
    tr_response_cutover_enabled = bool(
        settings.gateway_market_data_append_only_tr_response_cutover_enabled
    )
    tr_response_require_worker_side_effects = bool(
        settings.gateway_market_data_append_only_tr_response_require_worker_side_effects
    )
    tr_response_skip_budget_limit = int(
        settings.gateway_market_data_append_only_tr_response_max_skip_per_minute
    )
    tr_response_max_rows_per_event = int(
        settings.gateway_market_data_append_only_tr_response_max_rows_per_event
    )
    tr_response_require_synthetic_child_guard = bool(
        settings.gateway_market_data_append_only_tr_response_require_synthetic_child_guard
    )
    dry_run_enabled = bool(settings.gateway_market_data_append_only_dry_run_enabled)
    if event_type == "tr_response":
        dry_run_enabled = dry_run_enabled or tr_response_dry_run_enabled
    cutover_enabled = bool(settings.gateway_market_data_append_only_cutover_enabled)
    price_tick_cutover_enabled = bool(
        settings.gateway_market_data_append_only_price_tick_cutover_enabled
    )
    reconcile_required = bool(
        settings.gateway_market_data_append_only_require_reconcile_pass
    )
    latest_reconcile_required = bool(
        settings.gateway_market_data_append_only_require_latest_reconcile_pass
    )
    worker_apply_required = bool(
        settings.gateway_market_data_append_only_require_worker_apply_enabled
    )
    fail_closed_on_routing_error = bool(
        settings.gateway_market_data_append_only_fail_closed_on_routing_error
    )
    decided_at = datetime_to_wire(utc_now())
    latest_reconcile = get_latest_market_data_projection_reconcile(connection)
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
    outbox_job = _market_data_outbox_job(connection, event.event_id)
    outbox_job_present = outbox_job is not None
    normalized_outbox_status = _normalize_outbox_status(
        outbox_status or (outbox_job["status"] if outbox_job is not None else None)
    )
    source_event_status = _gateway_event_status(connection, event.event_id)
    allowed_event_types = {
        str(value).strip().lower()
        for value in settings.gateway_market_data_append_only_event_types
    }
    cutover_event_types = {
        str(value).strip().lower()
        for value in settings.gateway_market_data_append_only_cutover_event_types
    }
    worker_apply_enabled = bool(
        settings.projection_outbox_apply_projection_enabled
        and settings.projection_outbox_market_data_apply_enabled
    )
    tr_response_worker_side_effect_ready = (
        worker_apply_enabled if tr_response_require_worker_side_effects else True
    )
    tr_response_rows_count = (
        len(_tr_response_rows(event.payload)) if event_type == "tr_response" else None
    )
    tr_response_skip_budget_used = (
        _tr_response_skip_count_in_current_minute(connection)
        if event_type == "tr_response"
        else None
    )
    tr_response_skip_budget_remaining = (
        _tr_response_skip_budget_remaining(connection, settings=settings)
        if event_type == "tr_response"
        else None
    )
    synthetic_child_issue_count = (
        int(latest_run.get("synthetic_child_event_issue_count") or 0)
        if isinstance(latest_run, Mapping)
        else None
    )
    synthetic_child_guard_status = _synthetic_child_guard_status(
        required=tr_response_require_synthetic_child_guard,
        latest_status=latest_status,
        append_only_ready=append_only_ready,
        synthetic_child_issue_count=synthetic_child_issue_count,
    )
    skip_budget_limit = int(
        settings.gateway_market_data_append_only_price_tick_max_skip_per_minute
    )
    skip_budget_used = _price_tick_skip_count_in_current_minute(connection)
    skip_budget_remaining = _price_tick_skip_budget_remaining(
        connection,
        settings=settings,
    )

    reason_codes: list[str] = []
    would_skip_inline = False
    if event_type not in MARKET_DATA_EVENT_TYPES:
        reason_codes.append("NOT_MARKET_DATA_EVENT")
    elif not dry_run_enabled:
        reason_codes.append("DRY_RUN_DISABLED")
    elif event_type not in allowed_event_types:
        reason_codes.append("EVENT_TYPE_NOT_ALLOWED_FOR_APPEND_ONLY")
    elif not _outbox_is_ready(
        outbox_status=normalized_outbox_status,
        outbox_job_present=outbox_job_present,
        min_outbox_status=settings.gateway_market_data_append_only_min_outbox_status,
    ):
        reason_codes.append("MARKET_DATA_OUTBOX_JOB_MISSING_OR_NOT_ENQUEUED")
    elif reconcile_required and latest_run is None:
        reason_codes.append("MARKET_DATA_RECONCILE_MISSING")
    elif reconcile_required and (latest_status != "PASS" or not append_only_ready):
        reason_codes.append("MARKET_DATA_RECONCILE_NOT_PASS")
    elif (
        reconcile_required
        and latest_age_sec is not None
        and latest_age_sec > settings.gateway_market_data_append_only_reconcile_max_age_sec
    ):
        reason_codes.append("MARKET_DATA_RECONCILE_STALE")
    else:
        would_skip_inline = True
        reason_codes.append("DRY_RUN_WOULD_SKIP_INLINE")

    effective_skip_inline = False
    price_tick_identity = _price_tick_payload_identity(event.payload)
    effective_skip_reason = "FALLBACK_INLINE_PROJECTION"
    if event_type == "tr_response" and would_skip_inline:
        if not settings.gateway_market_data_append_only_dry_run_enabled:
            reason_codes.append("DRY_RUN_DISABLED")
            effective_skip_reason = "DRY_RUN_DISABLED"
        elif not cutover_enabled or not tr_response_cutover_enabled:
            reason_codes.append("TR_RESPONSE_CUTOVER_DISABLED")
            effective_skip_reason = "TR_RESPONSE_CUTOVER_DISABLED"
        elif worker_apply_required and not worker_apply_enabled:
            reason_codes.append("WORKER_APPLY_NOT_ENABLED")
            effective_skip_reason = "WORKER_APPLY_NOT_ENABLED"
        elif not worker_apply_enabled:
            reason_codes.append("WORKER_APPLY_NOT_ENABLED")
            effective_skip_reason = "WORKER_APPLY_NOT_ENABLED"
        elif latest_reconcile_required and latest_run is None:
            reason_codes.append("MARKET_DATA_RECONCILE_MISSING")
            effective_skip_reason = "MARKET_DATA_RECONCILE_MISSING"
        elif latest_reconcile_required and latest_status != "PASS":
            reason_codes.append("MARKET_DATA_RECONCILE_NOT_PASS")
            effective_skip_reason = "MARKET_DATA_RECONCILE_NOT_PASS"
        elif latest_reconcile_required and not append_only_ready:
            reason_codes.append("MARKET_DATA_RECONCILE_NOT_PASS")
            effective_skip_reason = "MARKET_DATA_RECONCILE_NOT_PASS"
        elif (
            latest_reconcile_required
            and latest_age_sec is not None
            and latest_age_sec
            > settings.gateway_market_data_append_only_reconcile_max_age_sec
        ):
            reason_codes.append("MARKET_DATA_RECONCILE_STALE")
            effective_skip_reason = "MARKET_DATA_RECONCILE_STALE"
        elif not _outbox_is_ready(
            outbox_status=normalized_outbox_status,
            outbox_job_present=outbox_job_present,
            min_outbox_status=settings.gateway_market_data_append_only_min_outbox_status,
        ):
            reason_codes.append("MARKET_DATA_OUTBOX_NOT_READY")
            effective_skip_reason = "MARKET_DATA_OUTBOX_NOT_READY"
        elif source_event_status != "ACCEPTED":
            reason_codes.append("SOURCE_GATEWAY_EVENT_NOT_ACCEPTED")
            effective_skip_reason = "SOURCE_GATEWAY_EVENT_NOT_ACCEPTED"
        elif tr_response_rows_count is None or tr_response_rows_count <= 0:
            reason_codes.append("TR_RESPONSE_ROWS_EMPTY")
            effective_skip_reason = "TR_RESPONSE_ROWS_EMPTY"
        elif tr_response_rows_count > tr_response_max_rows_per_event:
            reason_codes.append("TR_RESPONSE_TOO_MANY_ROWS")
            effective_skip_reason = "TR_RESPONSE_TOO_MANY_ROWS"
        elif (
            tr_response_require_synthetic_child_guard
            and synthetic_child_guard_status != "PASS"
        ):
            reason_codes.append("TR_RESPONSE_SYNTHETIC_CHILD_GUARD_NOT_READY")
            effective_skip_reason = "TR_RESPONSE_SYNTHETIC_CHILD_GUARD_NOT_READY"
        elif (
            tr_response_require_worker_side_effects
            and not tr_response_worker_side_effect_ready
        ):
            reason_codes.append("TR_RESPONSE_WORKER_SIDE_EFFECT_NOT_READY")
            effective_skip_reason = "TR_RESPONSE_WORKER_SIDE_EFFECT_NOT_READY"
        elif (
            tr_response_skip_budget_limit <= 0
            or (tr_response_skip_budget_remaining or 0) <= 0
        ):
            reason_codes.append("TR_RESPONSE_SKIP_BUDGET_EXHAUSTED")
            effective_skip_reason = "TR_RESPONSE_SKIP_BUDGET_EXHAUSTED"
        else:
            effective_skip_inline = True
            reason_codes.append("TR_RESPONSE_EFFECTIVE_SKIP_ALLOWED")
            effective_skip_reason = "TR_RESPONSE_EFFECTIVE_SKIP_ALLOWED"
        reason_codes.append(
            "TR_RESPONSE_WORKER_SIDE_EFFECT_READY"
            if tr_response_worker_side_effect_ready
            else "TR_RESPONSE_WORKER_SIDE_EFFECT_NOT_READY"
        )
    elif event_type == "condition_event" and would_skip_inline:
        reason_codes.append("CONDITION_EVENT_CUTOVER_DISABLED_IN_PR9")
        effective_skip_reason = "CONDITION_EVENT_CUTOVER_DISABLED_IN_PR9"
    elif event_type == PR7_ALLOWED_CUTOVER_EVENT_TYPE:
        if not would_skip_inline:
            effective_skip_reason = "WOULD_SKIP_INLINE_FALSE"
        elif not cutover_enabled or not price_tick_cutover_enabled:
            reason_codes.append("PRICE_TICK_CUTOVER_DISABLED")
            if cutover_enabled and not price_tick_cutover_enabled:
                reason_codes.append(PR6_EFFECTIVE_SKIP_DISABLED_REASON)
            effective_skip_reason = "PRICE_TICK_CUTOVER_DISABLED"
        elif PR7_ALLOWED_CUTOVER_EVENT_TYPE not in cutover_event_types:
            reason_codes.append("EVENT_TYPE_NOT_ENABLED_FOR_PR7_CUTOVER")
            effective_skip_reason = "EVENT_TYPE_NOT_ENABLED_FOR_PR7_CUTOVER"
        elif skip_budget_limit <= 0 or skip_budget_remaining <= 0:
            reason_codes.append("PRICE_TICK_SKIP_BUDGET_EXHAUSTED")
            effective_skip_reason = "PRICE_TICK_SKIP_BUDGET_EXHAUSTED"
        elif worker_apply_required and not worker_apply_enabled:
            reason_codes.append("WORKER_APPLY_NOT_ENABLED")
            effective_skip_reason = "WORKER_APPLY_NOT_ENABLED"
        elif not worker_apply_enabled:
            reason_codes.append("WORKER_APPLY_NOT_ENABLED")
            effective_skip_reason = "WORKER_APPLY_NOT_ENABLED"
        elif latest_reconcile_required and latest_run is None:
            reason_codes.append("MARKET_DATA_RECONCILE_MISSING")
            effective_skip_reason = "MARKET_DATA_RECONCILE_MISSING"
        elif latest_reconcile_required and latest_status != "PASS":
            reason_codes.append("MARKET_DATA_RECONCILE_NOT_PASS")
            effective_skip_reason = "MARKET_DATA_RECONCILE_NOT_PASS"
        elif latest_reconcile_required and not append_only_ready:
            reason_codes.append("MARKET_DATA_RECONCILE_NOT_PASS")
            effective_skip_reason = "MARKET_DATA_RECONCILE_NOT_PASS"
        elif (
            latest_reconcile_required
            and latest_age_sec is not None
            and latest_age_sec
            > settings.gateway_market_data_append_only_reconcile_max_age_sec
        ):
            reason_codes.append("MARKET_DATA_RECONCILE_STALE")
            effective_skip_reason = "MARKET_DATA_RECONCILE_STALE"
        elif not _outbox_is_ready(
            outbox_status=normalized_outbox_status,
            outbox_job_present=outbox_job_present,
            min_outbox_status=settings.gateway_market_data_append_only_min_outbox_status,
        ):
            reason_codes.append("MARKET_DATA_OUTBOX_NOT_READY")
            effective_skip_reason = "MARKET_DATA_OUTBOX_NOT_READY"
        elif source_event_status != "ACCEPTED":
            reason_codes.append("SOURCE_GATEWAY_EVENT_NOT_ACCEPTED")
            effective_skip_reason = "SOURCE_GATEWAY_EVENT_NOT_ACCEPTED"
        elif price_tick_identity is None:
            reason_codes.append("PRICE_TICK_PAYLOAD_INVALID")
            effective_skip_reason = "PRICE_TICK_PAYLOAD_INVALID"
        else:
            effective_skip_inline = True
            reason_codes.append("EFFECTIVE_SKIP_ALLOWED_PRICE_TICK")
            effective_skip_reason = "EFFECTIVE_SKIP_ALLOWED_PRICE_TICK"
    elif cutover_enabled and would_skip_inline:
        reason_codes.append("EVENT_TYPE_NOT_ENABLED_FOR_PR7_CUTOVER")
        reason_codes.append("EVENT_TYPE_NOT_ENABLED_FOR_PR8_CUTOVER")
        effective_skip_reason = "EVENT_TYPE_NOT_ENABLED_FOR_PR7_CUTOVER"

    if (
        fail_closed_on_routing_error
        and would_skip_inline
        and not effective_skip_inline
        and "EFFECTIVE_SKIP_ALLOWED_PRICE_TICK" not in reason_codes
        and "TR_RESPONSE_EFFECTIVE_SKIP_ALLOWED" not in reason_codes
    ):
        reason_codes.append("EFFECTIVE_SKIP_BLOCKED_FAIL_CLOSED")

    decision_cutover_scope = (
        PR7_CUTOVER_SCOPE if event_type == "price_tick" else PR9_CUTOVER_SCOPE
    )
    evidence = {
        "pr": "PR-9",
        "pr8_side_effect_migration": True,
        "cutover_scope": decision_cutover_scope,
        "price_tick_only_cutover": False,
        "tr_response_limited_cutover": True,
        "condition_event_cutover_enabled": False,
        "tr_response_dry_run_enabled": tr_response_dry_run_enabled,
        "tr_response_cutover_enabled": tr_response_cutover_enabled,
        "tr_response_worker_side_effect_ready": tr_response_worker_side_effect_ready,
        "tr_response_effective_skip_disabled_in_pr8": False,
        "tr_response_require_worker_side_effects": tr_response_require_worker_side_effects,
        "tr_response_worker_side_effect_required": tr_response_require_worker_side_effects,
        "tr_response_rows_count": tr_response_rows_count,
        "tr_response_max_rows_per_event": tr_response_max_rows_per_event,
        "synthetic_child_guard_required": tr_response_require_synthetic_child_guard,
        "synthetic_child_guard_status": synthetic_child_guard_status,
        "synthetic_child_event_issue_count": synthetic_child_issue_count,
        "inline_projection_remains_enabled_for_non_cutover_events": True,
        "fallback_inline_projection_expected": not effective_skip_inline,
        "price_tick_cutover_enabled": price_tick_cutover_enabled,
        "worker_apply_required": worker_apply_required,
        "worker_apply_enabled": worker_apply_enabled,
        "fail_closed_on_routing_error": fail_closed_on_routing_error,
        "skip_budget_limit_per_minute": skip_budget_limit,
        "skip_budget_used_per_minute": skip_budget_used,
        "skip_budget_remaining": skip_budget_remaining,
        "tr_response_skip_budget_limit_per_minute": tr_response_skip_budget_limit,
        "tr_response_skip_budget_used_per_minute": tr_response_skip_budget_used,
        "tr_response_skip_budget_remaining": tr_response_skip_budget_remaining,
        "effective_skip_reason": effective_skip_reason,
        "allowed_event_types": sorted(allowed_event_types),
        "cutover_event_types": sorted(cutover_event_types),
        "configured_min_outbox_status": (
            settings.gateway_market_data_append_only_min_outbox_status
        ),
        "outbox_enqueue_status": _normalize_outbox_status(outbox_status),
        "outbox_job_status": None if outbox_job is None else outbox_job["status"],
        "source_event_status": source_event_status,
        "price_tick_identity": price_tick_identity,
        "latest_reconcile": (
            None
            if not isinstance(latest_run, Mapping)
            else {
                "run_id": latest_run.get("run_id"),
                "status": latest_run.get("status"),
                "append_only_ready": bool(latest_run.get("append_only_ready")),
                "synthetic_child_event_issue_count": synthetic_child_issue_count,
                "created_at": latest_run.get("created_at"),
            }
        ),
        "no_trading_side_effects": True,
        "rollback_hint": (
            "Disable GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED "
            "or set GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE=0."
        ),
    }
    decision = MarketDataAppendOnlyRoutingDecision(
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
        outbox_status=normalized_outbox_status,
        outbox_job_present=outbox_job_present,
        would_skip_inline=would_skip_inline,
        effective_skip_inline=effective_skip_inline,
        cutover_scope=decision_cutover_scope,
        skip_budget_limit=skip_budget_limit,
        skip_budget_used=skip_budget_used,
        skip_budget_remaining=skip_budget_remaining,
        worker_apply_enabled=worker_apply_enabled,
        fallback_inline_projection_expected=not effective_skip_inline,
        tr_response_rows_count=tr_response_rows_count,
        tr_response_skip_budget_limit=tr_response_skip_budget_limit
        if event_type == "tr_response"
        else None,
        tr_response_skip_budget_used=tr_response_skip_budget_used,
        tr_response_skip_budget_remaining=tr_response_skip_budget_remaining,
        synthetic_child_guard_status=synthetic_child_guard_status
        if event_type == "tr_response"
        else None,
        worker_side_effect_ready=tr_response_worker_side_effect_ready
        if event_type == "tr_response"
        else None,
        deferred_side_effect_required=tr_response_require_worker_side_effects
        if event_type == "tr_response"
        else None,
        blocked_reason_codes=tuple(reason_codes),
        evidence=evidence,
        decided_at=decided_at,
    )
    _persist_market_data_projection_routing_decision(connection, decision)
    return decision


def get_latest_market_data_append_only_routing_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    rows = list_market_data_append_only_routing_decisions(connection, limit=500)
    total_count = _count_all_decisions(connection)
    would_skip_count = _count_decisions(connection, "would_skip_inline")
    effective_skip_count = _count_decisions(connection, "effective_skip_inline")
    effective_price_tick_skip_count = _count_effective_skip_by_event_type(
        connection,
        "price_tick",
    )
    condition_event_effective_skip_count = _count_effective_skip_by_event_type(
        connection,
        "condition_event",
    )
    tr_response_effective_skip_count = _count_effective_skip_by_event_type(
        connection,
        "tr_response",
    )
    tr_response_would_skip_inline_count = _count_would_skip_by_event_type(
        connection,
        "tr_response",
    )
    invalid_effective_skip_count = _count_invalid_effective_skips(connection)
    effective_skip_outbox_error_count = _count_effective_skip_outbox_errors(connection)
    deferred_incremental_enqueue_count = (
        _count_deferred_incremental_enqueue_records(connection)
    )
    tr_response_deferred_side_effect_count = (
        _count_tr_response_deferred_side_effect_records(connection)
    )
    tr_response_deferred_side_effect_error_count = (
        _count_tr_response_deferred_side_effect_errors(connection)
    )
    tr_response_duplicate_side_effect_count = (
        _count_tr_response_duplicate_side_effect_records(connection)
    )
    blocked_reason_counts = _blocked_reason_code_counts(rows)
    fail_closed_reason_counts = _fail_closed_reason_code_counts(rows)
    blocked_count = sum(
        1
        for row in rows
        if not row["would_skip_inline"]
        and "DRY_RUN_DISABLED" not in set(row["blocked_reason_codes"])
    )
    latest_reconcile = get_latest_market_data_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    append_only_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    worker_apply_enabled = bool(
        resolved_settings.projection_outbox_apply_projection_enabled
        and resolved_settings.projection_outbox_market_data_apply_enabled
    )
    tr_response_require_worker_side_effects = bool(
        resolved_settings.gateway_market_data_append_only_tr_response_require_worker_side_effects
    )
    tr_response_worker_side_effect_ready = (
        worker_apply_enabled if tr_response_require_worker_side_effects else True
    )
    skip_budget_limit = int(
        resolved_settings.gateway_market_data_append_only_price_tick_max_skip_per_minute
    )
    skip_budget_used = _price_tick_skip_count_in_current_minute(connection)
    skip_budget_remaining = _price_tick_skip_budget_remaining(
        connection,
        settings=resolved_settings,
    )
    tr_response_skip_budget_limit = int(
        resolved_settings.gateway_market_data_append_only_tr_response_max_skip_per_minute
    )
    tr_response_skip_budget_used = _tr_response_skip_count_in_current_minute(connection)
    tr_response_skip_budget_remaining = _tr_response_skip_budget_remaining(
        connection,
        settings=resolved_settings,
    )
    latest_status = (
        latest_run.get("status") if isinstance(latest_run, Mapping) else None
    )
    latest_synthetic_child_issue_count = (
        int(latest_run.get("synthetic_child_event_issue_count") or 0)
        if isinstance(latest_run, Mapping)
        else None
    )
    synthetic_child_guard_status = _synthetic_child_guard_status(
        required=bool(
            resolved_settings.gateway_market_data_append_only_tr_response_require_synthetic_child_guard
        ),
        latest_status=latest_status,
        append_only_ready=append_only_ready,
        synthetic_child_issue_count=latest_synthetic_child_issue_count,
    )
    tr_response_worker_applied_count = _count_tr_response_worker_applied_records(
        connection
    )
    tr_response_pending_worker_count = _count_tr_response_effective_skip_pending_worker(
        connection
    )
    failures: list[str] = []
    warnings = [
        "condition_event inline projection remains enabled",
        "PR-9 tr_response limited cutover requires strict flags and budget",
        "condition_event inline remains enabled",
        "LIVE_REAL/order behavior unchanged",
        "rollback: disable gateway_market_data_append_only_tr_response_cutover_enabled",
    ]
    if condition_event_effective_skip_count > 0:
        failures.append("CONDITION_EVENT_EFFECTIVE_SKIP_FORBIDDEN")
    if invalid_effective_skip_count > 0:
        failures.append("INVALID_EFFECTIVE_SKIP_EVENT_TYPE")
    if tr_response_effective_skip_count > 0 and not worker_apply_enabled:
        failures.append("TR_RESPONSE_EFFECTIVE_SKIP_WITH_WORKER_APPLY_DISABLED")
    if effective_price_tick_skip_count > 0 and not worker_apply_enabled:
        failures.append("PRICE_TICK_EFFECTIVE_SKIP_WITH_WORKER_APPLY_DISABLED")
    if effective_skip_outbox_error_count > 0:
        failures.append("EFFECTIVE_SKIP_OUTBOX_ERROR_OR_DEAD_LETTER")
    if tr_response_deferred_side_effect_error_count > 0:
        failures.append("TR_RESPONSE_DEFERRED_SIDE_EFFECT_ERROR")
    if tr_response_duplicate_side_effect_count > 0:
        failures.append("TR_RESPONSE_DUPLICATE_SIDE_EFFECT_FOR_INLINE_EVENT")
    if (
        resolved_settings.gateway_market_data_append_only_price_tick_cutover_enabled
        and skip_budget_limit <= 0
    ):
        warnings.append("price_tick cutover flag enabled but skip budget is 0")
    if (
        resolved_settings.gateway_market_data_append_only_tr_response_cutover_enabled
        and tr_response_skip_budget_limit <= 0
    ):
        warnings.append("tr_response cutover flag enabled but skip budget is 0")
    return {
        "dry_run_enabled": bool(
            resolved_settings.gateway_market_data_append_only_dry_run_enabled
        ),
        "cutover_enabled": bool(
            resolved_settings.gateway_market_data_append_only_cutover_enabled
        ),
        "price_tick_cutover_enabled": bool(
            resolved_settings.gateway_market_data_append_only_price_tick_cutover_enabled
        ),
        "tr_response_dry_run_enabled": bool(
            resolved_settings.gateway_market_data_append_only_tr_response_dry_run_enabled
        ),
        "tr_response_cutover_enabled": bool(
            resolved_settings.gateway_market_data_append_only_tr_response_cutover_enabled
        ),
        "tr_response_worker_side_effect_ready": tr_response_worker_side_effect_ready,
        "tr_response_require_worker_side_effects": tr_response_require_worker_side_effects,
        "cutover_scope": PR9_CUTOVER_SCOPE,
        "cutover_event_types": list(
            resolved_settings.gateway_market_data_append_only_cutover_event_types
        ),
        "skip_budget_limit_per_minute": skip_budget_limit,
        "skip_budget_used_current_minute": skip_budget_used,
        "skip_budget_remaining_current_minute": skip_budget_remaining,
        "tr_response_skip_budget_limit_per_minute": tr_response_skip_budget_limit,
        "tr_response_skip_budget_used_current_minute": tr_response_skip_budget_used,
        "tr_response_skip_budget_remaining_current_minute": (
            tr_response_skip_budget_remaining
        ),
        "reconcile_required": bool(
            resolved_settings.gateway_market_data_append_only_require_reconcile_pass
        ),
        "latest_reconcile_required": bool(
            resolved_settings.gateway_market_data_append_only_require_latest_reconcile_pass
        ),
        "reconcile_max_age_sec": int(
            resolved_settings.gateway_market_data_append_only_reconcile_max_age_sec
        ),
        "event_types": list(
            resolved_settings.gateway_market_data_append_only_event_types
        ),
        "min_outbox_status": (
            resolved_settings.gateway_market_data_append_only_min_outbox_status
        ),
        "total_decision_count": total_count,
        "would_skip_inline_count": would_skip_count,
        "effective_skip_inline_count": effective_skip_count,
        "effective_price_tick_skip_count": effective_price_tick_skip_count,
        "condition_event_effective_skip_count": condition_event_effective_skip_count,
        "tr_response_effective_skip_count": tr_response_effective_skip_count,
        "tr_response_would_skip_inline_count": tr_response_would_skip_inline_count,
        "tr_response_deferred_side_effect_count": tr_response_deferred_side_effect_count,
        "tr_response_deferred_side_effect_error_count": (
            tr_response_deferred_side_effect_error_count
        ),
        "tr_response_duplicate_side_effect_count": tr_response_duplicate_side_effect_count,
        "tr_response_pending_worker_count": tr_response_pending_worker_count,
        "tr_response_worker_applied_count": tr_response_worker_applied_count,
        "tr_response_deferred_quote_refresh_count": tr_response_deferred_side_effect_count,
        "tr_response_deferred_quote_refresh_error_count": (
            tr_response_deferred_side_effect_error_count
        ),
        "invalid_effective_skip_count": invalid_effective_skip_count,
        "effective_skip_outbox_error_count": effective_skip_outbox_error_count,
        "deferred_incremental_enqueue_count": deferred_incremental_enqueue_count,
        "blocked_count": blocked_count,
        "blocked_reason_code_counts": blocked_reason_counts,
        "fail_closed_reason_counts": fail_closed_reason_counts,
        "latest_decision": rows[0] if rows else None,
        "latest_reconcile": latest_reconcile,
        "latest_reconcile_status": (
            latest_run.get("status") if isinstance(latest_run, Mapping) else None
        ),
        "append_only_ready": append_only_ready,
        "worker_apply_enabled": worker_apply_enabled,
        "synthetic_child_guard_status": synthetic_child_guard_status,
        "worker_apply_required": bool(
            resolved_settings.gateway_market_data_append_only_require_worker_apply_enabled
        ),
        "fail_closed_on_routing_error": bool(
            resolved_settings.gateway_market_data_append_only_fail_closed_on_routing_error
        ),
        "rollback_hint": (
            "Set GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED=false, "
            "set GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE=0, "
            "or set GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=false."
        ),
        "failures": failures,
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def list_market_data_append_only_routing_decisions(
    connection: sqlite3.Connection,
    limit: int = 100,
) -> list[dict[str, Any]]:
    bounded_limit = min(max(int(limit), 1), 500)
    rows = connection.execute(
        """
        SELECT *
        FROM market_data_projection_routing_decisions
        ORDER BY decided_at DESC, id DESC
        LIMIT ?
        """,
        (bounded_limit,),
    ).fetchall()
    return [_routing_row_to_dict(row) for row in rows]


def _persist_market_data_projection_routing_decision(
    connection: sqlite3.Connection,
    decision: MarketDataAppendOnlyRoutingDecision,
) -> None:
    connection.execute(
        """
        INSERT INTO market_data_projection_routing_decisions (
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
            cutover_scope,
            skip_budget_limit,
            skip_budget_used,
            skip_budget_remaining,
            worker_apply_enabled,
            fallback_inline_projection_expected,
            tr_response_rows_count,
            tr_response_skip_budget_limit,
            tr_response_skip_budget_used,
            tr_response_skip_budget_remaining,
            synthetic_child_guard_status,
            worker_side_effect_ready,
            deferred_side_effect_required,
            post_apply_deferred_side_effects_json,
            blocked_reason_codes_json,
            evidence_json,
            decided_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            cutover_scope = excluded.cutover_scope,
            skip_budget_limit = excluded.skip_budget_limit,
            skip_budget_used = excluded.skip_budget_used,
            skip_budget_remaining = excluded.skip_budget_remaining,
            worker_apply_enabled = excluded.worker_apply_enabled,
            fallback_inline_projection_expected = excluded.fallback_inline_projection_expected,
            tr_response_rows_count = excluded.tr_response_rows_count,
            tr_response_skip_budget_limit = excluded.tr_response_skip_budget_limit,
            tr_response_skip_budget_used = excluded.tr_response_skip_budget_used,
            tr_response_skip_budget_remaining = excluded.tr_response_skip_budget_remaining,
            synthetic_child_guard_status = excluded.synthetic_child_guard_status,
            worker_side_effect_ready = excluded.worker_side_effect_ready,
            deferred_side_effect_required = excluded.deferred_side_effect_required,
            post_apply_deferred_side_effects_json = excluded.post_apply_deferred_side_effects_json,
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
            decision.cutover_scope,
            decision.skip_budget_limit,
            decision.skip_budget_used,
            decision.skip_budget_remaining,
            int(decision.worker_apply_enabled),
            int(decision.fallback_inline_projection_expected),
            decision.tr_response_rows_count,
            decision.tr_response_skip_budget_limit,
            decision.tr_response_skip_budget_used,
            decision.tr_response_skip_budget_remaining,
            decision.synthetic_child_guard_status,
            None
            if decision.worker_side_effect_ready is None
            else int(decision.worker_side_effect_ready),
            None
            if decision.deferred_side_effect_required is None
            else int(decision.deferred_side_effect_required),
            canonical_json(decision.post_apply_deferred_side_effects),
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


def _market_data_outbox_job(
    connection: sqlite3.Connection,
    event_id: str,
) -> Mapping[str, Any] | None:
    row = connection.execute(
        """
        SELECT outbox_id, status, updated_at, created_at, metadata_json
        FROM projection_outbox
        WHERE projection_name = 'market_data' AND event_id = ?
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    return None if row is None else dict(row)


def _outbox_is_ready(
    *,
    outbox_status: str | None,
    outbox_job_present: bool,
    min_outbox_status: str,
) -> bool:
    if not outbox_job_present:
        return False
    normalized = _normalize_outbox_status(outbox_status)
    if normalized in {"ERROR", "NOOP"}:
        return False
    required = _normalize_outbox_status(min_outbox_status)
    if required == "ENQUEUED":
        return normalized in {
            None,
            "ENQUEUED",
            "DUPLICATE",
            "PENDING",
            "PROCESSING",
            "APPLIED",
            "SKIPPED",
        }
    return normalized == required


def record_market_data_post_apply_deferred_side_effects(
    connection: sqlite3.Connection,
    event_id: str,
    side_effects: Mapping[str, Any],
) -> None:
    row = connection.execute(
        """
        SELECT evidence_json
        FROM market_data_projection_routing_decisions
        WHERE event_id = ? AND projection_name = 'market_data'
        LIMIT 1
        """,
        (event_id,),
    ).fetchone()
    if row is None:
        return
    payload = dict(side_effects)
    evidence = _json_object(row["evidence_json"])
    evidence["post_apply_deferred_side_effects"] = payload
    connection.execute(
        """
        UPDATE market_data_projection_routing_decisions
        SET
            post_apply_deferred_side_effects_json = ?,
            evidence_json = ?
        WHERE event_id = ? AND projection_name = 'market_data'
        """,
        (canonical_json(payload), canonical_json(evidence), event_id),
    )
    connection.commit()


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


def _price_tick_payload_identity(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        code = validate_stock_code(payload.get("code"))
        exchange = _price_tick_payload_exchange(payload)
    except Exception:
        return None
    return {"code": code, "exchange": exchange}


def _price_tick_payload_exchange(payload: Mapping[str, Any]) -> str:
    metadata = payload.get("metadata")
    if isinstance(metadata, Mapping) and metadata.get("exchange") is not None:
        return normalize_market_data_exchange(metadata.get("exchange"))
    if payload.get("exchange") is not None:
        return normalize_market_data_exchange(payload.get("exchange"))
    return "KRX"


def _price_tick_skip_count_in_current_minute(connection: sqlite3.Connection) -> int:
    minute_start = datetime_to_wire(utc_now().replace(second=0, microsecond=0))
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type = 'price_tick'
            AND effective_skip_inline = 1
            AND julianday(decided_at) >= julianday(?)
        """,
        (minute_start,),
    ).fetchone()
    return int(row["count"])


def _price_tick_skip_budget_remaining(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> int:
    limit = int(settings.gateway_market_data_append_only_price_tick_max_skip_per_minute)
    if limit <= 0:
        return 0
    used = _price_tick_skip_count_in_current_minute(connection)
    return max(limit - used, 0)


def _tr_response_skip_count_in_current_minute(connection: sqlite3.Connection) -> int:
    minute_start = datetime_to_wire(utc_now().replace(second=0, microsecond=0))
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type = 'tr_response'
            AND effective_skip_inline = 1
            AND julianday(decided_at) >= julianday(?)
        """,
        (minute_start,),
    ).fetchone()
    return int(row["count"])


def _tr_response_skip_budget_remaining(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> int:
    limit = int(settings.gateway_market_data_append_only_tr_response_max_skip_per_minute)
    if limit <= 0:
        return 0
    used = _tr_response_skip_count_in_current_minute(connection)
    return max(limit - used, 0)


def _tr_response_rows(payload: Mapping[str, Any]) -> list[Any]:
    rows = payload.get("rows")
    if isinstance(rows, list):
        return rows
    row = payload.get("row")
    return [row] if isinstance(row, Mapping) else []


def _synthetic_child_guard_status(
    *,
    required: bool,
    latest_status: str | None,
    append_only_ready: bool,
    synthetic_child_issue_count: int | None,
) -> str:
    if not required:
        return "NOT_REQUIRED"
    if latest_status != "PASS" or not append_only_ready:
        return "RECONCILE_NOT_READY"
    if synthetic_child_issue_count is None:
        return "UNKNOWN"
    if synthetic_child_issue_count > 0:
        return "ISSUES_FOUND"
    return "PASS"


def _count_all_decisions(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT COUNT(*) AS count FROM market_data_projection_routing_decisions"
    ).fetchone()
    return int(row["count"])


def _count_decisions(connection: sqlite3.Connection, column: str) -> int:
    if column not in {"would_skip_inline", "effective_skip_inline"}:
        raise ValueError(f"unsupported routing decision count column: {column}")
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE {column} = 1
        """
    ).fetchone()
    return int(row["count"])


def _count_effective_skip_by_event_type(
    connection: sqlite3.Connection,
    event_type: str,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE effective_skip_inline = 1 AND event_type = ?
        """,
        (event_type,),
    ).fetchone()
    return int(row["count"])


def _count_would_skip_by_event_type(
    connection: sqlite3.Connection,
    event_type: str,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE would_skip_inline = 1 AND event_type = ?
        """,
        (event_type,),
    ).fetchone()
    return int(row["count"])


def _count_invalid_effective_skips(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE effective_skip_inline = 1
            AND event_type NOT IN ('price_tick', 'tr_response')
        """
    ).fetchone()
    return int(row["count"])


def _count_effective_skip_outbox_errors(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions AS d
        JOIN projection_outbox AS po
            ON po.projection_name = d.projection_name
            AND po.event_id = d.event_id
        WHERE d.effective_skip_inline = 1
            AND po.status IN ('ERROR', 'DEAD_LETTER')
        """
    ).fetchone()
    return int(row["count"])


def _count_deferred_incremental_enqueue_records(
    connection: sqlite3.Connection,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE json_extract(
            post_apply_deferred_side_effects_json,
            '$.incremental_evaluation_enqueue_status'
        ) IS NOT NULL
        """
    ).fetchone()
    return int(row["count"])


def _count_tr_response_deferred_side_effect_records(
    connection: sqlite3.Connection,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type = 'tr_response'
            AND json_extract(
                post_apply_deferred_side_effects_json,
                '$.candidate_quote_refresh_enqueue_status'
            ) IS NOT NULL
        """
    ).fetchone()
    return int(row["count"])


def _count_tr_response_worker_applied_records(
    connection: sqlite3.Connection,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM projection_outbox
        WHERE projection_name = 'market_data'
            AND event_type = 'tr_response'
            AND status = 'APPLIED'
            AND json_extract(
                metadata_json,
                '$.last_worker_evidence.apply_result'
            ) = 'APPLIED_BY_WORKER'
        """
    ).fetchone()
    return int(row["count"])


def _count_tr_response_effective_skip_pending_worker(
    connection: sqlite3.Connection,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions AS d
        JOIN projection_outbox AS po
            ON po.projection_name = d.projection_name
            AND po.event_id = d.event_id
        WHERE d.event_type = 'tr_response'
            AND d.effective_skip_inline = 1
            AND po.status IN ('PENDING', 'PROCESSING')
        """
    ).fetchone()
    return int(row["count"])


def _count_tr_response_deferred_side_effect_errors(
    connection: sqlite3.Connection,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type = 'tr_response'
            AND (
                json_extract(
                    post_apply_deferred_side_effects_json,
                    '$.candidate_quote_refresh_error_count'
                ) > 0
                OR json_extract(
                    post_apply_deferred_side_effects_json,
                    '$.candidate_quote_refresh_enqueue_status'
                ) IN ('ERROR', 'COMPLETED_WITH_ERRORS')
            )
        """
    ).fetchone()
    return int(row["count"])


def _count_tr_response_duplicate_side_effect_records(
    connection: sqlite3.Connection,
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM projection_outbox
        WHERE projection_name = 'market_data'
            AND event_type = 'tr_response'
            AND json_extract(
                metadata_json,
                '$.last_worker_evidence.apply_result'
            ) = 'APPLIED_BY_VERIFY'
            AND json_extract(
                metadata_json,
                '$.last_worker_evidence.post_apply_side_effects.candidate_quote_refresh_enqueue_status'
            ) IS NOT NULL
        """
    ).fetchone()
    return int(row["count"])


def _blocked_reason_code_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        for reason in row.get("blocked_reason_codes", []):
            if reason != "DRY_RUN_WOULD_SKIP_INLINE":
                counter[str(reason)] += 1
    return dict(sorted(counter.items()))


def _fail_closed_reason_code_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        reasons = [str(reason) for reason in row.get("blocked_reason_codes", [])]
        if "EFFECTIVE_SKIP_BLOCKED_FAIL_CLOSED" not in reasons:
            continue
        for reason in reasons:
            if reason not in {
                "DRY_RUN_WOULD_SKIP_INLINE",
                "EFFECTIVE_SKIP_BLOCKED_FAIL_CLOSED",
            }:
                counter[reason] += 1
    return dict(sorted(counter.items()))


def _routing_row_to_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["dry_run_enabled"] = bool(payload["dry_run_enabled"])
    payload["cutover_enabled"] = bool(payload["cutover_enabled"])
    payload["reconcile_required"] = bool(payload["reconcile_required"])
    payload["append_only_ready"] = bool(payload["append_only_ready"])
    payload["outbox_job_present"] = bool(payload["outbox_job_present"])
    payload["would_skip_inline"] = bool(payload["would_skip_inline"])
    payload["effective_skip_inline"] = bool(payload["effective_skip_inline"])
    payload["worker_apply_enabled"] = bool(payload.get("worker_apply_enabled"))
    payload["fallback_inline_projection_expected"] = bool(
        payload.get("fallback_inline_projection_expected", True)
    )
    if payload.get("worker_side_effect_ready") is not None:
        payload["worker_side_effect_ready"] = bool(payload["worker_side_effect_ready"])
    if payload.get("deferred_side_effect_required") is not None:
        payload["deferred_side_effect_required"] = bool(
            payload["deferred_side_effect_required"]
        )
    payload["post_apply_deferred_side_effects"] = _json_object(
        payload.pop("post_apply_deferred_side_effects_json", "{}")
    )
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
