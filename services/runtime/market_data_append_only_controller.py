from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any
from uuid import uuid4

from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from storage.gateway_command_store import canonical_json

from services.config import Settings, load_settings
from services.runtime.market_data_projection_reconcile import (
    get_latest_market_data_projection_reconcile,
)

MARKET_DATA_APPEND_ONLY_EVENT_TYPES: tuple[str, ...] = (
    "price_tick",
    "tr_response",
    "condition_event",
)

OPERATING_MODE_ALLOWED_EVENT_TYPES: dict[str, tuple[str, ...]] = {
    "OFF": (),
    "DRY_RUN": (),
    "PRICE_TICK_ONLY": ("price_tick",),
    "TR_RESPONSE_ONLY": ("tr_response",),
    "CONDITION_EVENT_ONLY": ("condition_event",),
    "MARKET_DATA_LIMITED": MARKET_DATA_APPEND_ONLY_EVENT_TYPES,
    "MARKET_DATA_FULL_GUARDED": MARKET_DATA_APPEND_ONLY_EVENT_TYPES,
}


@dataclass(frozen=True, kw_only=True)
class MarketDataAppendOnlyEventTypeGate:
    event_type: str
    enabled_by_mode: bool
    specific_cutover_flag: bool
    specific_budget_limit: int
    specific_budget_used: int
    specific_budget_remaining: int
    worker_side_effect_ready: bool
    backlog_ready: bool
    reconcile_ready: bool
    effective_skip_allowed: bool
    blocked_reason_codes: Sequence[str] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "enabled_by_mode": self.enabled_by_mode,
            "specific_cutover_flag": self.specific_cutover_flag,
            "specific_budget_limit": self.specific_budget_limit,
            "specific_budget_used": self.specific_budget_used,
            "specific_budget_remaining": self.specific_budget_remaining,
            "worker_side_effect_ready": self.worker_side_effect_ready,
            "backlog_ready": self.backlog_ready,
            "reconcile_ready": self.reconcile_ready,
            "effective_skip_allowed": self.effective_skip_allowed,
            "blocked_reason_codes": list(self.blocked_reason_codes),
        }


@dataclass(frozen=True, kw_only=True)
class MarketDataAppendOnlyRollbackDecision:
    required: bool
    reason_codes: Sequence[str] = field(default_factory=tuple)
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "required": self.required,
            "reason_codes": list(self.reason_codes),
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, kw_only=True)
class MarketDataAppendOnlyControllerStatus:
    generated_at: str
    operating_mode: str
    global_kill_switch: bool
    auto_rollback_enabled: bool
    effective_cutover_enabled: bool
    allowed_event_types: Sequence[str]
    global_skip_budget_limit: int
    global_skip_budget_used: int
    global_skip_budget_remaining: int
    price_tick_gate: MarketDataAppendOnlyEventTypeGate
    tr_response_gate: MarketDataAppendOnlyEventTypeGate
    condition_event_gate: MarketDataAppendOnlyEventTypeGate
    latest_reconcile_status: str | None
    latest_reconcile_append_only_ready: bool
    latest_reconcile_age_sec: float | None
    backlog_readiness_status: str
    pr11_condition_event_cutover_ready: bool
    projection_outbox_pending_count: int
    projection_outbox_error_count: int
    projection_outbox_dead_letter_count: int
    projection_outbox_stale_processing_count: int
    invalid_effective_skip_count: int
    auto_rollback_required: bool
    auto_rollback_reason_codes: Sequence[str]
    rollback_hint: str
    status: str
    reason_codes: Sequence[str] = field(default_factory=tuple)
    dashboard_fast_status: str = "NOT_CHECKED"
    no_trading_side_effects: bool = True
    order_behavior_changed: bool = False
    live_real_allowed: bool = False
    read_only: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "operating_mode": self.operating_mode,
            "global_kill_switch": self.global_kill_switch,
            "auto_rollback_enabled": self.auto_rollback_enabled,
            "effective_cutover_enabled": self.effective_cutover_enabled,
            "allowed_event_types": list(self.allowed_event_types),
            "global_skip_budget_limit": self.global_skip_budget_limit,
            "global_skip_budget_used": self.global_skip_budget_used,
            "global_skip_budget_remaining": self.global_skip_budget_remaining,
            "price_tick_gate": self.price_tick_gate.to_dict(),
            "tr_response_gate": self.tr_response_gate.to_dict(),
            "condition_event_gate": self.condition_event_gate.to_dict(),
            "latest_reconcile_status": self.latest_reconcile_status,
            "latest_reconcile_append_only_ready": (
                self.latest_reconcile_append_only_ready
            ),
            "latest_reconcile_age_sec": self.latest_reconcile_age_sec,
            "backlog_readiness_status": self.backlog_readiness_status,
            "pr11_condition_event_cutover_ready": (
                self.pr11_condition_event_cutover_ready
            ),
            "projection_outbox_pending_count": self.projection_outbox_pending_count,
            "projection_outbox_error_count": self.projection_outbox_error_count,
            "projection_outbox_dead_letter_count": (
                self.projection_outbox_dead_letter_count
            ),
            "projection_outbox_stale_processing_count": (
                self.projection_outbox_stale_processing_count
            ),
            "invalid_effective_skip_count": self.invalid_effective_skip_count,
            "auto_rollback_required": self.auto_rollback_required,
            "auto_rollback_reason_codes": list(self.auto_rollback_reason_codes),
            "rollback_hint": self.rollback_hint,
            "status": self.status,
            "reason_codes": list(self.reason_codes),
            "dashboard_fast_status": self.dashboard_fast_status,
            "no_trading_side_effects": self.no_trading_side_effects,
            "order_behavior_changed": self.order_behavior_changed,
            "live_real_allowed": self.live_real_allowed,
            "read_only": self.read_only,
        }

    def gate_for_event_type(
        self,
        event_type: str,
    ) -> MarketDataAppendOnlyEventTypeGate | None:
        normalized = event_type.strip().lower()
        if normalized == "price_tick":
            return self.price_tick_gate
        if normalized == "tr_response":
            return self.tr_response_gate
        if normalized == "condition_event":
            return self.condition_event_gate
        return None


def build_market_data_append_only_controller_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    dashboard_fast_status: str | None = None,
) -> MarketDataAppendOnlyControllerStatus:
    resolved_settings = settings or load_settings()
    generated_at = datetime_to_wire(utc_now())
    operating_mode = resolved_settings.gateway_market_data_append_only_operating_mode
    resolved_dashboard_fast_status = str(
        dashboard_fast_status or "NOT_CHECKED"
    ).upper()
    allowed_event_types = OPERATING_MODE_ALLOWED_EVENT_TYPES[operating_mode]
    global_budget_limit = int(
        resolved_settings.gateway_market_data_append_only_global_max_skip_per_minute
    )
    global_budget_used = _global_skip_count_in_current_minute(connection)
    global_budget_remaining = (
        max(global_budget_limit - global_budget_used, 0)
        if global_budget_limit > 0
        else 0
    )
    latest_reconcile = get_latest_market_data_projection_reconcile(connection)
    latest_run = latest_reconcile.get("latest_run")
    latest_status = (
        str(latest_run.get("status"))
        if isinstance(latest_run, Mapping) and latest_run.get("status") is not None
        else None
    )
    latest_append_ready = bool(
        latest_run.get("append_only_ready") if isinstance(latest_run, Mapping) else False
    )
    latest_created_at = (
        str(latest_run.get("created_at"))
        if isinstance(latest_run, Mapping) and latest_run.get("created_at") is not None
        else None
    )
    latest_age_sec = _age_seconds(latest_created_at, now_wire=generated_at)
    reconcile_ready = _reconcile_ready(
        latest_status=latest_status,
        append_only_ready=latest_append_ready,
        latest_age_sec=latest_age_sec,
        settings=resolved_settings,
    )
    outbox_counts = _projection_outbox_counts(connection, settings=resolved_settings)
    backlog_readiness_status = _backlog_readiness_status(
        outbox_counts,
        settings=resolved_settings,
    )
    pr11_condition_event_cutover_ready = (
        backlog_readiness_status in {"PASS", "WARN"}
        and outbox_counts["condition_event_pending_count"]
        <= int(
            resolved_settings.gateway_market_data_append_only_max_condition_event_pending_within_sla
        )
        and outbox_counts["error_count"] <= int(
            resolved_settings.gateway_market_data_append_only_max_error_count
        )
        and outbox_counts["dead_letter_count"]
        <= int(resolved_settings.gateway_market_data_append_only_max_dead_letter_count)
        and outbox_counts["stale_processing_count"] == 0
        and latest_status == "PASS"
    )
    if not resolved_settings.gateway_market_data_append_only_require_backlog_ready:
        backlog_ready = True
    elif operating_mode == "MARKET_DATA_FULL_GUARDED":
        backlog_ready = backlog_readiness_status == "PASS"
    else:
        backlog_ready = backlog_readiness_status in {"PASS", "WARN"}
    worker_apply_enabled = bool(
        resolved_settings.projection_outbox_apply_projection_enabled
        and resolved_settings.projection_outbox_market_data_apply_enabled
    )
    side_effect_counts = _side_effect_counts(connection)
    rollback = evaluate_market_data_append_only_rollback(
        connection,
        settings=resolved_settings,
        latest_status=latest_status,
        append_only_ready=latest_append_ready,
        outbox_counts=outbox_counts,
        side_effect_counts=side_effect_counts,
        worker_apply_enabled=worker_apply_enabled,
        dashboard_fast_status=resolved_dashboard_fast_status,
    )
    common_blockers = _common_blockers(
        operating_mode=operating_mode,
        global_kill_switch=bool(
            resolved_settings.gateway_market_data_append_only_global_kill_switch
        ),
        auto_rollback_required=rollback.required,
        global_budget_remaining=global_budget_remaining,
        reconcile_ready=reconcile_ready,
        backlog_ready=backlog_ready,
        latest_age_sec=latest_age_sec,
        settings=resolved_settings,
    )
    price_tick_gate = _event_gate(
        event_type="price_tick",
        allowed_event_types=allowed_event_types,
        specific_cutover_flag=bool(
            resolved_settings.gateway_market_data_append_only_price_tick_cutover_enabled
        ),
        specific_budget_limit=int(
            resolved_settings.gateway_market_data_append_only_price_tick_max_skip_per_minute
        ),
        specific_budget_used=_event_skip_count_in_current_minute(
            connection,
            "price_tick",
        ),
        worker_side_effect_ready=(
            worker_apply_enabled
            if resolved_settings.gateway_market_data_append_only_require_worker_apply_enabled
            else True
        ),
        backlog_ready=backlog_ready,
        reconcile_ready=reconcile_ready,
        common_blockers=common_blockers,
    )
    tr_response_gate = _event_gate(
        event_type="tr_response",
        allowed_event_types=allowed_event_types,
        specific_cutover_flag=bool(
            resolved_settings.gateway_market_data_append_only_tr_response_cutover_enabled
        ),
        specific_budget_limit=int(
            resolved_settings.gateway_market_data_append_only_tr_response_max_skip_per_minute
        ),
        specific_budget_used=_event_skip_count_in_current_minute(
            connection,
            "tr_response",
        ),
        worker_side_effect_ready=(
            worker_apply_enabled
            if (
                resolved_settings.gateway_market_data_append_only_tr_response_require_worker_side_effects
            )
            else True
        ),
        backlog_ready=backlog_ready,
        reconcile_ready=reconcile_ready,
        common_blockers=common_blockers,
    )
    condition_fusion_ready = bool(
        resolved_settings.condition_fusion_event_incremental_enabled
        or not (
            resolved_settings.gateway_market_data_append_only_condition_event_require_fusion_enabled
        )
    )
    condition_event_gate = _event_gate(
        event_type="condition_event",
        allowed_event_types=allowed_event_types,
        specific_cutover_flag=bool(
            resolved_settings.gateway_market_data_append_only_condition_event_cutover_enabled
        ),
        specific_budget_limit=int(
            resolved_settings.gateway_market_data_append_only_condition_event_max_skip_per_minute
        ),
        specific_budget_used=_event_skip_count_in_current_minute(
            connection,
            "condition_event",
        ),
        worker_side_effect_ready=(
            worker_apply_enabled
            and condition_fusion_ready
            if (
                resolved_settings.gateway_market_data_append_only_condition_event_require_worker_side_effects
            )
            else True
        ),
        backlog_ready=(
            backlog_ready
            and (
                pr11_condition_event_cutover_ready
                or not (
                    resolved_settings.gateway_market_data_append_only_condition_event_require_backlog_ready
                )
            )
        ),
        reconcile_ready=reconcile_ready,
        common_blockers=common_blockers,
    )
    gates = (price_tick_gate, tr_response_gate, condition_event_gate)
    effective_cutover_enabled = any(gate.effective_skip_allowed for gate in gates)
    reason_codes = sorted(
        {
            reason
            for gate in gates
            for reason in gate.blocked_reason_codes
            if reason
        }
        | set(rollback.reason_codes)
    )
    status = _controller_status(
        gates=gates,
        rollback_required=rollback.required,
        global_kill_switch=bool(
            resolved_settings.gateway_market_data_append_only_global_kill_switch
        ),
        operating_mode=operating_mode,
    )
    return MarketDataAppendOnlyControllerStatus(
        generated_at=generated_at,
        operating_mode=operating_mode,
        global_kill_switch=bool(
            resolved_settings.gateway_market_data_append_only_global_kill_switch
        ),
        auto_rollback_enabled=bool(
            resolved_settings.gateway_market_data_append_only_auto_rollback_enabled
        ),
        effective_cutover_enabled=effective_cutover_enabled,
        allowed_event_types=allowed_event_types,
        global_skip_budget_limit=global_budget_limit,
        global_skip_budget_used=global_budget_used,
        global_skip_budget_remaining=global_budget_remaining,
        price_tick_gate=price_tick_gate,
        tr_response_gate=tr_response_gate,
        condition_event_gate=condition_event_gate,
        latest_reconcile_status=latest_status,
        latest_reconcile_append_only_ready=latest_append_ready,
        latest_reconcile_age_sec=latest_age_sec,
        backlog_readiness_status=backlog_readiness_status,
        pr11_condition_event_cutover_ready=pr11_condition_event_cutover_ready,
        projection_outbox_pending_count=outbox_counts["pending_count"],
        projection_outbox_error_count=outbox_counts["error_count"],
        projection_outbox_dead_letter_count=outbox_counts["dead_letter_count"],
        projection_outbox_stale_processing_count=outbox_counts[
            "stale_processing_count"
        ],
        invalid_effective_skip_count=_invalid_effective_skip_count(connection),
        auto_rollback_required=rollback.required,
        auto_rollback_reason_codes=rollback.reason_codes,
        rollback_hint=_rollback_hint(),
        status=status,
        reason_codes=tuple(reason_codes),
        dashboard_fast_status=resolved_dashboard_fast_status,
    )


def evaluate_market_data_append_only_rollback(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    latest_status: str | None,
    append_only_ready: bool,
    outbox_counts: Mapping[str, int],
    side_effect_counts: Mapping[str, int],
    worker_apply_enabled: bool,
    dashboard_fast_status: str = "NOT_CHECKED",
) -> MarketDataAppendOnlyRollbackDecision:
    if not settings.gateway_market_data_append_only_auto_rollback_enabled:
        return MarketDataAppendOnlyRollbackDecision(required=False)
    reason_codes: list[str] = []
    if outbox_counts["error_count"] > int(
        settings.gateway_market_data_append_only_max_error_count
    ):
        reason_codes.append("PROJECTION_OUTBOX_ERROR_COUNT_EXCEEDED")
    if outbox_counts["dead_letter_count"] > int(
        settings.gateway_market_data_append_only_max_dead_letter_count
    ):
        reason_codes.append("PROJECTION_OUTBOX_DEAD_LETTER_COUNT_EXCEEDED")
    if _invalid_effective_skip_count(connection) > 0:
        reason_codes.append("INVALID_EFFECTIVE_SKIP_EVENT_TYPE")
    if side_effect_counts["condition_event_candidate_ingest_executed_count"] > 0:
        reason_codes.append("CONDITION_EVENT_CANDIDATE_INGEST_EXECUTED")
    if latest_status == "FAIL":
        reason_codes.append("MARKET_DATA_RECONCILE_FAIL")
    if not append_only_ready and _recent_effective_skip_count(connection, settings) > 0:
        reason_codes.append("APPEND_ONLY_NOT_READY_AFTER_EFFECTIVE_SKIP")
    if _backlog_readiness_status(outbox_counts, settings=settings) == "FAIL":
        reason_codes.append("PROJECTION_OUTBOX_BACKLOG_FAIL")
    if (
        settings.gateway_market_data_append_only_require_dashboard_fast_ok
        and dashboard_fast_status == "FAIL"
    ):
        reason_codes.append("DASHBOARD_FAST_STATUS_FAIL")
    if (
        _any_cutover_flag_enabled(settings)
        or settings.gateway_market_data_append_only_operating_mode
        not in {"OFF", "DRY_RUN"}
    ) and not worker_apply_enabled:
        reason_codes.append("WORKER_APPLY_DISABLED_WITH_CUTOVER_FLAG")
    if outbox_counts["stale_processing_count"] > 0:
        reason_codes.append("PROJECTION_OUTBOX_STALE_PROCESSING")
    if (
        side_effect_counts["tr_response_deferred_side_effect_error_count"] > 0
        or side_effect_counts["condition_event_deferred_side_effect_error_count"] > 0
    ):
        reason_codes.append("MARKET_DATA_APPEND_ONLY_SIDE_EFFECT_ERROR")
    return MarketDataAppendOnlyRollbackDecision(
        required=bool(reason_codes),
        reason_codes=tuple(sorted(set(reason_codes))),
        evidence={
            "latest_reconcile_status": latest_status,
            "append_only_ready": append_only_ready,
            "outbox_counts": dict(outbox_counts),
            "side_effect_counts": dict(side_effect_counts),
            "worker_apply_enabled": worker_apply_enabled,
            "no_trading_side_effects": True,
        },
    )


def persist_market_data_append_only_controller_snapshot(
    connection: sqlite3.Connection,
    status: MarketDataAppendOnlyControllerStatus,
) -> str:
    snapshot_id = f"market_data_append_only_controller_{uuid4().hex}"
    snapshot = status.to_dict()
    gate_summary = {
        "price_tick": status.price_tick_gate.to_dict(),
        "tr_response": status.tr_response_gate.to_dict(),
        "condition_event": status.condition_event_gate.to_dict(),
    }
    connection.execute(
        """
        INSERT INTO market_data_append_only_controller_snapshots (
            snapshot_id,
            operating_mode,
            status,
            effective_cutover_enabled,
            global_kill_switch,
            auto_rollback_required,
            allowed_event_types_json,
            gate_summary_json,
            reason_codes_json,
            snapshot_json,
            created_at,
            no_trading_side_effects
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            snapshot_id,
            status.operating_mode,
            status.status,
            int(status.effective_cutover_enabled),
            int(status.global_kill_switch),
            int(status.auto_rollback_required),
            _canonical_json_value(list(status.allowed_event_types)),
            canonical_json(gate_summary),
            _canonical_json_value(list(status.reason_codes)),
            canonical_json(snapshot),
            status.generated_at,
        ),
    )
    connection.commit()
    return snapshot_id


def record_market_data_append_only_auto_rollback_event(
    connection: sqlite3.Connection,
    status: MarketDataAppendOnlyControllerStatus,
    *,
    settings: Settings | None = None,
) -> str | None:
    if not status.auto_rollback_required:
        return None
    resolved_settings = settings or load_settings()
    latest = connection.execute(
        """
        SELECT event_id, created_at
        FROM market_data_append_only_auto_rollback_events
        ORDER BY julianday(created_at) DESC
        LIMIT 1
        """
    ).fetchone()
    if latest is not None:
        age = _age_seconds(str(latest["created_at"]), now_wire=status.generated_at)
        if (
            age is not None
            and age
            < int(
                resolved_settings.gateway_market_data_append_only_auto_rollback_cooldown_sec
            )
        ):
            return str(latest["event_id"])
    event_id = f"market_data_append_only_auto_rollback_{uuid4().hex}"
    connection.execute(
        """
        INSERT INTO market_data_append_only_auto_rollback_events (
            event_id,
            status,
            reason_codes_json,
            evidence_json,
            created_at,
            no_trading_side_effects
        )
        VALUES (?, ?, ?, ?, ?, 1)
        """,
        (
            event_id,
            "REQUIRED",
            _canonical_json_value(list(status.auto_rollback_reason_codes)),
            canonical_json(status.to_dict()),
            status.generated_at,
        ),
    )
    connection.commit()
    return event_id


def list_market_data_append_only_auto_rollback_events(
    connection: sqlite3.Connection,
    *,
    limit: int = 50,
) -> list[dict[str, Any]]:
    bounded_limit = min(max(int(limit), 1), 500)
    rows = connection.execute(
        """
        SELECT *
        FROM market_data_append_only_auto_rollback_events
        ORDER BY julianday(created_at) DESC
        LIMIT ?
        """,
        (bounded_limit,),
    ).fetchall()
    return [
        {
            "event_id": str(row["event_id"]),
            "status": str(row["status"]),
            "reason_codes": _json_list(row["reason_codes_json"]),
            "evidence": _json_object(row["evidence_json"]),
            "created_at": row["created_at"],
            "no_trading_side_effects": bool(row["no_trading_side_effects"]),
        }
        for row in rows
    ]


def _event_gate(
    *,
    event_type: str,
    allowed_event_types: Sequence[str],
    specific_cutover_flag: bool,
    specific_budget_limit: int,
    specific_budget_used: int,
    worker_side_effect_ready: bool,
    backlog_ready: bool,
    reconcile_ready: bool,
    common_blockers: Sequence[str],
) -> MarketDataAppendOnlyEventTypeGate:
    specific_budget_remaining = (
        max(int(specific_budget_limit) - int(specific_budget_used), 0)
        if specific_budget_limit > 0
        else 0
    )
    blocked = list(common_blockers)
    enabled_by_mode = event_type in set(allowed_event_types)
    if not enabled_by_mode:
        blocked.append("MARKET_DATA_APPEND_ONLY_MODE_DOES_NOT_ALLOW_EVENT_TYPE")
    if not specific_cutover_flag:
        blocked.append(f"{event_type.upper()}_CUTOVER_DISABLED")
    if specific_budget_remaining <= 0:
        blocked.append(f"{event_type.upper()}_SKIP_BUDGET_EXHAUSTED")
    if not worker_side_effect_ready:
        blocked.append("WORKER_APPLY_NOT_ENABLED")
    if not reconcile_ready:
        blocked.append("MARKET_DATA_RECONCILE_NOT_PASS")
    if not backlog_ready:
        blocked.append("MARKET_DATA_APPEND_ONLY_CONTROLLER_BACKLOG_NOT_READY")
    return MarketDataAppendOnlyEventTypeGate(
        event_type=event_type,
        enabled_by_mode=enabled_by_mode,
        specific_cutover_flag=specific_cutover_flag,
        specific_budget_limit=int(specific_budget_limit),
        specific_budget_used=int(specific_budget_used),
        specific_budget_remaining=specific_budget_remaining,
        worker_side_effect_ready=worker_side_effect_ready,
        backlog_ready=backlog_ready,
        reconcile_ready=reconcile_ready,
        effective_skip_allowed=not blocked,
        blocked_reason_codes=tuple(sorted(set(blocked))),
    )


def _common_blockers(
    *,
    operating_mode: str,
    global_kill_switch: bool,
    auto_rollback_required: bool,
    global_budget_remaining: int,
    reconcile_ready: bool,
    backlog_ready: bool,
    latest_age_sec: float | None,
    settings: Settings,
) -> list[str]:
    blocked: list[str] = []
    if operating_mode == "OFF":
        blocked.append("MARKET_DATA_APPEND_ONLY_MODE_OFF")
    elif operating_mode == "DRY_RUN":
        blocked.append("MARKET_DATA_APPEND_ONLY_MODE_DOES_NOT_ALLOW_EVENT_TYPE")
    if global_kill_switch:
        blocked.append("MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH")
    if auto_rollback_required:
        blocked.append("MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_REQUIRED")
    if global_budget_remaining <= 0:
        blocked.append("MARKET_DATA_APPEND_ONLY_GLOBAL_BUDGET_EXHAUSTED")
    if not reconcile_ready:
        blocked.append("MARKET_DATA_RECONCILE_NOT_PASS")
    if not backlog_ready:
        blocked.append("MARKET_DATA_APPEND_ONLY_CONTROLLER_NOT_READY")
    if (
        latest_age_sec is not None
        and latest_age_sec > int(settings.gateway_market_data_append_only_health_stale_sec)
    ):
        blocked.append("MARKET_DATA_APPEND_ONLY_HEALTH_STALE")
    return blocked


def _reconcile_ready(
    *,
    latest_status: str | None,
    append_only_ready: bool,
    latest_age_sec: float | None,
    settings: Settings,
) -> bool:
    if not settings.gateway_market_data_append_only_require_reconcile_pass:
        return True
    if latest_status != "PASS" or not append_only_ready:
        return False
    if latest_age_sec is None:
        return False
    return latest_age_sec <= int(settings.gateway_market_data_append_only_reconcile_max_age_sec)


def _controller_status(
    *,
    gates: Sequence[MarketDataAppendOnlyEventTypeGate],
    rollback_required: bool,
    global_kill_switch: bool,
    operating_mode: str,
) -> str:
    if rollback_required:
        return "FAIL"
    if any(gate.effective_skip_allowed for gate in gates):
        return "PASS"
    if operating_mode in {"OFF", "DRY_RUN"} or global_kill_switch:
        return "WARN"
    return "WARN"


def _backlog_readiness_status(
    counts: Mapping[str, int],
    *,
    settings: Settings,
) -> str:
    if counts["error_count"] > int(settings.gateway_market_data_append_only_max_error_count):
        return "FAIL"
    if counts["dead_letter_count"] > int(
        settings.gateway_market_data_append_only_max_dead_letter_count
    ):
        return "FAIL"
    if counts["stale_processing_count"] > 0:
        return "FAIL"
    if counts["pending_count"] > int(
        settings.gateway_market_data_append_only_max_pending_within_sla
    ):
        return "FAIL"
    if counts["condition_event_pending_count"] > int(
        settings.gateway_market_data_append_only_max_condition_event_pending_within_sla
    ):
        return "FAIL"
    if counts["pending_count"] > 0:
        return "WARN"
    return "PASS"


def _projection_outbox_counts(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
) -> dict[str, int]:
    counts = {
        "pending_count": 0,
        "processing_count": 0,
        "error_count": 0,
        "dead_letter_count": 0,
        "condition_event_pending_count": 0,
        "stale_processing_count": 0,
    }
    for row in connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM projection_outbox
        GROUP BY status
        """
    ).fetchall():
        status = str(row["status"]).upper()
        if status == "PENDING":
            counts["pending_count"] = int(row["count"])
        elif status == "PROCESSING":
            counts["processing_count"] = int(row["count"])
        elif status == "ERROR":
            counts["error_count"] = int(row["count"])
        elif status == "DEAD_LETTER":
            counts["dead_letter_count"] = int(row["count"])
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM projection_outbox
        WHERE event_type = 'condition_event'
            AND status = 'PENDING'
        """
    ).fetchone()
    counts["condition_event_pending_count"] = int(row["count"])
    stale_cutoff = datetime_to_wire(
        utc_now()
        - timedelta(
            seconds=max(
                int(getattr(settings, "projection_outbox_processing_ttl_sec", 60)),
                1,
            )
        )
    )
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM projection_outbox
        WHERE status = 'PROCESSING'
            AND locked_at IS NOT NULL
            AND julianday(locked_at) < julianday(?)
        """,
        (stale_cutoff,),
    ).fetchone()
    counts["stale_processing_count"] = int(row["count"])
    return counts


def _side_effect_counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "tr_response_deferred_side_effect_error_count": _count_json_path_matches(
            connection,
            event_type="tr_response",
            path="$.candidate_quote_refresh_error_count",
            positive_number=True,
        ),
        "condition_event_deferred_side_effect_error_count": _count_json_path_matches(
            connection,
            event_type="condition_event",
            path="$.condition_fusion_error_count",
            positive_number=True,
        ),
        "condition_event_candidate_ingest_executed_count": _count_condition_event_candidate_ingest(
            connection
        ),
    }


def _count_json_path_matches(
    connection: sqlite3.Connection,
    *,
    event_type: str,
    path: str,
    positive_number: bool = False,
) -> int:
    operator = "> 0" if positive_number else "IS NOT NULL"
    row = connection.execute(
        f"""
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type = ?
            AND json_extract(post_apply_deferred_side_effects_json, ?) {operator}
        """,
        (event_type, path),
    ).fetchone()
    return int(row["count"])


def _count_condition_event_candidate_ingest(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type = 'condition_event'
            AND (
                candidate_ingest_executed = 1
                OR json_extract(
                    post_apply_deferred_side_effects_json,
                    '$.candidate_ingest_executed'
                ) = 1
            )
        """
    ).fetchone()
    return int(row["count"])


def _global_skip_count_in_current_minute(connection: sqlite3.Connection) -> int:
    minute_start = datetime_to_wire(utc_now().replace(second=0, microsecond=0))
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type IN ('price_tick', 'tr_response', 'condition_event')
            AND effective_skip_inline = 1
            AND julianday(decided_at) >= julianday(?)
        """,
        (minute_start,),
    ).fetchone()
    return int(row["count"])


def _event_skip_count_in_current_minute(
    connection: sqlite3.Connection,
    event_type: str,
) -> int:
    minute_start = datetime_to_wire(utc_now().replace(second=0, microsecond=0))
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type = ?
            AND effective_skip_inline = 1
            AND julianday(decided_at) >= julianday(?)
        """,
        (event_type, minute_start),
    ).fetchone()
    return int(row["count"])


def _recent_effective_skip_count(
    connection: sqlite3.Connection,
    settings: Settings,
) -> int:
    cutoff = datetime_to_wire(
        utc_now()
        - timedelta(seconds=int(settings.gateway_market_data_append_only_health_stale_sec))
    )
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE event_type IN ('price_tick', 'tr_response', 'condition_event')
            AND effective_skip_inline = 1
            AND julianday(decided_at) >= julianday(?)
        """,
        (cutoff,),
    ).fetchone()
    return int(row["count"])


def _invalid_effective_skip_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS count
        FROM market_data_projection_routing_decisions
        WHERE effective_skip_inline = 1
            AND event_type NOT IN ('price_tick', 'tr_response', 'condition_event')
        """
    ).fetchone()
    return int(row["count"])


def _any_cutover_flag_enabled(settings: Settings) -> bool:
    return any(
        (
            settings.gateway_market_data_append_only_price_tick_cutover_enabled,
            settings.gateway_market_data_append_only_tr_response_cutover_enabled,
            settings.gateway_market_data_append_only_condition_event_cutover_enabled,
        )
    )


def _age_seconds(value: str | None, *, now_wire: str) -> float | None:
    if not value:
        return None
    try:
        return max(
            (
                parse_timestamp(now_wire, "now")
                - parse_timestamp(value, "timestamp")
            ).total_seconds(),
            0.0,
        )
    except Exception:
        return None


def _canonical_json_value(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _json_list(raw: str | None) -> list[Any]:
    if not raw:
        return []
    try:
        import json

        payload = json.loads(raw)
    except Exception:
        return []
    return payload if isinstance(payload, list) else []


def _json_object(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        import json

        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _rollback_hint() -> str:
    return (
        "Set GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH=true, "
        "set GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE=OFF, or set "
        "GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE=0."
    )
