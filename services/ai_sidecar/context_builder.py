from __future__ import annotations

import json
import sqlite3
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from domain.ai_sidecar.context import (
    SCHEMA_VERSION,
    AISidecarContextPacket,
    AISidecarContextSection,
    AISidecarContextTaskSpec,
    calculate_context_hash,
    canonical_context_json,
    packet_hash_material,
    parse_task_type,
)
from domain.ai_sidecar.policy import get_allowed_tasks, get_forbidden_actions
from domain.ai_sidecar.tasks import AISidecarTaskType
from domain.broker.utils import datetime_to_wire, utc_now
from storage.event_store import get_gateway_status_values, list_recent_gateway_events
from storage.gateway_command_store import GatewayCommandStatus

from services.ai_sidecar.context_policy import sanitize_context_sections
from services.ai_sidecar.redaction import redact_context
from services.candidate_service import (
    get_candidate,
    get_candidate_status,
    list_candidate_projection_errors,
    list_candidates,
)
from services.config import Settings, candidate_timezone, load_settings
from services.dashboard_service import build_dashboard_errors, build_safety_section
from services.market_data_service import (
    get_latest_tick,
    get_market_data_readiness,
    get_market_data_status,
    list_projection_errors,
)
from services.risk_gate import (
    get_latest_risk_observation,
    get_risk_status,
    list_latest_risk_observations,
    list_risk_errors,
)
from services.strategy_engine import (
    get_latest_strategy_observation,
    get_strategy_status,
    list_latest_strategy_observations,
    list_strategy_errors,
)
from services.theme_service import (
    get_latest_theme_snapshot,
    get_theme,
    get_theme_status,
    list_latest_theme_snapshots,
    list_theme_projection_errors,
    list_theme_snapshot_members,
    list_theme_snapshots,
)

DEFAULT_CONTEXT_LIMIT = 50
MAX_CONTEXT_LIMIT = 200

TASK_SPECS: dict[AISidecarTaskType, AISidecarContextTaskSpec] = {
    AISidecarTaskType.DAILY_MARKET_BRIEF: AISidecarContextTaskSpec(
        task_type=AISidecarTaskType.DAILY_MARKET_BRIEF,
        required_sections=(
            "safety_summary",
            "gateway_status",
            "market_data_status",
            "theme_snapshot_summary",
            "candidate_state_counts",
            "strategy_status_counts",
            "risk_status_counts",
            "recent_events_errors_summary",
            "ai_sidecar_status",
        ),
    ),
    AISidecarTaskType.THEME_BRIEF: AISidecarContextTaskSpec(
        task_type=AISidecarTaskType.THEME_BRIEF,
        required_sections=("theme_detail", "latest_theme_snapshot"),
        optional_sections=(
            "theme_snapshot_members_top",
            "theme_snapshot_history_recent",
            "market_readiness_top_members",
            "related_candidates",
            "related_strategy_observations",
            "related_risk_observations",
        ),
        related_entity_type="theme",
    ),
    AISidecarTaskType.CANDIDATE_BLOCK_RCA: AISidecarContextTaskSpec(
        task_type=AISidecarTaskType.CANDIDATE_BLOCK_RCA,
        required_sections=("candidate_detail", "candidate_context_latest"),
        optional_sections=(
            "candidate_sources",
            "candidate_transitions",
            "latest_market_tick_readiness",
            "theme_context",
            "latest_strategy_observation",
            "latest_risk_observation",
            "related_projection_errors",
            "dashboard_safety_warnings",
        ),
        related_entity_type="candidate",
    ),
    AISidecarTaskType.NO_TRADE_RCA: AISidecarContextTaskSpec(
        task_type=AISidecarTaskType.NO_TRADE_RCA,
        required_sections=(
            "dashboard_safety",
            "pipeline_funnel_counts",
            "candidate_state_counts",
            "strategy_status_counts",
            "risk_status_counts",
            "reason_code_summary",
            "recent_errors",
            "gateway_transport_status",
            "ai_sidecar_status",
        ),
    ),
    AISidecarTaskType.TRADE_REVIEW: AISidecarContextTaskSpec(
        task_type=AISidecarTaskType.TRADE_REVIEW,
        required_sections=("trade_execution_unavailable",),
        optional_sections=(
            "candidate_observation_review",
            "strategy_observation_review",
            "risk_observation_review",
        ),
        related_entity_type="trade_or_candidate",
    ),
    AISidecarTaskType.OPS_INCIDENT_SUMMARY: AISidecarContextTaskSpec(
        task_type=AISidecarTaskType.OPS_INCIDENT_SUMMARY,
        required_sections=("gateway_status", "gateway_problem_events", "error_summary"),
        optional_sections=("gateway_transport_status", "recent_heartbeat_timestamps"),
    ),
    AISidecarTaskType.CODEX_PROMPT_DRAFT: AISidecarContextTaskSpec(
        task_type=AISidecarTaskType.CODEX_PROMPT_DRAFT,
        required_sections=(
            "selected_observation_summary",
            "safety_policy_summary",
            "forbidden_scope_summary",
        ),
        optional_sections=("docs_pointers", "recent_errors"),
    ),
}


def build_context_packet(
    connection: sqlite3.Connection,
    task_type: AISidecarTaskType | str,
    *,
    trade_date: str | None = None,
    related_entity_id: str | None = None,
    related_entity_type: str | None = None,
    limit: int | None = None,
    settings: Settings | None = None,
) -> AISidecarContextPacket:
    resolved_settings = settings or load_settings()
    task = parse_task_type(task_type)
    bounded_limit = _bounded_limit(limit, resolved_settings)
    target_trade_date = trade_date or _default_trade_date(resolved_settings)

    if task is AISidecarTaskType.DAILY_MARKET_BRIEF:
        return build_daily_market_brief_context(
            connection,
            target_trade_date,
            resolved_settings,
            bounded_limit,
        )
    if task is AISidecarTaskType.THEME_BRIEF:
        return build_theme_brief_context(
            connection,
            related_entity_id or "",
            resolved_settings,
            bounded_limit,
        )
    if task is AISidecarTaskType.CANDIDATE_BLOCK_RCA:
        return build_candidate_block_rca_context(
            connection,
            related_entity_id or "",
            resolved_settings,
            bounded_limit,
        )
    if task is AISidecarTaskType.NO_TRADE_RCA:
        return build_no_trade_rca_context(
            connection,
            target_trade_date,
            resolved_settings,
            bounded_limit,
        )
    if task is AISidecarTaskType.TRADE_REVIEW:
        if _is_live_sim_order_context(related_entity_type, related_entity_id):
            return build_live_sim_order_context(
                connection,
                related_entity_id or "",
                settings=resolved_settings,
                limit=bounded_limit,
            )
        return build_trade_review_context(
            connection,
            related_entity_id,
            resolved_settings,
            bounded_limit,
        )
    if task is AISidecarTaskType.OPS_INCIDENT_SUMMARY:
        if _is_live_sim_session_context(related_entity_type):
            return build_live_sim_session_context(
                connection,
                target_trade_date,
                settings=resolved_settings,
                limit=bounded_limit,
            )
        if _is_live_sim_reconcile_context(related_entity_type, related_entity_id):
            return build_live_sim_reconcile_context(
                connection,
                related_entity_id or "",
                settings=resolved_settings,
                limit=bounded_limit,
            )
        if _is_live_sim_incident_context(related_entity_type):
            return build_live_sim_incident_context(
                connection,
                related_entity_id=related_entity_id,
                trade_date=target_trade_date if trade_date else None,
                settings=resolved_settings,
                limit=bounded_limit,
            )
        return build_ops_incident_context(
            connection,
            related_entity_id=related_entity_id,
            trade_date=target_trade_date if trade_date else None,
            settings=resolved_settings,
            limit=bounded_limit,
        )
    if task is AISidecarTaskType.CODEX_PROMPT_DRAFT:
        return build_codex_prompt_context(
            connection,
            related_entity_id=related_entity_id,
            trade_date=target_trade_date if trade_date else None,
            settings=resolved_settings,
            limit=bounded_limit,
            related_entity_type=related_entity_type,
        )
    raise ValueError(f"unsupported AI Sidecar task_type: {task.value}")


def build_daily_market_brief_context(
    connection: sqlite3.Connection,
    trade_date: str,
    settings: Settings,
    limit: int,
) -> AISidecarContextPacket:
    sections = [
        _section("safety_summary", "dashboard", _ai_safety_summary(settings), row_count=1),
        _section("gateway_status", "gateway_status", _gateway_status(connection), row_count=1),
        _section(
            "market_data_status",
            "market_data_projection",
            get_market_data_status(connection, settings=settings),
            row_count=1,
        ),
        _section(
            "theme_snapshot_summary",
            "theme_latest_snapshots",
            {
                "status": get_theme_status(connection, settings=settings),
                "latest_snapshots": _compact_rows(
                    list_latest_theme_snapshots(connection, limit=limit),
                    keep_keys=(
                        "snapshot_id",
                        "theme_id",
                        "theme_name",
                        "calculated_at",
                        "state",
                        "quality_status",
                        "leading_code",
                        "leading_name",
                        "fresh_coverage_ratio",
                        "rising_ratio",
                        "total_trade_value",
                        "reason_codes",
                    ),
                ),
            },
            row_count=_count_rows(connection, "theme_latest_snapshots"),
        ),
        _section(
            "candidate_state_counts",
            "candidates",
            get_candidate_status(connection, settings=settings),
            row_count=_count_rows(connection, "candidates"),
        ),
        _section(
            "strategy_status_counts",
            "strategy_observations_latest",
            get_strategy_status(connection, settings),
            row_count=_count_rows(connection, "strategy_observations_latest"),
        ),
        _section(
            "risk_status_counts",
            "risk_observations_latest",
            get_risk_status(connection, settings),
            row_count=_count_rows(connection, "risk_observations_latest"),
        ),
        _section(
            "recent_events_errors_summary",
            "event_store_and_projection_errors",
            {
                "gateway_events": _gateway_event_summaries(
                    list_recent_gateway_events(connection, limit=min(limit, 20))
                ),
                "errors": _error_summary(connection, settings=settings, limit=min(limit, 20)),
            },
            row_count=limit,
        ),
        _section(
            "ai_sidecar_status",
            "ai_sidecar",
            build_context_builder_status(settings),
            row_count=1,
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.DAILY_MARKET_BRIEF,
        trade_date=trade_date,
        related_entity_type=None,
        related_entity_id=None,
        sections=sections,
        settings=settings,
    )


def build_theme_brief_context(
    connection: sqlite3.Connection,
    theme_id: str,
    settings: Settings,
    limit: int,
) -> AISidecarContextPacket:
    missing: list[str] = []
    warnings: list[str] = []
    theme = get_theme(connection, theme_id) if theme_id else None
    if theme is None:
        missing.append("THEME_NOT_FOUND")
        warnings.append("Requested theme_id was not found in theme membership table.")
    latest_snapshot = get_latest_theme_snapshot(connection, theme_id) if theme else None
    if latest_snapshot is None:
        missing.append("LATEST_THEME_SNAPSHOT_MISSING")

    member_rows = (
        []
        if latest_snapshot is None
        else list_theme_snapshot_members(connection, latest_snapshot["snapshot_id"])[:limit]
    )
    candidate_rows = (
        []
        if not theme_id
        else list_candidates(
            connection,
            theme_id=theme_id,
            active_only=False,
            limit=limit,
        )
    )
    sections = [
        _section(
            "theme_detail",
            "themes",
            theme or {},
            row_count=1 if theme else 0,
            missing=theme is None,
        ),
        _section(
            "latest_theme_snapshot",
            "theme_latest_snapshots",
            latest_snapshot or {},
            row_count=1 if latest_snapshot else 0,
            missing=latest_snapshot is None,
        ),
        _section(
            "theme_snapshot_members_top",
            "theme_snapshot_members",
            {"items": _compact_rows(member_rows, keep_keys=_THEME_MEMBER_KEYS)},
            row_count=len(member_rows),
        ),
        _section(
            "theme_snapshot_history_recent",
            "theme_snapshots",
            {
                "items": (
                    list_theme_snapshots(connection, theme_id, limit=min(limit, 20))
                    if theme
                    else []
                )
            },
            row_count=(
                _count_rows(connection, "theme_snapshots", "theme_id = ?", (theme_id,))
                if theme
                else 0
            ),
        ),
        _section(
            "market_readiness_top_members",
            "market_data_readiness",
            {
                "items": [
                    get_market_data_readiness(connection, row["code"], settings=settings)
                    for row in member_rows[: min(limit, 20)]
                    if row.get("code")
                ]
            },
            row_count=min(len(member_rows), 20),
        ),
        _section(
            "related_candidates",
            "candidates",
            {"items": candidate_rows},
            row_count=len(candidate_rows),
        ),
        _section(
            "related_strategy_observations",
            "strategy_observations_latest",
            {
                "items": [
                    item
                    for item in (
                        get_latest_strategy_observation(
                            connection,
                            candidate["candidate_instance_id"],
                            include_setups=False,
                        )
                        for candidate in candidate_rows[:limit]
                    )
                    if item is not None
                ]
            },
            row_count=len(candidate_rows),
        ),
        _section(
            "related_risk_observations",
            "risk_observations_latest",
            {
                "items": [
                    item
                    for item in (
                        get_latest_risk_observation(
                            connection,
                            candidate["candidate_instance_id"],
                            include_checks=False,
                        )
                        for candidate in candidate_rows[:limit]
                    )
                    if item is not None
                ]
            },
            row_count=len(candidate_rows),
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.THEME_BRIEF,
        trade_date=None,
        related_entity_type="theme",
        related_entity_id=theme_id,
        sections=sections,
        settings=settings,
        missing_sections=missing,
        warnings=warnings,
    )


def build_candidate_block_rca_context(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
    settings: Settings,
    limit: int,
) -> AISidecarContextPacket:
    missing: list[str] = []
    warnings = [
        "MATCHED_OBSERVATION is a classifier result, not a buy signal.",
        "OBSERVE_PASS is not order approval.",
    ]
    candidate = (
        get_candidate(
            connection,
            candidate_instance_id,
            include_context=True,
            include_sources=True,
            include_transitions=True,
        )
        if candidate_instance_id
        else None
    )
    if candidate is None:
        missing.append("CANDIDATE_NOT_FOUND")
    code = candidate.get("code") if candidate else None
    theme_id = candidate.get("theme_id") if candidate else None
    strategy = (
        get_latest_strategy_observation(connection, candidate_instance_id, include_setups=True)
        if candidate
        else None
    )
    risk = (
        get_latest_risk_observation(connection, candidate_instance_id, include_checks=True)
        if candidate
        else None
    )
    if strategy is None:
        missing.append("STRATEGY_OBSERVATION_MISSING")
    if risk is None:
        missing.append("RISK_OBSERVATION_MISSING")

    sections = [
        _section(
            "candidate_detail",
            "candidates",
            _drop_nested(candidate or {}, ("context", "sources", "transitions")),
            row_count=1 if candidate else 0,
            missing=candidate is None,
        ),
        _section(
            "candidate_context_latest",
            "candidate_context_latest",
            {"context": (candidate or {}).get("context")},
            row_count=1 if candidate and candidate.get("context") else 0,
            missing=not bool(candidate and candidate.get("context")),
        ),
        _section(
            "candidate_sources",
            "candidate_source_events",
            {"items": (candidate or {}).get("sources", [])[:limit]},
            row_count=len((candidate or {}).get("sources", [])),
        ),
        _section(
            "candidate_transitions",
            "candidate_state_transitions",
            {"items": (candidate or {}).get("transitions", [])[:limit]},
            row_count=len((candidate or {}).get("transitions", [])),
        ),
        _section(
            "latest_market_tick_readiness",
            "market_data_projection",
            {
                "latest_tick": get_latest_tick(connection, code) if code else None,
                "readiness": (
                    get_market_data_readiness(connection, code, settings=settings) if code else None
                ),
            },
            row_count=1 if code else 0,
        ),
        _section(
            "theme_context",
            "theme_latest_snapshots",
            {
                "theme_id": theme_id,
                "latest_snapshot": (
                    get_latest_theme_snapshot(connection, theme_id) if theme_id else None
                ),
            },
            row_count=1 if theme_id else 0,
        ),
        _section(
            "latest_strategy_observation",
            "strategy_observations_latest",
            strategy or {},
            row_count=1 if strategy else 0,
            missing=strategy is None,
        ),
        _section(
            "latest_risk_observation",
            "risk_observations_latest",
            risk or {},
            row_count=1 if risk else 0,
            missing=risk is None,
        ),
        _section(
            "related_projection_errors",
            "projection_errors",
            _related_errors(
                connection,
                candidate_instance_id=candidate_instance_id,
                code=str(code) if code else None,
                limit=limit,
            ),
            row_count=limit,
        ),
        _section(
            "dashboard_safety_warnings",
            "dashboard",
            {"warnings": build_safety_section(settings)["warnings"]},
            row_count=1,
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.CANDIDATE_BLOCK_RCA,
        trade_date=candidate.get("trade_date") if candidate else None,
        related_entity_type="candidate",
        related_entity_id=candidate_instance_id,
        sections=sections,
        settings=settings,
        missing_sections=missing,
        warnings=warnings,
    )


def build_no_trade_rca_context(
    connection: sqlite3.Connection,
    trade_date: str,
    settings: Settings,
    limit: int,
) -> AISidecarContextPacket:
    warnings = [
        "NO_ORDER_PATH_BY_DESIGN: PR AI-1 has no OMS/order path and no AI execution path.",
        "MATCHED_OBSERVATION is not a buy signal.",
        "OBSERVE_PASS is not order approval.",
    ]
    sections = [
        _section("dashboard_safety", "dashboard", _ai_safety_summary(settings), row_count=1),
        _section(
            "pipeline_funnel_counts",
            "pipeline_projection_tables",
            _pipeline_counts(connection, settings),
            row_count=1,
        ),
        _section(
            "candidate_state_counts",
            "candidates",
            get_candidate_status(connection, settings=settings),
            row_count=_count_rows(connection, "candidates", "trade_date = ?", (trade_date,)),
        ),
        _section(
            "strategy_status_counts",
            "strategy_observations_latest",
            get_strategy_status(connection, settings),
            row_count=_count_rows(
                connection,
                "strategy_observations_latest",
                "trade_date = ?",
                (trade_date,),
            ),
        ),
        _section(
            "risk_status_counts",
            "risk_observations_latest",
            get_risk_status(connection, settings),
            row_count=_count_rows(
                connection,
                "risk_observations_latest",
                "trade_date = ?",
                (trade_date,),
            ),
        ),
        _section(
            "reason_code_summary",
            "candidate_strategy_risk_reason_codes",
            _reason_code_summary(connection, trade_date=trade_date, limit=limit),
            row_count=1,
        ),
        _section(
            "recent_errors",
            "projection_and_evaluation_errors",
            _error_summary(connection, settings=settings, limit=limit),
            row_count=limit,
        ),
        _section(
            "gateway_transport_status",
            "gateway_commands",
            _gateway_transport_status(connection),
            row_count=1,
        ),
        _section(
            "ai_sidecar_status",
            "ai_sidecar",
            build_context_builder_status(settings),
            row_count=1,
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.NO_TRADE_RCA,
        trade_date=trade_date,
        related_entity_type=None,
        related_entity_id=None,
        sections=sections,
        settings=settings,
        warnings=warnings,
    )


def build_trade_review_context(
    connection: sqlite3.Connection,
    trade_id_or_candidate_id: str | None,
    settings: Settings,
    limit: int,
) -> AISidecarContextPacket:
    missing = ["OMS_UNAVAILABLE", "TRADE_TABLE_UNAVAILABLE", "LIVE_SIM_NOT_ENABLED"]
    warnings = ["PR AI-1 has no trade execution data; this is observation review context only."]
    candidate = (
        get_candidate(connection, trade_id_or_candidate_id, include_context=True)
        if trade_id_or_candidate_id
        else None
    )
    sections = [
        _section(
            "trade_execution_unavailable",
            "execution_tables_unavailable",
            {
                "available": False,
                "reason": "OMS and trade tables are not implemented in PR AI-1.",
            },
            row_count=0,
            missing=True,
        ),
        _section(
            "candidate_observation_review",
            "candidates",
            candidate
            or {
                "recent_candidates": list_candidates(
                    connection,
                    active_only=False,
                    limit=limit,
                )
            },
            row_count=1 if candidate else limit,
            missing=candidate is None and bool(trade_id_or_candidate_id),
        ),
        _section(
            "strategy_observation_review",
            "strategy_observations_latest",
            (
                get_latest_strategy_observation(
                    connection,
                    trade_id_or_candidate_id,
                    include_setups=True,
                )
                if trade_id_or_candidate_id and candidate
                else {
                    "recent_observations": list_latest_strategy_observations(
                        connection,
                        limit=limit,
                    )
                }
            ),
            row_count=limit,
        ),
        _section(
            "risk_observation_review",
            "risk_observations_latest",
            (
                get_latest_risk_observation(
                    connection,
                    trade_id_or_candidate_id,
                    include_checks=True,
                )
                if trade_id_or_candidate_id and candidate
                else {
                    "recent_observations": list_latest_risk_observations(
                        connection,
                        limit=limit,
                    )
                }
            ),
            row_count=limit,
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.TRADE_REVIEW,
        trade_date=candidate.get("trade_date") if candidate else None,
        related_entity_type="trade_or_candidate" if trade_id_or_candidate_id else None,
        related_entity_id=trade_id_or_candidate_id,
        sections=sections,
        settings=settings,
        missing_sections=missing,
        warnings=warnings,
    )


def build_live_sim_session_context(
    connection: sqlite3.Connection,
    trade_date: str,
    settings: Settings | None = None,
    limit: int | None = None,
) -> AISidecarContextPacket:
    from services.live_sim.live_sim_service import (
        get_live_sim_status,
        list_live_sim_errors,
        list_live_sim_executions,
        list_live_sim_intents,
        list_live_sim_orders,
        list_live_sim_reconcile_snapshots,
        list_live_sim_rejections,
    )

    resolved_settings = settings or load_settings()
    bounded_limit = _bounded_limit(limit, resolved_settings)
    intents = list_live_sim_intents(connection, trade_date=trade_date, limit=bounded_limit)
    orders = list_live_sim_orders(connection, trade_date=trade_date, limit=bounded_limit)
    command_ids = _live_sim_command_ids(intents, orders)
    executions = _live_sim_related_executions(
        list_live_sim_executions(connection, limit=bounded_limit),
        intents=intents,
        orders=orders,
    )
    rejections = [
        row
        for row in list_live_sim_rejections(connection, limit=bounded_limit)
        if row.get("trade_date") == trade_date
    ]
    reconcile_rows = [
        row
        for row in list_live_sim_reconcile_snapshots(connection, limit=bounded_limit)
        if row.get("trade_date") == trade_date
    ]
    errors = [
        row
        for row in list_live_sim_errors(connection, limit=bounded_limit)
        if str(row.get("created_at") or "").startswith(trade_date)
        or row.get("live_sim_intent_id") in {item.get("live_sim_intent_id") for item in intents}
        or row.get("live_sim_order_id") in {item.get("live_sim_order_id") for item in orders}
    ]
    warnings = [
        "LIVE_SIM review context is read-only and cannot create order actions.",
        "AI insight is optional and is not Strategy/Risk/OMS input.",
    ]
    sections = [
        _section(
            "live_sim_review_policy",
            "ai_sidecar_policy",
            _live_sim_review_policy_payload(),
            row_count=1,
        ),
        _section(
            "live_sim_session_status",
            "live_sim_status",
            get_live_sim_status(connection, resolved_settings),
            row_count=1,
        ),
        _section(
            "live_sim_session_activity",
            "live_sim_session_tables",
            {
                "trade_date": trade_date,
                "intents": _compact_live_sim_rows(intents),
                "orders": _compact_live_sim_rows(orders),
                "executions": _compact_live_sim_rows(executions),
                "rejections": _compact_live_sim_rows(rejections),
                "reconcile_snapshots": _compact_live_sim_rows(reconcile_rows),
                "errors": _compact_live_sim_rows(errors),
            },
            row_count=len(intents) + len(orders) + len(executions) + len(rejections),
        ),
        _section(
            "live_sim_gateway_status",
            "gateway_commands",
            {
                "commands": _context_gateway_commands(
                    connection,
                    command_ids=command_ids,
                    include_live_sim=True,
                    limit=bounded_limit,
                ),
                "command_events": _context_gateway_command_events(connection, command_ids),
            },
            row_count=len(command_ids),
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.OPS_INCIDENT_SUMMARY,
        trade_date=trade_date,
        related_entity_type="live_sim_session",
        related_entity_id=trade_date,
        sections=sections,
        settings=resolved_settings,
        warnings=warnings,
    )


def build_live_sim_order_context(
    connection: sqlite3.Connection,
    live_sim_order_id: str,
    settings: Settings | None = None,
    limit: int | None = None,
) -> AISidecarContextPacket:
    from services.live_sim.live_sim_service import get_live_sim_intent, get_live_sim_order

    resolved_settings = settings or load_settings()
    bounded_limit = _bounded_limit(limit, resolved_settings)
    order = get_live_sim_order(connection, live_sim_order_id) if live_sim_order_id else None
    intent = (
        get_live_sim_intent(connection, str(order.get("live_sim_intent_id")))
        if order and order.get("live_sim_intent_id")
        else None
    )
    command_id = str(order.get("gateway_command_id") or "") if order else ""
    executions = _context_live_sim_executions_for_order(connection, order or {})
    reconcile_rows = _context_live_sim_reconcile_for_order(
        connection,
        order or {},
        limit=bounded_limit,
    )
    missing = [] if order else ["LIVE_SIM_ORDER_NOT_FOUND"]
    sections = [
        _section(
            "live_sim_review_policy",
            "ai_sidecar_policy",
            _live_sim_review_policy_payload(),
            row_count=1,
        ),
        _section(
            "live_sim_order_detail",
            "live_sim_orders",
            order or {},
            row_count=1 if order else 0,
            missing=order is None,
        ),
        _section(
            "live_sim_intent_detail",
            "live_sim_intents",
            intent or {},
            row_count=1 if intent else 0,
            missing=order is not None and intent is None,
        ),
        _section(
            "live_sim_order_gateway",
            "gateway_commands",
            {
                "command": _context_gateway_command(connection, command_id) if command_id else {},
                "command_events": (
                    _context_gateway_command_events(connection, [command_id]) if command_id else []
                ),
            },
            row_count=1 if command_id else 0,
        ),
        _section(
            "live_sim_order_execution_reconcile",
            "live_sim_execution_and_reconcile",
            {"executions": executions, "reconcile_snapshots": reconcile_rows},
            row_count=len(executions) + len(reconcile_rows),
        ),
        _section(
            "live_sim_order_safety_reminders",
            "dashboard",
            {
                "review_only": True,
                "order_action_allowed": False,
                "live_real_allowed": False,
                "gateway_command_allowed": False,
            },
            row_count=1,
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.TRADE_REVIEW,
        trade_date=order.get("trade_date") if order else None,
        related_entity_type="live_sim_order",
        related_entity_id=live_sim_order_id,
        sections=sections,
        settings=resolved_settings,
        missing_sections=missing,
        warnings=("LIVE_SIM_ORDER_REVIEW_CONTEXT_ONLY",),
    )


def build_live_sim_reconcile_context(
    connection: sqlite3.Connection,
    reconcile_id: str,
    settings: Settings | None = None,
    limit: int | None = None,
) -> AISidecarContextPacket:
    from services.live_sim.live_sim_service import list_live_sim_orders

    resolved_settings = settings or load_settings()
    bounded_limit = _bounded_limit(limit, resolved_settings)
    snapshot = _context_live_sim_reconcile_snapshot(connection, reconcile_id)
    trade_date = snapshot.get("trade_date") if snapshot else None
    code = snapshot.get("code") if snapshot else None
    open_orders = (
        list_live_sim_orders(
            connection,
            trade_date=trade_date,
            code=code,
            limit=bounded_limit,
            open_only=True,
        )
        if snapshot
        else []
    )
    command_ids = _live_sim_command_ids([], open_orders)
    missing = [] if snapshot else ["LIVE_SIM_RECONCILE_NOT_FOUND"]
    sections = [
        _section(
            "live_sim_review_policy",
            "ai_sidecar_policy",
            _live_sim_review_policy_payload(),
            row_count=1,
        ),
        _section(
            "live_sim_reconcile_snapshot",
            "live_sim_reconcile_snapshots",
            snapshot or {},
            row_count=1 if snapshot else 0,
            missing=snapshot is None,
        ),
        _section(
            "live_sim_reconcile_local_state",
            "live_sim_orders_and_executions",
            {
                "local_open_orders": open_orders,
                "local_executions": _context_live_sim_executions_for_orders(
                    connection,
                    open_orders,
                    limit=bounded_limit,
                ),
            },
            row_count=len(open_orders),
        ),
        _section(
            "live_sim_reconcile_gateway_status",
            "gateway_commands",
            {
                "commands": _context_gateway_commands(
                    connection,
                    command_ids=command_ids,
                    limit=bounded_limit,
                ),
                "command_events": _context_gateway_command_events(connection, command_ids),
            },
            row_count=len(command_ids),
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.OPS_INCIDENT_SUMMARY,
        trade_date=trade_date,
        related_entity_type="live_sim_reconcile",
        related_entity_id=reconcile_id,
        sections=sections,
        settings=resolved_settings,
        missing_sections=missing,
        warnings=("LIVE_SIM_RECONCILE_REVIEW_CONTEXT_ONLY",),
    )


def build_live_sim_incident_context(
    connection: sqlite3.Connection,
    related_entity_id: str | None = None,
    trade_date: str | None = None,
    settings: Settings | None = None,
    limit: int | None = None,
) -> AISidecarContextPacket:
    from services.live_sim.live_sim_service import (
        list_live_sim_errors,
        list_live_sim_reconcile_snapshots,
        list_live_sim_rejections,
    )

    resolved_settings = settings or load_settings()
    bounded_limit = _bounded_limit(limit, resolved_settings)
    rejections = list_live_sim_rejections(connection, limit=bounded_limit)
    reconcile_rows = [
        row
        for row in list_live_sim_reconcile_snapshots(connection, limit=bounded_limit)
        if int(row.get("mismatch_count") or 0) > 0
    ]
    if trade_date:
        rejections = [row for row in rejections if row.get("trade_date") == trade_date]
        reconcile_rows = [row for row in reconcile_rows if row.get("trade_date") == trade_date]
    sections = [
        _section(
            "live_sim_review_policy",
            "ai_sidecar_policy",
            _live_sim_review_policy_payload(),
            row_count=1,
        ),
        _section(
            "live_sim_incident_errors",
            "live_sim_errors",
            {"items": list_live_sim_errors(connection, limit=bounded_limit)},
            row_count=bounded_limit,
        ),
        _section(
            "live_sim_incident_rejections",
            "live_sim_rejections",
            {"items": rejections},
            row_count=len(rejections),
        ),
        _section(
            "live_sim_incident_reconcile_mismatches",
            "live_sim_reconcile_snapshots",
            {"items": reconcile_rows},
            row_count=len(reconcile_rows),
        ),
        _section(
            "live_sim_incident_gateway_problems",
            "gateway_events_and_commands",
            {
                "gateway_problem_events": _context_gateway_problem_events(
                    connection,
                    limit=bounded_limit,
                ),
                "failed_commands": _context_gateway_failed_commands(
                    connection,
                    limit=bounded_limit,
                ),
            },
            row_count=bounded_limit,
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.OPS_INCIDENT_SUMMARY,
        trade_date=trade_date,
        related_entity_type="live_sim_incident" if related_entity_id else None,
        related_entity_id=related_entity_id,
        sections=sections,
        settings=resolved_settings,
        warnings=("LIVE_SIM_INCIDENT_REVIEW_CONTEXT_ONLY",),
    )


def build_ops_incident_context(
    connection: sqlite3.Connection,
    related_entity_id: str | None = None,
    trade_date: str | None = None,
    settings: Settings | None = None,
    limit: int | None = None,
) -> AISidecarContextPacket:
    resolved_settings = settings or load_settings()
    bounded_limit = _bounded_limit(limit, resolved_settings)
    recent_events = list_recent_gateway_events(connection, limit=bounded_limit)
    problem_events = [
        event
        for event in recent_events
        if event.get("status") in {"REJECTED", "UNKNOWN_EVENT_TYPE", "CONFLICT"}
        or event.get("error_message")
    ]
    sections = [
        _section("gateway_status", "gateway_status", _gateway_status(connection), row_count=1),
        _section(
            "gateway_problem_events",
            "gateway_events",
            {"items": _gateway_event_summaries(problem_events)},
            row_count=len(problem_events),
        ),
        _section(
            "gateway_transport_status",
            "gateway_commands",
            _gateway_transport_status(connection),
            row_count=1,
        ),
        _section(
            "error_summary",
            "projection_and_evaluation_errors",
            _error_summary(connection, settings=resolved_settings, limit=bounded_limit),
            row_count=bounded_limit,
        ),
        _section(
            "recent_heartbeat_timestamps",
            "gateway_status",
            {"last_heartbeat_at": get_gateway_status_values(connection).get("last_heartbeat_at")},
            row_count=1,
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.OPS_INCIDENT_SUMMARY,
        trade_date=trade_date,
        related_entity_type="incident" if related_entity_id else None,
        related_entity_id=related_entity_id,
        sections=sections,
        settings=resolved_settings,
    )


def build_codex_prompt_context(
    connection: sqlite3.Connection,
    related_entity_id: str | None = None,
    trade_date: str | None = None,
    settings: Settings | None = None,
    limit: int | None = None,
    related_entity_type: str | None = None,
) -> AISidecarContextPacket:
    resolved_settings = settings or load_settings()
    bounded_limit = _bounded_limit(limit, resolved_settings)
    selected_summary = _selected_observation_summary(
        connection,
        related_entity_id=related_entity_id,
        related_entity_type=related_entity_type,
        limit=bounded_limit,
    )
    sections = [
        _section(
            "selected_observation_summary",
            "read_only_projection_state",
            selected_summary,
            row_count=1,
        ),
        _section(
            "docs_pointers",
            "docs",
            {
                "items": [
                    "docs/ai_sidecar_architecture.md",
                    "docs/ai_sidecar_safety_policy.md",
                    "docs/ai_context_builder.md",
                    "docs/dashboard_v1.md",
                    "docs/roadmap.md",
                ]
            },
            row_count=5,
        ),
        _section(
            "recent_errors",
            "projection_and_evaluation_errors",
            _error_summary(connection, settings=resolved_settings, limit=bounded_limit),
            row_count=bounded_limit,
        ),
        _section(
            "safety_policy_summary",
            "ai_sidecar_policy",
            _ai_safety_summary(resolved_settings),
            row_count=1,
        ),
        _section(
            "forbidden_scope_summary",
            "ai_sidecar_policy",
            {
                "automatic_code_changes": False,
                "automatic_branch_commit_pr": False,
                "prompt_body_generation": False,
                "forbidden_actions": get_forbidden_actions(),
                "note": "PR AI-1 builds context only; human prompt drafting is later scope.",
            },
            row_count=1,
        ),
    ]
    return _finalize_packet(
        AISidecarTaskType.CODEX_PROMPT_DRAFT,
        trade_date=trade_date,
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        sections=sections,
        settings=resolved_settings,
        warnings=("CODEX_PROMPT_CONTEXT_ONLY",),
    )


def build_context_builder_status(settings: Settings) -> dict[str, Any]:
    return {
        "context_builder_available": True,
        "openai_client_available": False,
        "execution_api_available": False,
        "enabled": settings.ai_sidecar_enabled,
        "builder_enabled": settings.ai_sidecar_context_builder_enabled,
        "max_context_chars": settings.ai_sidecar_max_context_chars,
        "default_limit": settings.ai_sidecar_context_default_limit,
        "max_limit": settings.ai_sidecar_context_max_limit,
        "allow_intraday": settings.ai_sidecar_intraday_allowed,
        "allow_order_context": settings.ai_sidecar_order_context_allowed,
        "order_context_allowed": settings.ai_sidecar_order_context_allowed,
        "supported_tasks": get_allowed_tasks(),
        "schema_version": settings.ai_sidecar_context_schema_version,
    }


def _finalize_packet(
    task_type: AISidecarTaskType | str,
    *,
    trade_date: str | None,
    related_entity_type: str | None,
    related_entity_id: str | None,
    sections: Sequence[AISidecarContextSection],
    settings: Settings,
    missing_sections: Sequence[str] = (),
    warnings: Sequence[str] = (),
) -> AISidecarContextPacket:
    task = parse_task_type(task_type)
    stripped_sections = [
        replace(section, payload=_strip_volatile_fields(section.payload)) for section in sections
    ]
    redacted_sections = [
        replace(section, payload=redact_context(section.payload)) for section in stripped_sections
    ]
    redaction_applied = [section.to_dict() for section in stripped_sections] != [
        section.to_dict() for section in redacted_sections
    ]
    policy_sections, policy_warnings = sanitize_context_sections(
        redacted_sections,
        allow_order_context=settings.ai_sidecar_order_context_allowed,
    )
    all_warnings = _unique([*warnings, *policy_warnings])
    all_missing = _unique(
        [
            *missing_sections,
            *[
                section.section_name.upper()
                for section in policy_sections
                if section.missing and section.section_name.upper() not in missing_sections
            ],
        ]
    )
    packet = _make_packet(
        task,
        trade_date=trade_date,
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        sections=policy_sections,
        settings=settings,
        missing_sections=all_missing,
        warnings=all_warnings,
        redaction_applied=redaction_applied,
        truncated=any(section.truncated for section in policy_sections),
    )
    if packet.size_chars <= packet.max_size_chars:
        return packet
    return _truncate_packet(packet, settings)


def _make_packet(
    task_type: AISidecarTaskType | str,
    *,
    trade_date: str | None,
    related_entity_type: str | None,
    related_entity_id: str | None,
    sections: Sequence[AISidecarContextSection],
    settings: Settings,
    missing_sections: Sequence[str],
    warnings: Sequence[str],
    redaction_applied: bool,
    truncated: bool,
) -> AISidecarContextPacket:
    task = parse_task_type(task_type)
    payload = {section.section_name: section.payload for section in sections}
    packet_data = {
        "task_type": task.value,
        "schema_version": settings.ai_sidecar_context_schema_version,
        "trade_date": trade_date,
        "related_entity_type": related_entity_type,
        "related_entity_id": related_entity_id,
        "source_sections": [section.to_dict() for section in sections],
        "truncated": truncated,
        "redaction_applied": redaction_applied,
        "order_context_included": settings.ai_sidecar_order_context_allowed,
        "missing_sections": list(missing_sections),
        "warnings": list(warnings),
        "payload": payload,
    }
    context_hash = calculate_context_hash(packet_hash_material(packet_data))
    packet = AISidecarContextPacket(
        context_id=_context_id(task, context_hash),
        task_type=task,
        schema_version=settings.ai_sidecar_context_schema_version or SCHEMA_VERSION,
        trade_date=trade_date,
        related_entity_type=related_entity_type,
        related_entity_id=related_entity_id,
        generated_at=datetime_to_wire(utc_now()),
        source_sections=sections,
        context_hash=context_hash,
        size_chars=0,
        max_size_chars=settings.ai_sidecar_max_context_chars,
        truncated=truncated,
        redaction_applied=redaction_applied,
        order_context_included=settings.ai_sidecar_order_context_allowed,
        missing_sections=missing_sections,
        warnings=warnings,
        payload=payload,
    )
    return _with_stable_size(packet)


def _truncate_packet(packet: AISidecarContextPacket, settings: Settings) -> AISidecarContextPacket:
    sections = list(packet.source_sections)
    spec = TASK_SPECS[packet.task_type]
    optional_names = set(spec.optional_sections)
    warnings = _unique([*packet.warnings, "CONTEXT_TRUNCATED"])
    truncated_sections: list[AISidecarContextSection] = []
    for section in sections:
        if section.section_name in optional_names:
            truncated_sections.append(
                replace(
                    section,
                    truncated=True,
                    payload={"summary": "Section omitted because max context size was exceeded."},
                )
            )
        else:
            truncated_sections.append(replace(section, payload=_compact_payload(section.payload)))
    rebuilt = _make_packet(
        packet.task_type,
        trade_date=packet.trade_date,
        related_entity_type=packet.related_entity_type,
        related_entity_id=packet.related_entity_id,
        sections=truncated_sections,
        settings=settings,
        missing_sections=packet.missing_sections,
        warnings=warnings,
        redaction_applied=packet.redaction_applied,
        truncated=True,
    )
    if rebuilt.size_chars <= rebuilt.max_size_chars:
        return rebuilt

    summary_sections = [
        replace(
            section,
            truncated=True,
            payload={"summary": "Summary-only context retained after size limit enforcement."},
        )
        for section in truncated_sections
        if section.section_name in spec.required_sections
    ]
    return _make_packet(
        packet.task_type,
        trade_date=packet.trade_date,
        related_entity_type=packet.related_entity_type,
        related_entity_id=packet.related_entity_id,
        sections=summary_sections,
        settings=settings,
        missing_sections=packet.missing_sections,
        warnings=warnings,
        redaction_applied=packet.redaction_applied,
        truncated=True,
    )


def _with_stable_size(packet: AISidecarContextPacket) -> AISidecarContextPacket:
    size = len(canonical_context_json(packet.to_dict()))
    for _ in range(3):
        updated = replace(packet, size_chars=size)
        next_size = len(canonical_context_json(updated.to_dict()))
        if next_size == size:
            return updated
        packet = updated
        size = next_size
    return replace(packet, size_chars=size)


def _section(
    section_name: str,
    source: str,
    payload: Mapping[str, Any] | None,
    *,
    row_count: int = 0,
    truncated: bool = False,
    missing: bool = False,
) -> AISidecarContextSection:
    return AISidecarContextSection(
        section_name=section_name,
        source=source,
        row_count=row_count,
        truncated=truncated,
        missing=missing,
        payload=payload or {},
    )


def _ai_safety_summary(settings: Settings) -> dict[str, Any]:
    return {
        "read_only": True,
        "context_builder_available": True,
        "openai_client_available": False,
        "ai_execution_available": False,
        "ai_insight_creation_available": False,
        "observe_only_pipeline": True,
        "trading_mode": settings.trading_mode.value,
        "allowed_tasks": get_allowed_tasks(),
        "forbidden_actions": get_forbidden_actions(),
        "warnings": build_safety_section(settings)["warnings"],
    }


def _gateway_status(connection: sqlite3.Connection) -> dict[str, Any]:
    values = get_gateway_status_values(connection)
    return {
        "last_event_received_at": values.get("last_event_received_at"),
        "last_heartbeat_at": values.get("last_heartbeat_at"),
        "event_count": _count_rows(connection, "gateway_events"),
        "raw_event_count": _count_rows(connection, "raw_events"),
        "transport_status_counts": _gateway_transport_status(connection)["status_counts"],
    }


def _gateway_transport_status(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM gateway_commands
        GROUP BY status
        """
    ).fetchall()
    counts = {status.value: 0 for status in GatewayCommandStatus}
    for row in rows:
        counts[str(row["status"])] = int(row["count"])
    return {"status_counts": counts, "transport_is_read_only_context": True}


def _pipeline_counts(connection: sqlite3.Connection, settings: Settings) -> dict[str, Any]:
    return {
        "gateway_events": _count_rows(connection, "gateway_events"),
        "market_data": get_market_data_status(connection, settings=settings),
        "themes": get_theme_status(connection, settings=settings),
        "candidates": get_candidate_status(connection, settings=settings),
        "strategy": get_strategy_status(connection, settings),
        "risk": get_risk_status(connection, settings),
    }


def _error_summary(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    limit: int,
) -> dict[str, Any]:
    errors = build_dashboard_errors(connection, settings=settings, limit=limit)
    return {
        "market_projection_errors": errors["market_projection_errors"],
        "theme_projection_errors": errors["theme_projection_errors"],
        "candidate_projection_errors": errors["candidate_projection_errors"],
        "strategy_errors": errors["strategy_errors"],
        "risk_errors": errors["risk_errors"],
        "gateway_problem_events": _gateway_event_summaries(errors["gateway_problem_events"]),
    }


def _related_errors(
    connection: sqlite3.Connection,
    *,
    candidate_instance_id: str,
    code: str | None,
    limit: int,
) -> dict[str, Any]:
    market_errors = [
        error
        for error in list_projection_errors(connection, limit=limit)
        if code and error.get("code") == code
    ]
    theme_errors = [
        error
        for error in list_theme_projection_errors(connection, limit=limit)
        if code and error.get("code") == code
    ]
    candidate_errors = [
        error
        for error in list_candidate_projection_errors(connection, limit=limit)
        if error.get("candidate_instance_id") == candidate_instance_id
        or (code and error.get("code") == code)
    ]
    strategy_errors = [
        error
        for error in list_strategy_errors(connection, limit=limit)
        if error.get("candidate_instance_id") == candidate_instance_id
        or (code and error.get("code") == code)
    ]
    risk_errors = [
        error
        for error in list_risk_errors(connection, limit=limit)
        if error.get("candidate_instance_id") == candidate_instance_id
        or (code and error.get("code") == code)
    ]
    return {
        "market_projection_errors": market_errors,
        "theme_projection_errors": theme_errors,
        "candidate_projection_errors": candidate_errors,
        "strategy_errors": strategy_errors,
        "risk_errors": risk_errors,
    }


def _reason_code_summary(
    connection: sqlite3.Connection,
    *,
    trade_date: str,
    limit: int,
) -> dict[str, Any]:
    return {
        "candidate_reason_codes": _top_reason_codes(
            connection,
            table_name="candidates",
            reason_column="reason_codes_json",
            where="trade_date = ?",
            params=(trade_date,),
            limit=limit,
        ),
        "strategy_reason_codes": _top_reason_codes(
            connection,
            table_name="strategy_observations_latest",
            reason_column="reason_codes_json",
            where="trade_date = ?",
            params=(trade_date,),
            limit=limit,
        ),
        "risk_reason_codes": _top_reason_codes(
            connection,
            table_name="risk_observations_latest",
            reason_column="reason_codes_json",
            where="trade_date = ?",
            params=(trade_date,),
            limit=limit,
        ),
    }


def _top_reason_codes(
    connection: sqlite3.Connection,
    *,
    table_name: str,
    reason_column: str,
    where: str,
    params: tuple[Any, ...],
    limit: int,
) -> list[dict[str, Any]]:
    import json

    rows = connection.execute(
        f"""
        SELECT {reason_column} AS reason_codes_json
        FROM {table_name}
        WHERE {where}
        LIMIT ?
        """,
        (*params, max(limit, 1)),
    ).fetchall()
    counter: Counter[str] = Counter()
    for row in rows:
        try:
            reasons = json.loads(row["reason_codes_json"])
        except (TypeError, ValueError):
            reasons = []
        if isinstance(reasons, list):
            for reason in reasons:
                counter[str(reason)] += 1
    return [{"reason_code": reason, "count": count} for reason, count in counter.most_common(20)]


def _selected_observation_summary(
    connection: sqlite3.Connection,
    *,
    related_entity_id: str | None,
    related_entity_type: str | None,
    limit: int,
) -> dict[str, Any]:
    if related_entity_id and (related_entity_type or "").lower() in {
        "candidate",
        "candidate_instance",
    }:
        candidate = get_candidate(connection, related_entity_id, include_context=True)
        return {
            "candidate": candidate,
            "strategy": (
                get_latest_strategy_observation(
                    connection,
                    related_entity_id,
                    include_setups=True,
                )
                if candidate
                else None
            ),
            "risk": (
                get_latest_risk_observation(connection, related_entity_id, include_checks=True)
                if candidate
                else None
            ),
        }
    if related_entity_id:
        candidate = get_candidate(connection, related_entity_id, include_context=True)
        if candidate:
            return {
                "candidate": candidate,
                "strategy": get_latest_strategy_observation(
                    connection,
                    related_entity_id,
                    include_setups=True,
                ),
                "risk": get_latest_risk_observation(
                    connection,
                    related_entity_id,
                    include_checks=True,
                ),
            }
    return {
        "recent_candidates": list_candidates(connection, active_only=False, limit=limit),
        "recent_strategy_observations": list_latest_strategy_observations(connection, limit=limit),
        "recent_risk_observations": list_latest_risk_observations(connection, limit=limit),
    }


def _gateway_event_summaries(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "event_id": event.get("event_id"),
            "event_type": event.get("event_type"),
            "source": event.get("source"),
            "event_ts": event.get("event_ts"),
            "received_at": event.get("received_at"),
            "status": event.get("status"),
            "error_message": event.get("error_message"),
        }
        for event in events
    ]


def _compact_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    keep_keys: Sequence[str],
) -> list[dict[str, Any]]:
    keep = set(keep_keys)
    return [{key: value for key, value in row.items() if key in keep} for row in rows]


def _drop_nested(mapping: Mapping[str, Any], keys: Sequence[str]) -> dict[str, Any]:
    return {key: value for key, value in mapping.items() if key not in set(keys)}


def _strip_volatile_fields(value: object) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_volatile_fields(item)
            for key, item in value.items()
            if str(key) not in {"tick_age_sec"}
        }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_strip_volatile_fields(item) for item in value]
    return value


def _compact_payload(value: object) -> Any:
    if isinstance(value, Mapping):
        compacted: dict[str, Any] = {}
        for key, item in value.items():
            if key in {"payload", "raw_payload", "metadata"}:
                continue
            compacted[str(key)] = _compact_payload(item)
        return compacted
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_compact_payload(item) for item in list(value)[:3]]
    return value


def _count_rows(
    connection: sqlite3.Connection,
    table_name: str,
    where: str | None = None,
    params: tuple[Any, ...] = (),
) -> int:
    where_sql = "" if where is None else f"WHERE {where}"
    row = connection.execute(
        f"SELECT COUNT(*) AS count FROM {table_name} {where_sql}",
        params,
    ).fetchone()
    return int(row["count"])


def _bounded_limit(limit: int | None, settings: Settings) -> int:
    default_limit = getattr(settings, "ai_sidecar_context_default_limit", DEFAULT_CONTEXT_LIMIT)
    max_limit = getattr(settings, "ai_sidecar_context_max_limit", MAX_CONTEXT_LIMIT)
    value = default_limit if limit is None else int(limit)
    return min(max(value, 1), max_limit)


def _default_trade_date(settings: Settings) -> str:
    return (
        datetime.now(candidate_timezone(settings.candidate_trade_date_timezone)).date().isoformat()
    )


def _context_id(task_type: AISidecarTaskType, context_hash: str) -> str:
    digest = context_hash[:20]
    return f"ai_ctx_{task_type.value.lower()}_{digest}"


def _unique(values: Sequence[str]) -> list[str]:
    return [*dict.fromkeys(str(value) for value in values if str(value).strip())]


def _is_live_sim_order_context(
    related_entity_type: str | None,
    related_entity_id: str | None,
) -> bool:
    normalized = str(related_entity_type or "").strip().lower()
    return normalized in {"live_sim_order", "live-sim-order"} or str(
        related_entity_id or ""
    ).startswith("live_sim_order")


def _is_live_sim_session_context(related_entity_type: str | None) -> bool:
    normalized = str(related_entity_type or "").strip().lower()
    return normalized in {"live_sim_session", "live-sim-session"}


def _is_live_sim_reconcile_context(
    related_entity_type: str | None,
    related_entity_id: str | None,
) -> bool:
    normalized = str(related_entity_type or "").strip().lower()
    return normalized in {"live_sim_reconcile", "live-sim-reconcile"} or str(
        related_entity_id or ""
    ).startswith("live_sim_reconcile")


def _is_live_sim_incident_context(related_entity_type: str | None) -> bool:
    normalized = str(related_entity_type or "").strip().lower()
    return normalized in {"live_sim_incident", "live-sim-incident"}


def _live_sim_review_policy_payload() -> dict[str, Any]:
    return {
        "read_only": True,
        "review_only": True,
        "deterministic_report_available_without_openai": True,
        "ai_run_default": False,
        "ai_output_is_order_input": False,
        "order_action_allowed": False,
        "gateway_command_allowed": False,
        "live_real_allowed": False,
        "forbidden_actions": [
            "send_order",
            "cancel_order",
            "modify_order",
            "order_retry",
            "live_real_enablement",
            "background_review_worker",
        ],
    }


def _live_sim_command_ids(
    intents: Sequence[Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
) -> list[str]:
    values = [
        *(row.get("gateway_command_id") for row in intents),
        *(row.get("gateway_command_id") for row in orders),
    ]
    return _unique([str(value) for value in values if value])


def _compact_live_sim_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    limit: int = 20,
) -> list[dict[str, Any]]:
    omitted_keys = {"payload", "raw_payload", "raw_event_json", "snapshot_json"}
    compacted = []
    for row in rows[:limit]:
        compacted.append({key: value for key, value in row.items() if key not in omitted_keys})
    return compacted


def _live_sim_related_executions(
    executions: Sequence[Mapping[str, Any]],
    *,
    intents: Sequence[Mapping[str, Any]],
    orders: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    order_ids = {str(row.get("live_sim_order_id")) for row in orders}
    intent_ids = {str(row.get("live_sim_intent_id")) for row in intents}
    if not order_ids and not intent_ids:
        return _compact_live_sim_rows(executions)
    return [
        dict(row)
        for row in executions
        if str(row.get("live_sim_order_id")) in order_ids
        or str(row.get("live_sim_intent_id")) in intent_ids
    ]


def _context_gateway_command(
    connection: sqlite3.Connection,
    command_id: str,
) -> dict[str, Any]:
    if not command_id:
        return {}
    row = connection.execute(
        """
        SELECT *
        FROM gateway_commands
        WHERE command_id = ?
        """,
        (command_id,),
    ).fetchone()
    if row is None:
        return {}
    item = _context_row_to_dict(row)
    item["payload"] = _context_json_object(item.pop("payload_json"))
    return item


def _context_gateway_commands(
    connection: sqlite3.Connection,
    *,
    command_ids: Sequence[str],
    include_live_sim: bool = False,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if command_ids:
        clauses.append(f"command_id IN ({', '.join('?' for _ in command_ids)})")
        params.extend(command_ids)
    if include_live_sim:
        clauses.append("(LOWER(source) = 'live_sim' OR LOWER(command_type) = 'send_order')")
    if not clauses:
        return []
    rows = connection.execute(
        f"""
        SELECT *
        FROM gateway_commands
        WHERE {" OR ".join(clauses)}
        ORDER BY created_at DESC, command_id DESC
        LIMIT ?
        """,
        (*params, min(max(int(limit), 1), 200)),
    ).fetchall()
    commands = []
    for row in rows:
        item = _context_row_to_dict(row)
        item["payload"] = _context_json_object(item.pop("payload_json"))
        commands.append(item)
    return commands


def _context_gateway_command_events(
    connection: sqlite3.Connection,
    command_ids: Sequence[str],
) -> list[dict[str, Any]]:
    ids = [str(command_id) for command_id in command_ids if str(command_id).strip()]
    if not ids:
        return []
    rows = connection.execute(
        f"""
        SELECT *
        FROM gateway_command_events
        WHERE command_id IN ({', '.join('?' for _ in ids)})
        ORDER BY id ASC
        """,
        tuple(ids),
    ).fetchall()
    events = []
    for row in rows:
        item = _context_row_to_dict(row)
        item["payload"] = _context_json_object(item.pop("payload_json"))
        events.append(item)
    return events


def _context_gateway_problem_events(
    connection: sqlite3.Connection,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM gateway_events
        WHERE status NOT IN ('ACCEPTED')
            OR error_message IS NOT NULL
        ORDER BY received_at DESC, event_id DESC
        LIMIT ?
        """,
        (min(max(int(limit), 1), 200),),
    ).fetchall()
    events = []
    for row in rows:
        item = _context_row_to_dict(row)
        item["payload"] = _context_json_object(item.pop("payload_json"))
        events.append(item)
    return events


def _context_gateway_failed_commands(
    connection: sqlite3.Connection,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT *
        FROM gateway_commands
        WHERE status IN ('FAILED', 'REJECTED', 'EXPIRED')
        ORDER BY COALESCE(completed_at, created_at) DESC, command_id DESC
        LIMIT ?
        """,
        (min(max(int(limit), 1), 200),),
    ).fetchall()
    commands = []
    for row in rows:
        item = _context_row_to_dict(row)
        item["payload"] = _context_json_object(item.pop("payload_json"))
        commands.append(item)
    return commands


def _context_live_sim_executions_for_order(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not order:
        return []
    rows = connection.execute(
        """
        SELECT *
        FROM live_sim_executions
        WHERE live_sim_order_id = ?
            OR broker_order_no = ?
        ORDER BY executed_at ASC, live_sim_execution_id ASC
        """,
        (order.get("live_sim_order_id"), order.get("broker_order_no")),
    ).fetchall()
    return [_context_live_sim_execution_row(row) for row in rows]


def _context_live_sim_executions_for_orders(
    connection: sqlite3.Connection,
    orders: Sequence[Mapping[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    order_ids = [
        str(row.get("live_sim_order_id")) for row in orders if row.get("live_sim_order_id")
    ]
    if not order_ids:
        return []
    rows = connection.execute(
        f"""
        SELECT *
        FROM live_sim_executions
        WHERE live_sim_order_id IN ({', '.join('?' for _ in order_ids)})
        ORDER BY executed_at ASC, live_sim_execution_id ASC
        LIMIT ?
        """,
        (*order_ids, min(max(int(limit), 1), 200)),
    ).fetchall()
    return [_context_live_sim_execution_row(row) for row in rows]


def _context_live_sim_reconcile_for_order(
    connection: sqlite3.Connection,
    order: Mapping[str, Any],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not order:
        return []
    rows = connection.execute(
        """
        SELECT *
        FROM live_sim_reconcile_snapshots
        WHERE trade_date = ?
            AND (code = ? OR code IS NULL)
        ORDER BY created_at DESC, reconcile_id DESC
        LIMIT ?
        """,
        (order.get("trade_date"), order.get("code"), min(max(int(limit), 1), 200)),
    ).fetchall()
    snapshots = []
    for row in rows:
        item = _context_row_to_dict(row)
        item["snapshot_json"] = _context_json_object(item.pop("snapshot_json"))
        snapshots.append(item)
    return snapshots


def _context_live_sim_reconcile_snapshot(
    connection: sqlite3.Connection,
    reconcile_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT *
        FROM live_sim_reconcile_snapshots
        WHERE reconcile_id = ?
        """,
        (reconcile_id,),
    ).fetchone()
    if row is None:
        return None
    item = _context_row_to_dict(row)
    item["snapshot_json"] = _context_json_object(item.pop("snapshot_json"))
    return item


def _context_live_sim_execution_row(row: sqlite3.Row) -> dict[str, Any]:
    item = _context_row_to_dict(row)
    item["raw_event_json"] = _context_json_object(item.pop("raw_event_json"))
    return item


def _context_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _context_json_object(value: object) -> dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    loaded = json.loads(str(value))
    return loaded if isinstance(loaded, dict) else {}


_THEME_MEMBER_KEYS = (
    "snapshot_id",
    "theme_id",
    "code",
    "name",
    "price",
    "change_rate",
    "cumulative_trade_value",
    "trade_value_delta_1m",
    "trade_value_delta_3m",
    "readiness_status",
    "member_role",
    "event_ts",
)
