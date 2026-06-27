from __future__ import annotations

import json
import sqlite3
from collections import Counter
from typing import Any, Literal

from domain.ai_sidecar.policy import get_allowed_tasks, get_forbidden_actions
from domain.ai_sidecar.schemas import insight_row_to_dict
from domain.broker.utils import datetime_to_wire, utc_now
from domain.candidate.state import CandidateState
from domain.risk.status import RiskObservationStatus
from domain.strategy.status import StrategyObservationStatus
from domain.theme.state import ThemeState
from storage.event_store import (
    count_recent_gateway_events,
    get_gateway_status_values,
    list_recent_gateway_events,
)

from services.candidate_service import (
    get_candidate,
    get_candidate_status,
    list_candidate_projection_errors,
    list_candidates,
)
from services.config import Settings
from services.market_data_service import (
    get_market_data_status,
    list_latest_ticks,
    list_projection_errors,
)
from services.risk_gate import (
    get_risk_status,
    list_latest_risk_observations,
    list_risk_check_observations,
    list_risk_errors,
)
from services.strategy_engine import (
    get_strategy_status,
    list_latest_strategy_observations,
    list_strategy_errors,
    list_strategy_setup_observations,
)
from services.theme_service import (
    get_theme_status,
    list_latest_theme_snapshots,
    list_theme_projection_errors,
)

DashboardDetail = Literal["summary", "full"]

DASHBOARD_SECTIONS = [
    "safety",
    "system",
    "gateway",
    "market_data",
    "themes",
    "candidates",
    "strategy",
    "risk",
    "ai_sidecar",
    "recent_events",
    "errors",
    "pipeline_summary",
]

COMMAND_STATUSES = (
    "QUEUED",
    "DISPATCHED",
    "ACKED",
    "REJECTED",
    "FAILED",
    "EXPIRED",
    "CANCELLED",
)


def build_dashboard_snapshot(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    detail: DashboardDetail = "summary",
    limit: int | None = None,
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit, settings)
    include_detail = detail == "full"
    generated_at = _now()

    gateway_status_values = get_gateway_status_values(connection)
    command_counts = _command_status_counts(connection)
    gateway_recent_events = list_recent_gateway_events(connection, limit=bounded_limit)
    gateway_status = _gateway_status_section(
        settings,
        gateway_status_values,
        command_counts,
        recent_event_count=count_recent_gateway_events(connection),
    )

    market_data_status = get_market_data_status(connection, settings=settings)
    theme_status = get_theme_status(connection, settings=settings)
    candidate_status = get_candidate_status(connection, settings=settings)
    strategy_status = get_strategy_status(connection, settings)
    risk_status = get_risk_status(connection, settings)

    latest_ticks = list_latest_ticks(connection, limit=bounded_limit)
    latest_theme_snapshots = list_latest_theme_snapshots(connection, limit=bounded_limit)
    candidates = list_candidates(connection, active_only=True, limit=bounded_limit)
    strategy_observations = list_latest_strategy_observations(connection, limit=bounded_limit)
    risk_observations = list_latest_risk_observations(connection, limit=bounded_limit)
    ai_insights = list_ai_insights(connection, limit=bounded_limit)

    strategy_status_counts = _enum_counts(
        connection,
        "strategy_observations_latest",
        "overall_status",
        StrategyObservationStatus,
    )
    risk_status_counts = _enum_counts(
        connection,
        "risk_observations_latest",
        "overall_status",
        RiskObservationStatus,
    )
    theme_state_counts = _enum_counts(
        connection,
        "theme_latest_snapshots",
        "state",
        ThemeState,
    )

    errors = build_dashboard_errors(connection, settings=settings, limit=bounded_limit)
    pipeline_summary = _pipeline_summary(
        gateway_status=gateway_status,
        market_data_status=market_data_status,
        theme_status=theme_status,
        theme_state_counts=theme_state_counts,
        candidate_status=candidate_status,
        strategy_status=strategy_status,
        strategy_status_counts=strategy_status_counts,
        risk_status=risk_status,
        risk_status_counts=risk_status_counts,
        ai_insights=ai_insights,
        settings=settings,
    )

    return {
        "generated_at": generated_at,
        "detail": detail,
        "limit": bounded_limit,
        "safety": build_safety_section(settings),
        "system": _system_section(settings, generated_at),
        "gateway": gateway_status,
        "market_data": {
            "status": market_data_status,
            "latest_ticks": latest_ticks,
        },
        "themes": {
            "status": theme_status,
            "latest_snapshots": latest_theme_snapshots,
            "state_counts": theme_state_counts,
            "top_leading_themes": _filter_theme_state(latest_theme_snapshots, "LEADING"),
            "top_spreading_themes": _filter_theme_state(latest_theme_snapshots, "SPREADING"),
        },
        "candidates": {
            "status": candidate_status,
            "state_counts": candidate_status["state_counts"],
            "candidates": _candidate_rows(connection, candidates, include_detail),
            "details_included": include_detail,
        },
        "strategy": {
            "status": strategy_status,
            "status_counts": strategy_status_counts,
            "latest_observations": _strategy_rows(
                connection,
                strategy_observations,
                include_detail,
            ),
            "matched_observations": _filter_status(
                strategy_observations,
                "MATCHED_OBSERVATION",
            ),
            "forming_observations": _filter_status(strategy_observations, "FORMING"),
            "notice": "MATCHED_OBSERVATION은 매수 신호가 아닙니다.",
            "details_included": include_detail,
        },
        "risk": {
            "status": risk_status,
            "status_counts": risk_status_counts,
            "latest_observations": _risk_rows(connection, risk_observations, include_detail),
            "top_blocks": _filter_status(risk_observations, "OBSERVE_BLOCK"),
            "top_cautions": _filter_status(risk_observations, "OBSERVE_CAUTION"),
            "reason_code_counts": _reason_code_counts(risk_observations),
            "notice": "OBSERVE_PASS는 주문 승인이 아닙니다.",
            "details_included": include_detail,
        },
        "ai_sidecar": {
            "status": build_ai_sidecar_status(settings),
            "insights": ai_insights,
            "insight_count": len(ai_insights),
            "execution_controls_available": False,
            "notice": "AI Sidecar는 read-only 표시 전용이며 이번 PR에서 실행하지 않습니다.",
        },
        "recent_events": {
            "gateway_events": gateway_recent_events,
        },
        "errors": errors,
        "pipeline_summary": pipeline_summary,
    }


def build_dashboard_status(settings: Settings) -> dict[str, Any]:
    return {
        "enabled": settings.dashboard_enabled,
        "generated_at": _now(),
        "read_only": True,
        "order_controls_available": False,
        "ai_execution_available": False,
        "route_enabled": settings.dashboard_route_enabled,
        "refresh_sec": settings.dashboard_refresh_sec,
        "snapshot_default_limit": settings.dashboard_snapshot_default_limit,
        "max_limit": settings.dashboard_max_limit,
        "show_raw_json": settings.dashboard_show_raw_json,
        "sections": DASHBOARD_SECTIONS,
    }


def build_safety_section(settings: Settings) -> dict[str, Any]:
    warnings = [
        "현재 Dashboard는 읽기 전용이며 주문 기능이 없습니다.",
        "OBSERVE_PASS는 주문 승인이 아닙니다.",
        "MATCHED_OBSERVATION은 매수 신호가 아닙니다.",
    ]
    if settings.live_sim_allowed or settings.live_real_allowed:
        warnings.append(
            "LIVE flag가 켜져 있어도 이 PR에는 주문 라우팅이 없어서 주문이 전송되지 않습니다."
        )
    return {
        "trading_mode": settings.trading_mode.value,
        "live_sim_allowed": settings.live_sim_allowed,
        "live_real_allowed": settings.live_real_allowed,
        "order_routing_enabled": False,
        "order_controls_available": False,
        "gateway_order_commands_allowed": False,
        "ai_sidecar_enabled": settings.ai_sidecar_enabled,
        "ai_execution_available": False,
        "openai_client_available": False,
        "observe_only_pipeline": True,
        "warnings": warnings,
    }


def build_ai_sidecar_status(settings: Settings) -> dict[str, Any]:
    return {
        "enabled": settings.ai_sidecar_enabled,
        "allow_intraday": settings.ai_sidecar_intraday_allowed,
        "allow_order_context": settings.ai_sidecar_order_context_allowed,
        "model": settings.ai_sidecar_model,
        "allowed_tasks": get_allowed_tasks(),
        "forbidden_actions": get_forbidden_actions(),
        "openai_client_available": False,
        "execution_api_available": False,
    }


def build_dashboard_errors(
    connection: sqlite3.Connection,
    *,
    settings: Settings,
    limit: int | None = None,
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit, settings)
    recent_gateway_events = list_recent_gateway_events(connection, limit=bounded_limit)
    gateway_problem_events = [
        event
        for event in recent_gateway_events
        if event.get("status") not in {"ACCEPTED", None} or event.get("error_message")
    ]
    return {
        "market_projection_errors": list_projection_errors(connection, limit=bounded_limit),
        "theme_projection_errors": list_theme_projection_errors(
            connection,
            limit=bounded_limit,
        ),
        "candidate_projection_errors": list_candidate_projection_errors(
            connection,
            limit=bounded_limit,
        ),
        "strategy_errors": list_strategy_errors(connection, limit=bounded_limit),
        "risk_errors": list_risk_errors(connection, limit=bounded_limit),
        "gateway_problem_events": gateway_problem_events,
        "gateway_command_failures": _list_gateway_command_failures(
            connection,
            limit=bounded_limit,
        ),
    }


def list_ai_insights(
    connection: sqlite3.Connection,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            insight_id,
            request_id,
            task_type,
            trade_date,
            related_entity_type,
            related_entity_id,
            summary,
            root_cause,
            severity,
            operator_action,
            schema_version,
            created_at
        FROM ai_insights
        ORDER BY created_at DESC, insight_id DESC
        LIMIT ?
        """,
        (max(int(limit), 1),),
    ).fetchall()
    return [insight_row_to_dict(row) for row in rows]


def _system_section(settings: Settings, generated_at: str) -> dict[str, Any]:
    return {
        "api_health": "ok",
        "status": "ok",
        "mode": settings.trading_mode.value,
        "token_required": bool(settings.trading_core_token),
        "database_path": str(settings.trading_db_path),
        "generated_at": generated_at,
        "dashboard": build_dashboard_status(settings),
    }


def _gateway_status_section(
    settings: Settings,
    status_values: dict[str, str],
    command_counts: dict[str, int],
    *,
    recent_event_count: int,
) -> dict[str, Any]:
    return {
        "last_event_received_at": status_values.get("last_event_received_at"),
        "last_heartbeat_at": status_values.get("last_heartbeat_at"),
        "queued_command_count": command_counts["QUEUED"],
        "dispatched_command_count": command_counts["DISPATCHED"],
        "acked_command_count": command_counts["ACKED"],
        "failed_command_count": command_counts["FAILED"],
        "rejected_command_count": command_counts["REJECTED"],
        "expired_command_count": command_counts["EXPIRED"],
        "recent_event_count": recent_event_count,
        "token_required": bool(settings.trading_core_token),
        "order_commands_allowed": False,
        "command_status_counts": command_counts,
    }


def _pipeline_summary(
    *,
    gateway_status: dict[str, Any],
    market_data_status: dict[str, Any],
    theme_status: dict[str, Any],
    theme_state_counts: dict[str, int],
    candidate_status: dict[str, Any],
    strategy_status: dict[str, Any],
    strategy_status_counts: dict[str, int],
    risk_status: dict[str, Any],
    risk_status_counts: dict[str, int],
    ai_insights: list[dict[str, Any]],
    settings: Settings,
) -> dict[str, Any]:
    return {
        "gateway": {
            "recent_event_count": gateway_status["recent_event_count"],
            "queued_command_count": gateway_status["queued_command_count"],
            "failed_command_count": gateway_status["failed_command_count"],
        },
        "market_data": {
            "latest_tick_count": market_data_status["latest_tick_count"],
            "bar_count": market_data_status["bar_count"],
            "projection_error_count": market_data_status["projection_error_count"],
        },
        "themes": {
            "theme_count": theme_status["theme_count"],
            "latest_snapshot_count": theme_status["latest_snapshot_count"],
            "state_counts": theme_state_counts,
        },
        "candidates": {
            "candidate_count": candidate_status["candidate_count"],
            "state_counts": candidate_status["state_counts"],
        },
        "strategy": {
            "latest_observation_count": strategy_status["latest_observation_count"],
            "status_counts": strategy_status_counts,
            "matched_observation_count": strategy_status["matched_observation_count"],
        },
        "risk": {
            "latest_observation_count": risk_status["latest_observation_count"],
            "status_counts": risk_status_counts,
            "observe_pass_count": risk_status["observe_pass_count"],
            "caution_count": risk_status["caution_count"],
            "block_count": risk_status["block_count"],
        },
        "ai_sidecar": {
            "enabled": settings.ai_sidecar_enabled,
            "insight_count": len(ai_insights),
            "execution_api_available": False,
        },
        "funnel": [
            {
                "key": "gateway_events",
                "label": "Gateway Events",
                "count": gateway_status["recent_event_count"],
            },
            {
                "key": "market_ticks",
                "label": "Market Ticks",
                "count": market_data_status["latest_tick_count"],
            },
            {
                "key": "theme_snapshots",
                "label": "Theme Snapshots",
                "count": theme_status["latest_snapshot_count"],
            },
            {
                "key": "candidates",
                "label": "Candidates",
                "count": candidate_status["active_candidate_count"],
            },
            {
                "key": "strategy_observations",
                "label": "Strategy Observations",
                "count": strategy_status["latest_observation_count"],
            },
            {
                "key": "risk_observations",
                "label": "Risk Observations",
                "count": risk_status["latest_observation_count"],
            },
        ],
    }


def _candidate_rows(
    connection: sqlite3.Connection,
    candidates: list[dict[str, Any]],
    include_detail: bool,
) -> list[dict[str, Any]]:
    if not include_detail:
        return candidates
    detailed = []
    for candidate in candidates:
        item = get_candidate(
            connection,
            candidate["candidate_instance_id"],
            include_context=True,
            include_sources=True,
            include_transitions=True,
        )
        detailed.append(item or candidate)
    return detailed


def _strategy_rows(
    connection: sqlite3.Connection,
    observations: list[dict[str, Any]],
    include_detail: bool,
) -> list[dict[str, Any]]:
    if not include_detail:
        return observations
    detailed = []
    for observation in observations:
        item = dict(observation)
        item["setup_observations"] = list_strategy_setup_observations(
            connection,
            observation["strategy_observation_id"],
        )
        detailed.append(item)
    return detailed


def _risk_rows(
    connection: sqlite3.Connection,
    observations: list[dict[str, Any]],
    include_detail: bool,
) -> list[dict[str, Any]]:
    if not include_detail:
        return observations
    detailed = []
    for observation in observations:
        item = dict(observation)
        item["check_observations"] = list_risk_check_observations(
            connection,
            observation["risk_observation_id"],
        )
        detailed.append(item)
    return detailed


def _filter_theme_state(rows: list[dict[str, Any]], state: str) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("state") == state]


def _filter_status(rows: list[dict[str, Any]], status: str) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("overall_status") == status]


def _reason_code_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        for reason in row.get("reason_codes", []):
            counter[str(reason)] += 1
    return dict(counter)


def _command_status_counts(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM gateway_commands
        GROUP BY status
        """
    ).fetchall()
    counts = {status: 0 for status in COMMAND_STATUSES}
    for row in rows:
        counts[str(row["status"])] = int(row["count"])
    return counts


def _enum_counts(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    enum_type: type[ThemeState] | type[CandidateState] | type[StrategyObservationStatus]
    | type[RiskObservationStatus],
) -> dict[str, int]:
    rows = connection.execute(
        f"""
        SELECT {column_name} AS value, COUNT(*) AS count
        FROM {table_name}
        GROUP BY {column_name}
        """
    ).fetchall()
    counts = {item.value: 0 for item in enum_type}
    for row in rows:
        counts[str(row["value"])] = int(row["count"])
    return counts


def _list_gateway_command_failures(
    connection: sqlite3.Connection,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT
            command_id,
            command_type,
            source,
            status,
            idempotency_key,
            payload_json,
            created_at,
            dispatched_at,
            completed_at,
            attempts,
            last_error
        FROM gateway_commands
        WHERE status IN ('REJECTED', 'FAILED', 'EXPIRED')
        ORDER BY COALESCE(completed_at, created_at) DESC, command_id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    failures = []
    for row in rows:
        item = {key: row[key] for key in row.keys()}
        item["payload"] = json.loads(item.pop("payload_json"))
        failures.append(item)
    return failures


def _bounded_limit(limit: int | None, settings: Settings) -> int:
    value = settings.dashboard_snapshot_default_limit if limit is None else int(limit)
    return min(max(value, 1), settings.dashboard_max_limit)


def _now() -> str:
    return datetime_to_wire(utc_now())
