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
from storage.gateway_command_store import FORBIDDEN_ORDER_COMMAND_TYPES

from services.ai_advisory.storage import (
    build_status as build_ai_advisory_status,
)
from services.ai_advisory.storage import (
    list_errors as list_ai_advisory_errors,
)
from services.ai_advisory.storage import (
    list_latest_scores as list_ai_advisory_latest_scores,
)
from services.ai_sidecar.codex_prompt_store import (
    count_codex_prompt_drafts,
    list_codex_prompt_drafts,
    list_codex_prompt_errors,
)
from services.ai_sidecar.live_sim_review_store import (
    count_live_sim_review_reports,
    list_live_sim_review_errors,
    list_live_sim_review_reports,
)
from services.ai_sidecar.openai_client import get_openai_client_status
from services.ai_sidecar.rca_report_store import (
    count_rca_reports,
    list_rca_report_errors,
    list_rca_reports,
)
from services.ai_sidecar.request_store import (
    get_ai_request_status_counts,
    get_last_ai_request_error,
    list_ai_requests,
)
from services.candidate_service import (
    get_candidate,
    get_candidate_status,
    list_candidate_projection_errors,
    list_candidates,
)
from services.condition_fusion import (
    get_condition_profile_metrics,
    list_condition_fusion,
)
from services.config import Settings
from services.dashboard_ai_explanations import build_ai_explanation_cards
from services.entry_timing.service import (
    get_entry_timing_status,
    list_latest_order_plan_drafts,
)
from services.exit_engine import (
    get_exit_status,
    list_exit_errors,
    list_exit_evaluations,
    list_exit_executions,
    list_exit_intents,
    list_exit_orders,
    list_exit_signals,
)
from services.live_sim.live_sim_service import (
    get_live_sim_status,
    list_live_sim_cancel_intents,
    list_live_sim_errors,
    list_live_sim_executions,
    list_live_sim_exit_signals,
    list_live_sim_intents,
    list_live_sim_lifecycle_events,
    list_live_sim_orders,
    list_live_sim_positions,
    list_live_sim_reconcile_snapshots,
    list_live_sim_rejections,
)
from services.market_data_service import (
    get_market_data_status,
    list_latest_ticks,
    list_projection_errors,
)
from services.oms.dry_run_service import (
    get_dry_run_status,
    list_dry_run_errors,
    list_dry_run_intents,
    list_dry_run_orders,
    list_dry_run_positions,
)
from services.operator.no_buy_sentinel import build_no_buy_sentinel_snapshot
from services.realtime_subscription import build_realtime_subscription_plan
from services.risk_gate import (
    get_risk_status,
    list_latest_risk_observations,
    list_risk_check_observations,
    list_risk_errors,
)
from services.runtime.live_sim_operating_orchestrator import build_live_sim_operator_status
from services.runtime.market_open_observe_cycle import (
    get_latest_market_open_observe_cycle_run,
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
    "condition_fusion",
    "market_data",
    "realtime_subscription",
    "themes",
    "candidates",
    "strategy",
    "risk",
    "dry_run",
    "live_sim",
    "ai_advisory",
    "ai_sidecar",
    "ai_explanations",
    "no_buy_sentinel",
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

STAGE_ENDPOINTS = {
    "Core": "/health",
    "Gateway": "/api/gateway/status",
    "MarketData": "/api/market-data/status",
    "RealtimeSubscription": "/api/operator/realtime-subscriptions/plan",
    "Theme": "/api/themes/status",
    "Candidate": "/api/candidates/status",
    "Strategy": "/api/strategy/status",
    "Risk": "/api/risk/status",
    "EntryTiming": "/api/entry-timing/status",
    "LiveSim": "/api/live-sim/status",
    "OrderSafety": "/api/gateway/commands/status",
    "ObserveCycle": "/api/operator/observe-cycle/runs/latest",
}


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
    command_type_counts = _command_type_counts(connection)
    order_command_count = _order_command_count(command_type_counts)
    gateway_recent_events = list_recent_gateway_events(connection, limit=bounded_limit)
    latest_gateway_heartbeat_payload = _latest_gateway_heartbeat_payload(gateway_recent_events)
    gateway_status = _gateway_status_section(
        settings,
        gateway_status_values,
        command_counts,
        recent_event_count=count_recent_gateway_events(connection),
        latest_heartbeat_payload=latest_gateway_heartbeat_payload,
    )

    market_data_status = get_market_data_status(connection, settings=settings)
    realtime_subscription = build_realtime_subscription_plan(
        connection,
        settings=settings,
        registered_codes=gateway_status["realtime_registered_codes"],
        queue_commands=False,
    ).to_dict()
    condition_fusion_rows = list_condition_fusion(
        connection,
        settings=settings,
        registered_codes=gateway_status["realtime_registered_codes"],
        limit=bounded_limit,
    )
    condition_profile_metrics = get_condition_profile_metrics(
        connection,
        settings=settings,
        limit=bounded_limit,
    )
    theme_status = get_theme_status(connection, settings=settings)
    candidate_status = get_candidate_status(connection, settings=settings)
    strategy_status = get_strategy_status(connection, settings)
    risk_status = get_risk_status(connection, settings)
    entry_timing_status = get_entry_timing_status(connection, settings=settings)
    dry_run_status = get_dry_run_status(connection, settings)
    exit_status = get_exit_status(connection, settings)
    live_sim_status = get_live_sim_status(connection, settings)
    live_sim_operator_status = build_live_sim_operator_status(connection, settings=settings)
    latest_observe_cycle = get_latest_market_open_observe_cycle_run(connection)
    no_buy_sentinel = build_no_buy_sentinel_snapshot(
        connection,
        settings=settings,
        manual=True,
        limit=min(bounded_limit, settings.no_buy_sentinel_top_near_miss_limit),
        write_snapshot=False,
    ).to_dict()

    latest_ticks = list_latest_ticks(connection, limit=bounded_limit)
    latest_theme_snapshots = list_latest_theme_snapshots(connection, limit=bounded_limit)
    candidates = list_candidates(connection, active_only=True, limit=bounded_limit)
    strategy_observations = list_latest_strategy_observations(connection, limit=bounded_limit)
    risk_observations = list_latest_risk_observations(connection, limit=bounded_limit)
    latest_order_plan_drafts = list_latest_order_plan_drafts(
        connection,
        limit=min(bounded_limit, 10),
    )
    ai_insights = list_ai_insights(connection, limit=bounded_limit)
    ai_requests = list_ai_requests(connection, limit=bounded_limit)
    ai_request_status_counts = get_ai_request_status_counts(connection)
    ai_last_error = get_last_ai_request_error(connection)
    ai_advisory_status = build_ai_advisory_status(connection, settings=settings)
    ai_advisory_latest = list_ai_advisory_latest_scores(
        connection,
        limit=min(bounded_limit, 10),
    )
    ai_advisory_errors = list_ai_advisory_errors(connection, limit=min(bounded_limit, 10))
    latest_rca_reports = list_rca_reports(connection, limit=min(bounded_limit, 10))
    latest_rca_errors = list_rca_report_errors(connection, limit=min(bounded_limit, 10))
    rca_report_count = count_rca_reports(connection)
    latest_live_sim_review_reports = list_live_sim_review_reports(
        connection,
        limit=min(bounded_limit, 10),
    )
    latest_live_sim_review_errors = list_live_sim_review_errors(
        connection,
        limit=min(bounded_limit, 10),
    )
    live_sim_review_report_count = count_live_sim_review_reports(connection)
    latest_codex_prompt_drafts = list_codex_prompt_drafts(
        connection,
        limit=min(bounded_limit, 10),
    )
    latest_codex_prompt_errors = list_codex_prompt_errors(
        connection,
        limit=min(bounded_limit, 10),
    )
    codex_draft_count = count_codex_prompt_drafts(connection)
    dry_run_intents = list_dry_run_intents(connection, limit=min(bounded_limit, 10))
    dry_run_orders = list_dry_run_orders(connection, limit=min(bounded_limit, 10))
    dry_run_positions = list_dry_run_positions(connection, limit=min(bounded_limit, 10))
    live_sim_intents = list_live_sim_intents(connection, limit=min(bounded_limit, 10))
    live_sim_orders = list_live_sim_orders(connection, limit=min(bounded_limit, 10))
    live_sim_executions = list_live_sim_executions(connection, limit=min(bounded_limit, 10))
    live_sim_rejections = list_live_sim_rejections(connection, limit=min(bounded_limit, 10))
    live_sim_positions = list_live_sim_positions(
        connection,
        open_only=True,
        limit=min(bounded_limit, 10),
    )
    live_sim_exit_signals = list_live_sim_exit_signals(connection, limit=min(bounded_limit, 10))
    live_sim_cancel_intents = list_live_sim_cancel_intents(connection, limit=min(bounded_limit, 10))
    live_sim_lifecycle_events = list_live_sim_lifecycle_events(
        connection,
        limit=min(bounded_limit, 10),
    )
    live_sim_reconcile = list_live_sim_reconcile_snapshots(
        connection,
        limit=min(bounded_limit, 10),
    )
    exit_evaluations = list_exit_evaluations(connection, limit=min(bounded_limit, 10))
    exit_signals = list_exit_signals(connection, limit=min(bounded_limit, 10))
    exit_intents = list_exit_intents(connection, limit=min(bounded_limit, 10))
    exit_orders = list_exit_orders(connection, limit=min(bounded_limit, 10))
    exit_executions = list_exit_executions(connection, limit=min(bounded_limit, 10))
    ai_explanations = build_ai_explanation_cards(
        connection,
        settings,
        limit=bounded_limit if include_detail else min(bounded_limit, 8),
        candidate_limit=bounded_limit if include_detail else min(bounded_limit, 8),
    )

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
        realtime_subscription=realtime_subscription,
        theme_status=theme_status,
        theme_state_counts=theme_state_counts,
        candidate_status=candidate_status,
        strategy_status=strategy_status,
        strategy_status_counts=strategy_status_counts,
        risk_status=risk_status,
        risk_status_counts=risk_status_counts,
        entry_timing_status=entry_timing_status,
        latest_order_plan_drafts=latest_order_plan_drafts,
        command_type_counts=command_type_counts,
        order_command_count=order_command_count,
        ai_insights=ai_insights,
        ai_request_status_counts=ai_request_status_counts,
        codex_draft_count=codex_draft_count,
        dry_run_status=dry_run_status,
        exit_status=exit_status,
        live_sim_status=live_sim_status,
        ai_advisory_status=ai_advisory_status,
        no_buy_sentinel=no_buy_sentinel,
        latest_observe_cycle=latest_observe_cycle,
        settings=settings,
    )

    return {
        "generated_at": generated_at,
        "detail": detail,
        "limit": bounded_limit,
        "safety": build_safety_section(settings),
        "system": _system_section(settings, generated_at),
        "gateway": gateway_status,
        "condition_fusion": {
            "status": {
                "profile_count": len(condition_profile_metrics),
                "fused_code_count": len(condition_fusion_rows),
                "risk_blocked_count": sum(
                    1 for row in condition_fusion_rows if row.get("risk_blocked")
                ),
                "subscribed_count": sum(
                    1 for row in condition_fusion_rows if row.get("subscribed")
                ),
                "read_only": True,
                "not_buy_signal": True,
            },
            "profiles": condition_profile_metrics
            or gateway_status.get("condition_profile_metrics", []),
            "codes": condition_fusion_rows,
        },
        "market_data": {
            "status": market_data_status,
            "latest_ticks": latest_ticks,
        },
        "realtime_subscription": realtime_subscription,
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
        "dry_run": {
            "status": dry_run_status,
            "enabled": dry_run_status["enabled"],
            "intent_creation_enabled": dry_run_status["intent_creation_enabled"],
            "simulated_fill_enabled": dry_run_status["simulated_fill_enabled"],
            "order_routing_enabled": False,
            "gateway_command_enabled": False,
            "live_order_allowed": False,
            "broker_order_sent": False,
            "active_position_count": dry_run_status["active_position_count"],
            "intent_count": dry_run_status["intent_count"],
            "order_count": dry_run_status["order_count"],
            "execution_count": dry_run_status["execution_count"],
            "recent_intents": dry_run_intents,
            "recent_orders": dry_run_orders,
            "positions": dry_run_positions,
            "exit_engine": {
                "enabled": exit_status["enabled"],
                "intent_creation_enabled": exit_status["intent_creation_enabled"],
                "order_creation_enabled": exit_status["order_creation_enabled"],
                "simulated_fill_enabled": exit_status["simulated_fill_enabled"],
                "broker_order_sent": False,
                "gateway_command_allowed": False,
                "live_order_allowed": False,
                "evaluation_count": exit_status["evaluation_count"],
                "signal_count": exit_status["signal_count"],
                "exit_intent_count": exit_status["exit_intent_count"],
                "exit_order_count": exit_status["exit_order_count"],
                "exit_execution_count": exit_status["exit_execution_count"],
                "recent_exit_evaluations": exit_evaluations,
                "recent_exit_signals": exit_signals,
                "recent_exit_intents": exit_intents,
                "recent_exit_orders": exit_orders,
                "recent_exit_executions": exit_executions,
                "warnings": [
                    "DRY_RUN Exit Engine은 simulated close 판단과 기록만 표시합니다.",
                    "Dashboard에는 exit 실행 버튼이 없습니다.",
                    "Exit Engine은 Gateway 명령이나 broker 주문을 만들지 않습니다.",
                ],
            },
            "warnings": [
                "DRY_RUN OMS는 내부 시뮬레이션 기록만 생성합니다.",
                "DRY_RUN OMS는 Gateway 명령이나 broker 주문을 만들지 않습니다.",
                "Dashboard는 DRY_RUN 실행 컨트롤 없이 읽기 전용으로 표시합니다.",
            ],
        },
        "live_sim": {
            "status": live_sim_status,
            "enabled": live_sim_status["enabled"],
            "order_routing_enabled": live_sim_status["order_routing_enabled"],
            "gateway_command_enabled": live_sim_status["gateway_command_enabled"],
            "kill_switch": live_sim_status["kill_switch"],
            "live_real_allowed": False,
            "account_mode": live_sim_status["account_mode"],
            "broker_env": live_sim_status["broker_env"],
            "server_mode": live_sim_status["server_mode"],
            "safety_gate": live_sim_status["safety_gate"],
            "intent_count": live_sim_status["intent_count"],
            "order_count": live_sim_status["order_count"],
            "execution_count": live_sim_status["execution_count"],
            "position_count": live_sim_status["position_count"],
            "open_position_count": live_sim_status["open_position_count"],
            "cancel_pending_count": live_sim_status["cancel_pending_count"],
            "active_exit_signal_count": live_sim_status["active_exit_signal_count"],
            "rejection_count": live_sim_status["rejection_count"],
            "open_order_count": live_sim_status["open_order_count"],
            "recent_intents": live_sim_intents,
            "recent_orders": live_sim_orders,
            "recent_executions": live_sim_executions,
            "recent_rejections": live_sim_rejections,
            "open_positions": live_sim_positions,
            "active_exit_signals": live_sim_exit_signals,
            "recent_cancel_intents": live_sim_cancel_intents,
            "recent_lifecycle_events": live_sim_lifecycle_events,
            "reconcile_status": live_sim_reconcile[0] if live_sim_reconcile else None,
            "recent_reconcile_snapshots": live_sim_reconcile,
            "operating": live_sim_operator_status,
            "live_sim_review_available": True,
            "live_sim_review_report_count": live_sim_review_report_count,
            "latest_live_sim_review_reports": latest_live_sim_review_reports,
            "latest_live_sim_review_errors": latest_live_sim_review_errors,
            "warnings": [
                "LIVE_SIM은 모의투자 전용이며 실계좌 주문이 아닙니다.",
                "LIVE_REAL은 비활성화되어 있습니다.",
                "Dashboard는 PR12에서 LIVE_SIM 실행 버튼을 제공하지 않습니다.",
                "LIVE_SIM Review Sidecar는 장후 복기 artifact이며 주문 입력이 아닙니다.",
            ],
            "order_controls_available": False,
            "execution_controls_available": False,
            "read_only": True,
        },
        "ai_advisory": {
            "status": ai_advisory_status,
            "latest": ai_advisory_latest,
            "latest_run": ai_advisory_latest.get("run"),
            "top_scores": ai_advisory_latest.get("scores", [])[:10],
            "risk_reward_suggestions": ai_advisory_latest.get(
                "risk_reward_suggestions",
                [],
            )[:10],
            "latest_errors": ai_advisory_errors,
            "latest_error_count": len(ai_advisory_errors),
            "advisory_only": True,
            "no_order_side_effects": True,
            "execution_controls_available": False,
            "order_controls_available": False,
            "notice": "AI Candidate Scorer는 후보 평가 annotation이며 주문 입력이 아닙니다.",
        },
        "ai_sidecar": {
            "status": build_ai_sidecar_status(settings),
            "insights": ai_insights,
            "insight_count": len(ai_insights),
            "requests": ai_requests,
            "request_status_counts": ai_request_status_counts,
            "recent_request_count": len(ai_requests),
            "recent_insight_count": len(ai_insights),
            "last_error": ai_last_error,
            "rca_available": True,
            "rca_report_count": rca_report_count,
            "latest_rca_reports": latest_rca_reports,
            "latest_rca_errors": latest_rca_errors,
            "latest_rca_report_count": len(latest_rca_reports),
            "latest_rca_error_count": len(latest_rca_errors),
            "live_sim_review_available": True,
            "live_sim_review_report_count": live_sim_review_report_count,
            "latest_live_sim_review_reports": latest_live_sim_review_reports,
            "latest_live_sim_review_errors": latest_live_sim_review_errors,
            "latest_live_sim_review_report_count": len(latest_live_sim_review_reports),
            "latest_live_sim_review_error_count": len(latest_live_sim_review_errors),
            "codex_prompt_generator_available": True,
            "codex_draft_count": codex_draft_count,
            "latest_codex_prompt_drafts": latest_codex_prompt_drafts,
            "latest_codex_prompt_errors": latest_codex_prompt_errors,
            "latest_codex_prompt_draft_count": len(latest_codex_prompt_drafts),
            "latest_codex_prompt_error_count": len(latest_codex_prompt_errors),
            "auto_apply_allowed": False,
            "github_write_allowed": False,
            "codex_execution_allowed": False,
            "no_trading_side_effects": True,
            "execution_controls_available": False,
            "notice": "AI Sidecar 결과는 Strategy/Risk/OMS 자동 입력이 아닙니다.",
        },
        "ai_explanations": ai_explanations,
        "no_buy_sentinel": no_buy_sentinel,
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
        "AI Sidecar 결과는 Strategy/Risk/OMS 자동 입력이 아닙니다.",
        "AI Candidate Scorer 결과는 advisory-only이며 주문 승인 근거가 아닙니다.",
        "AI Sidecar tools/function calling은 비활성화되어 있습니다.",
        "AI Sidecar에는 주문 tool이 없습니다.",
        "PR10 OMS는 DRY_RUN-only이며 broker 주문을 전송하지 않습니다.",
        "DRY_RUN에서도 Gateway 주문 명령은 비활성화되어 있습니다.",
        "LIVE_SIM은 모의투자 전용이며 실계좌 주문이 아닙니다.",
        "LIVE_REAL은 비활성화되어 있습니다.",
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
        "dry_run_only": True,
        "live_sim_enabled": settings.live_sim_enabled,
        "live_sim_order_routing_enabled": settings.live_sim_order_routing_enabled,
        "live_sim_gateway_command_configured": settings.live_sim_gateway_command_enabled,
        "live_sim_order_controls_available": False,
        "dry_run_order_controls_available": False,
        "broker_order_sent": False,
        "ai_sidecar_enabled": settings.ai_sidecar_enabled,
        "ai_context_builder_available": True,
        "ai_context_preview_available": settings.ai_sidecar_context_builder_enabled,
        "ai_execution_available": False,
        "openai_client_available": get_openai_client_status(settings)["available"],
        "order_context_allowed": settings.ai_sidecar_order_context_allowed,
        "observe_only_pipeline": True,
        "warnings": warnings,
    }


def build_ai_sidecar_status(settings: Settings) -> dict[str, Any]:
    client_status = get_openai_client_status(settings)
    return {
        "enabled": settings.ai_sidecar_enabled,
        "allow_intraday": settings.ai_sidecar_intraday_allowed,
        "allow_order_context": settings.ai_sidecar_order_context_allowed,
        "model": settings.ai_sidecar_model,
        "allowed_tasks": get_allowed_tasks(),
        "forbidden_actions": get_forbidden_actions(),
        "openai_client_available": client_status["available"],
        "execution_api_available": True,
        "context_builder_available": True,
        "context_builder_enabled": settings.ai_sidecar_context_builder_enabled,
        "ai_execution_available": (
            settings.ai_sidecar_enabled and settings.ai_sidecar_allow_manual_run
        ),
        "order_context_allowed": settings.ai_sidecar_order_context_allowed,
        "responses_api_enabled": settings.ai_sidecar_use_responses_api,
        "structured_outputs_enabled": settings.ai_sidecar_structured_outputs_enabled,
        "strict_schema": settings.ai_sidecar_strict_schema,
        "tools_enabled": False,
        "order_tools_enabled": False,
        "max_context_chars": settings.ai_sidecar_max_context_chars,
        "context_schema_version": settings.ai_sidecar_context_schema_version,
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
        "dry_run_errors": list_dry_run_errors(connection, limit=bounded_limit),
        "dry_run_exit_errors": list_exit_errors(connection, limit=bounded_limit),
        "live_sim_errors": list_live_sim_errors(connection, limit=bounded_limit),
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
    latest_heartbeat_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    heartbeat_payload = dict(latest_heartbeat_payload or {})
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
        "live_sim_order_command_allowed": settings.live_sim_gateway_command_enabled,
        "command_status_counts": command_counts,
        "kiwoom_logged_in": heartbeat_payload.get("kiwoom_logged_in"),
        "login_threaded": heartbeat_payload.get("login_threaded"),
        "server_mode": heartbeat_payload.get("server_mode"),
        "condition_load_state": heartbeat_payload.get("condition_load_state"),
        "condition_load_requested_at": heartbeat_payload.get("condition_load_requested_at"),
        "condition_load_retry_count": heartbeat_payload.get("condition_load_retry_count"),
        "condition_load_timeout_count": heartbeat_payload.get("condition_load_timeout_count"),
        "condition_reason_codes": heartbeat_payload.get("condition_reason_codes") or [],
        "condition_session_profile": heartbeat_payload.get("condition_session_profile"),
        "condition_profiles": heartbeat_payload.get("condition_profiles") or [],
        "condition_profile_screen_map": heartbeat_payload.get("condition_profile_screen_map")
        or {},
        "condition_send_results": heartbeat_payload.get("condition_send_results") or [],
        "condition_profile_metrics": heartbeat_payload.get("condition_profile_metrics")
        or [],
        "adaptive_realtime_budget": heartbeat_payload.get("adaptive_realtime_budget")
        or {},
        "latest_condition_ver_callback_at": heartbeat_payload.get(
            "latest_condition_ver_callback_at"
        ),
        "latest_condition_ver_result": heartbeat_payload.get("latest_condition_ver_result")
        or {},
        "registered_realtime_code_count": heartbeat_payload.get(
            "registered_realtime_code_count"
        ),
        "realtime_registered_codes": heartbeat_payload.get("realtime_registered_codes") or [],
        "realtime_exchange": heartbeat_payload.get("realtime_exchange") or "",
        "realtime_registered_kiwoom_codes": (
            heartbeat_payload.get("realtime_registered_kiwoom_codes") or []
        ),
        "realtime_registration_requested_count": heartbeat_payload.get(
            "realtime_registration_requested_count"
        ),
        "realtime_registration_success_count": heartbeat_payload.get(
            "realtime_registration_success_count"
        ),
        "latest_realtime_registration_at": heartbeat_payload.get(
            "latest_realtime_registration_at"
        ),
        "latest_realtime_registration_result": heartbeat_payload.get(
            "latest_realtime_registration_result"
        )
        or {},
        "latest_realtime_callback_at": heartbeat_payload.get(
            "latest_realtime_callback_at"
        ),
        "raw_realtime_callback_count": heartbeat_payload.get("raw_realtime_callback_count"),
        "realtime_callback_count": heartbeat_payload.get("realtime_callback_count"),
        "parsed_price_tick_count": heartbeat_payload.get("parsed_price_tick_count"),
        "realtime_parse_error_count": heartbeat_payload.get("realtime_parse_error_count"),
        "latest_realtime_parse_error": heartbeat_payload.get("latest_realtime_parse_error")
        or {},
        "realtime_subscription_health": heartbeat_payload.get(
            "realtime_subscription_health"
        ),
        "realtime_callback_real_type_counts": heartbeat_payload.get(
            "realtime_callback_real_type_counts"
        )
        or {},
        "realtime_recover_count": heartbeat_payload.get("realtime_recover_count"),
        "raw_callback_counts": heartbeat_payload.get("raw_callback_counts") or {},
        "latest_callback_at_by_method": heartbeat_payload.get("latest_callback_at_by_method")
        or {},
        "latest_active_x_thread_audit": heartbeat_payload.get(
            "latest_active_x_thread_audit"
        )
        or {},
    }


def _pipeline_summary(
    *,
    gateway_status: dict[str, Any],
    market_data_status: dict[str, Any],
    realtime_subscription: dict[str, Any],
    theme_status: dict[str, Any],
    theme_state_counts: dict[str, int],
    candidate_status: dict[str, Any],
    strategy_status: dict[str, Any],
    strategy_status_counts: dict[str, int],
    risk_status: dict[str, Any],
    risk_status_counts: dict[str, int],
    entry_timing_status: dict[str, Any],
    latest_order_plan_drafts: list[dict[str, Any]],
    command_type_counts: dict[str, int],
    order_command_count: int,
    ai_insights: list[dict[str, Any]],
    ai_request_status_counts: dict[str, int],
    codex_draft_count: int,
    dry_run_status: dict[str, Any],
    exit_status: dict[str, Any],
    live_sim_status: dict[str, Any],
    ai_advisory_status: dict[str, Any],
    no_buy_sentinel: dict[str, Any],
    latest_observe_cycle: dict[str, Any] | None,
    settings: Settings,
) -> dict[str, Any]:
    return {
        "stage_statuses": _pipeline_stage_statuses(
            gateway_status=gateway_status,
            market_data_status=market_data_status,
            realtime_subscription=realtime_subscription,
            theme_status=theme_status,
            candidate_status=candidate_status,
            strategy_status=strategy_status,
            risk_status=risk_status,
            entry_timing_status=entry_timing_status,
            live_sim_status=live_sim_status,
            order_command_count=order_command_count,
            latest_observe_cycle=latest_observe_cycle,
        ),
        "latest_observe_cycle": latest_observe_cycle,
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
        "realtime_subscription": {
            "status": realtime_subscription.get("status"),
            "planned_register_count": realtime_subscription.get("counts", {}).get(
                "planned_register_count", 0
            ),
            "planned_remove_count": realtime_subscription.get("counts", {}).get(
                "planned_remove_count", 0
            ),
            "already_registered_count": realtime_subscription.get("counts", {}).get(
                "already_registered_count", 0
            ),
            "queue_commands": False,
            "read_only": True,
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
        "entry_timing": {
            "latest_plan_count": entry_timing_status["latest_plan_count"],
            "plan_ready_count": entry_timing_status["plan_ready_count"],
            "wait_retry_count": entry_timing_status.get("wait_retry_count", 0),
            "data_wait_count": entry_timing_status.get("data_wait_count", 0),
            "no_plan_count": entry_timing_status.get("no_plan_count", 0),
            "evaluation_count": entry_timing_status["evaluation_count"],
            "error_count": entry_timing_status["error_count"],
            "latest_order_plan_draft_count": len(latest_order_plan_drafts),
            "observe_only": True,
            "not_order_intent": True,
        },
        "order_safety": {
            "order_command_count": order_command_count,
            "command_type_counts": command_type_counts,
            "order_commands_allowed": False,
            "read_only": True,
        },
        "dry_run": {
            "enabled": dry_run_status["enabled"],
            "intent_count": dry_run_status["intent_count"],
            "order_count": dry_run_status["order_count"],
            "execution_count": dry_run_status["execution_count"],
            "active_position_count": dry_run_status["active_position_count"],
            "exit_evaluation_count": exit_status["evaluation_count"],
            "exit_signal_count": exit_status["signal_count"],
            "order_routing_enabled": False,
            "gateway_command_enabled": False,
            "live_order_allowed": False,
        },
        "live_sim": {
            "enabled": live_sim_status["enabled"],
            "intent_count": live_sim_status["intent_count"],
            "order_count": live_sim_status["order_count"],
            "execution_count": live_sim_status["execution_count"],
            "rejection_count": live_sim_status["rejection_count"],
            "open_order_count": live_sim_status["open_order_count"],
            "safety_gate_status": live_sim_status["safety_gate"]["status"],
            "live_real_allowed": False,
            "order_controls_available": False,
            "operating_mode": (
                live_sim_status.get("operating", {}).get("current_operating_mode")
                if "operating" in live_sim_status
                else settings.live_sim_operating_default_mode
            ),
        },
        "ai_sidecar": {
            "enabled": settings.ai_sidecar_enabled,
            "insight_count": len(ai_insights),
            "rca_available": True,
            "codex_prompt_generator_available": True,
            "codex_draft_count": codex_draft_count,
            "request_status_counts": ai_request_status_counts,
            "execution_api_available": True,
            "context_builder_available": True,
            "openai_client_available": get_openai_client_status(settings)["available"],
            "order_context_allowed": settings.ai_sidecar_order_context_allowed,
            "tools_enabled": False,
            "order_tools_enabled": False,
        },
        "ai_advisory": {
            "enabled": ai_advisory_status["enabled"],
            "provider": ai_advisory_status["provider"],
            "model": ai_advisory_status["model"],
            "external_enabled": ai_advisory_status["external_enabled"],
            "external_call_attempted": ai_advisory_status["external_call_attempted"],
            "latest_status": (
                None
                if ai_advisory_status.get("latest_run") is None
                else ai_advisory_status["latest_run"].get("status")
            ),
            "latest_selected_count": (
                0
                if ai_advisory_status.get("latest_run") is None
                else ai_advisory_status["latest_run"].get("selected_count", 0)
            ),
            "error_count": ai_advisory_status["error_count"],
            "invalid_schema_error_count": ai_advisory_status["invalid_schema_error_count"],
            "fallback_used": ai_advisory_status["fallback_used"],
            "advisory_only": True,
            "no_order_side_effects": True,
        },
        "no_buy_sentinel": {
            "status": no_buy_sentinel.get("status"),
            "no_buy_detected": no_buy_sentinel.get("no_buy_detected"),
            "intent_count": no_buy_sentinel.get("intent_count", 0),
            "order_count": no_buy_sentinel.get("order_count", 0),
            "command_count": no_buy_sentinel.get("command_count", 0),
            "plan_ready_count": no_buy_sentinel.get("plan_ready_count", 0),
            "buy_eligible_count": no_buy_sentinel.get("buy_eligible_count", 0),
            "ai_selected_count": no_buy_sentinel.get("ai_selected_count", 0),
            "read_only": True,
            "no_order_side_effects": True,
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
                "key": "realtime_subscription_targets",
                "label": "Realtime Plan",
                "count": realtime_subscription.get("counts", {}).get(
                    "planned_register_count", 0
                ),
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
            {
                "key": "entry_timing_plans",
                "label": "EntryTiming Plans",
                "count": entry_timing_status["latest_plan_count"],
            },
            {
                "key": "dry_run_intents",
                "label": "DRY_RUN Intents",
                "count": dry_run_status["intent_count"],
            },
            {
                "key": "live_sim_orders",
                "label": "LIVE_SIM Orders",
                "count": live_sim_status["order_count"],
            },
            {
                "key": "no_buy",
                "label": "No-Buy Sentinel",
                "count": 1 if no_buy_sentinel.get("no_buy_detected") else 0,
            },
        ],
    }


def _pipeline_stage_statuses(
    *,
    gateway_status: dict[str, Any],
    market_data_status: dict[str, Any],
    realtime_subscription: dict[str, Any],
    theme_status: dict[str, Any],
    candidate_status: dict[str, Any],
    strategy_status: dict[str, Any],
    risk_status: dict[str, Any],
    entry_timing_status: dict[str, Any],
    live_sim_status: dict[str, Any],
    order_command_count: int,
    latest_observe_cycle: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    last_run_status = None if latest_observe_cycle is None else latest_observe_cycle.get("status")
    return [
        _stage_status(
            "Core",
            "PASS",
            "Core snapshot was generated.",
            count=None,
            updated_at=None,
            reason_codes=[],
        ),
        _gateway_stage_status(gateway_status),
        _market_data_stage_status(market_data_status),
        _realtime_subscription_stage_status(realtime_subscription),
        _stage_status(
            "Theme",
            "BLOCK"
            if not theme_status.get("member_count")
            else "PASS"
            if theme_status.get("latest_snapshot_count")
            else "WARN",
            "Theme snapshots are present."
            if theme_status.get("latest_snapshot_count")
            else "Theme snapshot has not been built.",
            count=theme_status.get("latest_snapshot_count"),
            updated_at=None,
            reason_codes=["THEME_MEMBERSHIP_EMPTY"]
            if not theme_status.get("member_count")
            else []
            if theme_status.get("latest_snapshot_count")
            else ["THEME_SNAPSHOT_NOT_BUILT"],
        ),
        _stage_status(
            "Candidate",
            "PASS" if candidate_status.get("active_candidate_count") else "WARN",
            "Active candidates are present."
            if candidate_status.get("active_candidate_count")
            else "No active candidate exists.",
            count=candidate_status.get("active_candidate_count"),
            updated_at=None,
            reason_codes=(
                [] if candidate_status.get("active_candidate_count") else ["CANDIDATE_EMPTY"]
            ),
        ),
        _stage_status(
            "Strategy",
            "PASS" if strategy_status.get("latest_observation_count") else "WARN",
            "Strategy observations are present."
            if strategy_status.get("latest_observation_count")
            else "No strategy observation exists.",
            count=strategy_status.get("latest_observation_count"),
            updated_at=None,
            reason_codes=(
                [] if strategy_status.get("latest_observation_count") else ["STRATEGY_EMPTY"]
            ),
        ),
        _stage_status(
            "Risk",
            "PASS" if risk_status.get("latest_observation_count") else "WARN",
            "Risk observations are present."
            if risk_status.get("latest_observation_count")
            else "No risk observation exists.",
            count=risk_status.get("latest_observation_count"),
            updated_at=None,
            reason_codes=[] if risk_status.get("latest_observation_count") else ["RISK_EMPTY"],
        ),
        _stage_status(
            "EntryTiming",
            "PASS" if entry_timing_status.get("latest_plan_count") else "WARN",
            "OrderPlanDrafts exist as observe-only drafts."
            if entry_timing_status.get("latest_plan_count")
            else "No order plan draft exists.",
            count=entry_timing_status.get("latest_plan_count"),
            updated_at=None,
            reason_codes=(
                [] if entry_timing_status.get("latest_plan_count") else ["ORDER_PLAN_EMPTY"]
            ),
        ),
        _stage_status(
            "LiveSim",
            "PASS",
            "LIVE_SIM safety flags are read-only in this dashboard.",
            count=live_sim_status.get("order_count"),
            updated_at=None,
            reason_codes=[
                *([] if live_sim_status.get("enabled") else ["LIVE_SIM_DISABLED_EXPECTED"]),
                *(
                    []
                    if not live_sim_status.get("kill_switch")
                    else ["LIVE_SIM_KILL_SWITCH_ON_EXPECTED"]
                ),
                "ORDER_COMMAND_ZERO_EXPECTED",
            ],
        ),
        _stage_status(
            "OrderSafety",
            "BLOCK" if order_command_count else "PASS",
            "Order-like GatewayCommand rows exist."
            if order_command_count
            else "No order-like GatewayCommand row exists.",
            count=order_command_count,
            updated_at=None,
            reason_codes=["ORDER_COMMAND_ZERO_EXPECTED"],
        ),
        _stage_status(
            "ObserveCycle",
            "UNKNOWN" if latest_observe_cycle is None else "PASS",
            "No observe cycle run has been recorded yet."
            if latest_observe_cycle is None
            else f"Latest observe cycle status: {last_run_status}",
            count=0 if latest_observe_cycle is None else 1,
            updated_at=(
                None if latest_observe_cycle is None else latest_observe_cycle.get("created_at")
            ),
            reason_codes=["CANDIDATE_REBUILD_NOT_RUN"] if latest_observe_cycle is None else [],
        ),
    ]


def _stage_status(
    stage: str,
    status: str,
    summary: str,
    *,
    count: Any,
    updated_at: Any,
    reason_codes: list[str],
) -> dict[str, Any]:
    return {
        "stage": stage,
        "status": status,
        "summary": summary,
        "count": count,
        "updated_at": updated_at,
        "last_updated_at": updated_at,
        "reason_codes": reason_codes,
        "endpoint": STAGE_ENDPOINTS.get(stage),
    }


def _realtime_subscription_stage_status(payload: dict[str, Any]) -> dict[str, Any]:
    counts = payload.get("counts") or {}
    planned_register_count = int(counts.get("planned_register_count") or 0)
    planned_remove_count = int(counts.get("planned_remove_count") or 0)
    reason_codes = []
    status_value = str(payload.get("status") or "").upper()
    if status_value == "DISABLED":
        reason_codes.append("REALTIME_SUBSCRIPTION_DISABLED")
    elif planned_register_count <= 0 and planned_remove_count <= 0:
        reason_codes.append("REALTIME_SUBSCRIPTION_NOOP")
    return _stage_status(
        "RealtimeSubscription",
        "WARN" if status_value == "DISABLED" else "PASS",
        (
            f"register={planned_register_count}, remove={planned_remove_count}, "
            f"registered={counts.get('already_registered_count', 0)}"
        ),
        count=planned_register_count,
        updated_at=None,
        reason_codes=reason_codes,
    )


def _gateway_stage_status(gateway_status: dict[str, Any]) -> dict[str, Any]:
    if not gateway_status.get("last_heartbeat_at"):
        return _stage_status(
            "Gateway",
            "BLOCK",
            "Gateway heartbeat is missing.",
            count=gateway_status.get("recent_event_count"),
            updated_at=None,
            reason_codes=["GATEWAY_HEARTBEAT_MISSING"],
        )
    registered_count = int(gateway_status.get("registered_realtime_code_count") or 0)
    callback_count = int(gateway_status.get("realtime_callback_count") or 0)
    recover_count = int(gateway_status.get("realtime_recover_count") or 0)
    condition_state = str(gateway_status.get("condition_load_state") or "").upper()
    if condition_state == "CALLBACK_TIMEOUT":
        condition_health = str(
            gateway_status.get("condition_callback_health") or "ACTIVE_X_CALLBACK_SUSPECTED"
        ).upper()
        return _stage_status(
            "Gateway",
            "BLOCK",
            "Condition load callback timeout suggests ActiveX callback delivery trouble.",
            count=gateway_status.get("recent_event_count"),
            updated_at=gateway_status.get("last_heartbeat_at"),
            reason_codes=[
                "CONDITION_VER_CALLBACK_TIMEOUT",
                condition_health,
            ],
        )
    health = str(gateway_status.get("realtime_subscription_health") or "")
    callback_missing_health = {
        "CALLBACK_TIMEOUT",
        "CORE_IO_BLOCKING_SUSPECTED",
        "ACTIVE_X_CALLBACK_SUSPECTED",
    }
    if health in callback_missing_health or (
        registered_count > 0 and callback_count <= 0 and recover_count > 0
    ):
        return _stage_status(
            "Gateway",
            "BLOCK",
            "Realtime registration exists, but Kiwoom realtime callbacks are missing.",
            count=gateway_status.get("recent_event_count"),
            updated_at=gateway_status.get("last_heartbeat_at"),
            reason_codes=[
                "REALTIME_CALLBACK_MISSING",
                health if health in callback_missing_health else "ACTIVE_X_CALLBACK_SUSPECTED",
            ],
        )
    if health == "PARSE_ERROR":
        return _stage_status(
            "Gateway",
            "BLOCK",
            "Realtime callbacks arrived, but price tick parsing failed.",
            count=gateway_status.get("recent_event_count"),
            updated_at=gateway_status.get("last_heartbeat_at"),
            reason_codes=["REALTIME_PARSE_ERROR"],
        )
    return _stage_status(
        "Gateway",
        "PASS",
        "Gateway heartbeat exists.",
        count=gateway_status.get("recent_event_count"),
        updated_at=gateway_status.get("last_heartbeat_at"),
        reason_codes=[],
    )


def _market_data_stage_status(market_data_status: dict[str, Any]) -> dict[str, Any]:
    latest_tick_count = int(market_data_status.get("latest_tick_count") or 0)
    fresh_tick_count = int(market_data_status.get("fresh_tick_count") or 0)
    stale_tick_count = int(market_data_status.get("stale_tick_count") or 0)
    recent_projection_error_count = int(
        market_data_status.get("recent_projection_error_count") or 0
    )
    historical_projection_error_count = int(market_data_status.get("projection_error_count") or 0)
    reason_codes: list[str] = []
    if recent_projection_error_count:
        reason_codes.append("MARKET_PROJECTION_ERROR")
    if latest_tick_count <= 0:
        reason_codes.append("TICK_MISSING")
    elif fresh_tick_count <= 0:
        reason_codes.append("TICK_STALE")

    status = "PASS" if not reason_codes else "BLOCK"
    summary = (
        f"fresh_ticks={fresh_tick_count}, stale_ticks={stale_tick_count}, "
        f"recent_projection_errors={recent_projection_error_count}, "
        f"historical_projection_errors={historical_projection_error_count}"
    )
    return _stage_status(
        "MarketData",
        status,
        summary,
        count=latest_tick_count,
        updated_at=None,
        reason_codes=reason_codes,
    )


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


def _command_type_counts(connection: sqlite3.Connection) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT LOWER(command_type) AS command_type, COUNT(*) AS count
        FROM gateway_commands
        GROUP BY LOWER(command_type)
        """
    ).fetchall()
    return {str(row["command_type"]): int(row["count"]) for row in rows}


def _order_command_count(command_type_counts: dict[str, int]) -> int:
    return sum(
        int(count)
        for command_type, count in command_type_counts.items()
        if command_type in FORBIDDEN_ORDER_COMMAND_TYPES or "order" in command_type
    )


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


def _latest_gateway_heartbeat_payload(events: list[dict[str, Any]]) -> dict[str, Any]:
    for event in events:
        if str(event.get("event_type") or "") != "heartbeat":
            continue
        payload = event.get("payload")
        if isinstance(payload, dict):
            return dict(payload)
    return {}


def _bounded_limit(limit: int | None, settings: Settings) -> int:
    value = settings.dashboard_snapshot_default_limit if limit is None else int(limit)
    return min(max(value, 1), settings.dashboard_max_limit)


def _now() -> str:
    return datetime_to_wire(utc_now())
