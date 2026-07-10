from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter
from collections.abc import Mapping
from typing import Any, Literal

from domain.ai_sidecar.policy import get_allowed_tasks, get_forbidden_actions
from domain.ai_sidecar.schemas import insight_row_to_dict
from domain.broker.utils import datetime_to_wire, parse_timestamp, utc_now
from domain.candidate.state import CandidateState
from domain.risk.status import RiskObservationStatus
from domain.strategy.status import StrategyObservationStatus
from domain.theme.state import ThemeState
from storage.event_retention import get_event_retention_status
from storage.event_store import (
    count_recent_gateway_events,
    get_gateway_status_values,
    list_recent_gateway_events,
)
from storage.gateway_command_store import FORBIDDEN_ORDER_COMMAND_TYPES
from storage.gateway_order_broker_boundary import (
    get_order_broker_boundary_status,
)
from storage.live_sim_order_plan_uniqueness import (
    get_live_sim_order_plan_uniqueness_status,
)
from storage.projection_outbox import get_projection_outbox_status
from storage.projection_retention import build_projection_retention_rca
from storage.projection_watermarks import get_projection_watermark_status

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
from services.market_context_service import get_market_context_status
from services.market_data_service import (
    get_market_data_status,
    list_latest_ticks,
    list_projection_errors,
    list_recent_cross_exchange_observations,
)
from services.market_index_service import (
    get_market_index_status,
    list_latest_market_index_ticks,
)
from services.market_regime_service import get_market_regime_status
from services.market_scan_service import get_market_scan_status
from services.oms.dry_run_service import (
    get_dry_run_status,
    list_dry_run_errors,
    list_dry_run_intents,
    list_dry_run_orders,
    list_dry_run_positions,
)
from services.operator.no_buy_sentinel import build_no_buy_sentinel_snapshot
from services.pipeline_coherency import build_pipeline_coherency_status
from services.realtime_subscription import build_realtime_subscription_plan
from services.risk_gate import (
    get_risk_status,
    list_latest_risk_observations,
    list_risk_check_observations,
    list_risk_errors,
)
from services.runtime.evaluation_run_guard import get_runtime_execution_lock_status
from services.runtime.gateway_live_sim_lifecycle_routing import (
    build_live_sim_lifecycle_cutover_status,
)
from services.runtime.gateway_market_index_routing import (
    get_latest_market_index_append_only_routing_status,
)
from services.runtime.gateway_market_reference_routing import (
    build_market_reference_status,
    get_latest_market_reference_append_only_routing_status,
)
from services.runtime.gateway_market_regime_routing import (
    get_latest_market_regime_append_only_routing_status,
)
from services.runtime.gateway_market_scan_routing import (
    get_latest_market_scan_append_only_routing_status,
)
from services.runtime.gateway_projection_routing import (
    get_latest_market_data_append_only_routing_status,
)
from services.runtime.incremental_evaluation import get_incremental_evaluation_status
from services.runtime.live_sim_operating_orchestrator import build_live_sim_operator_status
from services.runtime.market_data_append_only_controller import (
    build_market_data_append_only_controller_status,
)
from services.runtime.market_data_projection_reconcile import (
    get_latest_market_data_projection_reconcile,
)
from services.runtime.market_index_projection_reconcile import (
    get_latest_market_index_projection_reconcile,
)
from services.runtime.market_open_observe_cycle import (
    get_latest_market_open_observe_cycle_run,
)
from services.runtime.market_reference_projection_reconcile import (
    get_latest_market_reference_projection_reconcile,
)
from services.runtime.market_regime_projection_reconcile import (
    get_latest_market_regime_projection_reconcile,
)
from services.runtime.market_scan_projection_reconcile import (
    get_latest_market_scan_projection_reconcile,
)
from services.runtime.projection_outbox_backlog import (
    build_projection_outbox_backlog_status,
    projection_outbox_backlog_summary_fields,
)
from services.runtime.projection_replay import get_projection_replay_status
from services.strategy_engine import (
    get_strategy_status,
    list_latest_strategy_observations,
    list_strategy_errors,
    list_strategy_setup_observations,
)
from services.theme_leadership import rebuild_theme_leadership
from services.theme_service import (
    get_theme_status,
    list_latest_theme_snapshots,
    list_theme_projection_errors,
    list_top_theme_snapshots_for_dashboard,
)

DashboardDetail = Literal["summary", "full"]

DASHBOARD_SECTIONS = [
    "safety",
    "system",
    "runtime_execution_locks",
    "live_sim_order_plan_uniqueness",
    "order_broker_boundaries",
    "live_sim_lifecycle_consumer",
    "projection_replay",
    "projection_watermarks",
    "projection_retention",
    "gateway",
    "condition_fusion",
    "market_data",
    "market_reference",
    "market_indexes",
    "market_context",
    "market_regime",
    "market_scan",
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
    "projection_outbox",
    "projection_outbox_backlog",
    "incremental_evaluation",
    "pipeline_coherency",
    "market_data_projection_reconcile",
    "market_data_append_only_routing",
    "market_data_append_only_controller",
    "market_reference_projection_reconcile",
    "market_reference_append_only_routing",
    "market_index_projection_reconcile",
    "market_index_append_only_routing",
    "market_regime_projection_reconcile",
    "market_regime_append_only_routing",
    "market_scan_projection_reconcile",
    "market_scan_append_only_routing",
    "pipeline_summary",
]

FAST_DASHBOARD_DEFAULT_SECTIONS = (
    "system",
    "runtime_execution_locks",
    "live_sim_order_plan_uniqueness",
    "order_broker_boundaries",
    "live_sim_lifecycle_consumer",
    "projection_replay",
    "projection_watermarks",
    "projection_retention",
    "gateway",
    "market_data",
    "market_reference",
    "projection_outbox",
    "projection_outbox_backlog",
    "incremental_evaluation",
    "pipeline_coherency",
    "market_data_projection_reconcile",
    "market_data_append_only_routing",
    "market_data_append_only_controller",
    "market_reference_projection_reconcile",
    "market_reference_append_only_routing",
    "market_index_projection_reconcile",
    "market_index_append_only_routing",
    "market_regime_projection_reconcile",
    "market_regime_append_only_routing",
    "market_scan_projection_reconcile",
    "market_scan_append_only_routing",
    "pipeline_summary",
    "errors",
)

FAST_DASHBOARD_SUPPORTED_SECTIONS = {
    "safety",
    "system",
    "runtime_execution_locks",
    "live_sim_order_plan_uniqueness",
    "order_broker_boundaries",
    "live_sim_lifecycle_consumer",
    "projection_replay",
    "projection_watermarks",
    "projection_retention",
    "gateway",
    "condition_fusion",
    "market_data",
    "market_reference",
    "market_indexes",
    "market_context",
    "market_regime",
    "market_scan",
    "realtime_subscription",
    "recent_events",
    "errors",
    "projection_outbox",
    "projection_outbox_backlog",
    "incremental_evaluation",
    "pipeline_coherency",
    "market_data_projection_reconcile",
    "market_data_append_only_routing",
    "market_data_append_only_controller",
    "market_reference_projection_reconcile",
    "market_reference_append_only_routing",
    "market_index_projection_reconcile",
    "market_index_append_only_routing",
    "market_regime_projection_reconcile",
    "market_regime_append_only_routing",
    "market_scan_projection_reconcile",
    "market_scan_append_only_routing",
    "pipeline_summary",
}

FAST_DASHBOARD_REQUIRED_SECTIONS = {
    "gateway",
    "projection_outbox",
    "market_data_append_only_routing",
    "market_data_append_only_controller",
}

GATEWAY_HEARTBEAT_STALE_SEC = 120.0

COMMAND_STATUSES = (
    "QUEUED",
    "DISPATCHED",
    "CLAIMED",
    "GATEWAY_STARTED",
    "PRE_ACK_RECORDED",
    "BROKER_ACCEPTED",
    "CHEJAN_CONFIRMED",
    "UNCONFIRMED",
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
    "ConditionFusion": "/api/dashboard/snapshot?sections=condition_fusion",
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
    started_at = time.monotonic()
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
    market_index_status = get_market_index_status(connection, settings=settings)
    market_context_status = get_market_context_status(connection, settings=settings)
    market_regime_status = get_market_regime_status(connection, settings=settings)
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
    condition_fusion_section = _condition_fusion_section(
        condition_fusion_rows,
        condition_profile_metrics,
        fallback_profiles=gateway_status.get("condition_profile_metrics", []),
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
    runtime_execution_locks = get_runtime_execution_lock_status(connection)
    live_sim_order_plan_uniqueness = get_live_sim_order_plan_uniqueness_status(
        connection
    )
    order_broker_boundaries = get_order_broker_boundary_status(connection)
    live_sim_lifecycle_consumer = build_live_sim_lifecycle_cutover_status(
        connection,
        settings=settings,
    )
    projection_replay = get_projection_replay_status()
    projection_watermarks = get_projection_watermark_status(connection)
    projection_retention = get_event_retention_status(
        connection,
        settings=settings,
    )
    projection_outbox_status = get_projection_outbox_status(connection, settings=settings)
    incremental_evaluation = get_incremental_evaluation_status(
        connection,
        settings=settings,
    )
    pipeline_coherency = build_pipeline_coherency_status(
        connection,
        max_age_sec=settings.entry_timing_stale_max_seconds,
        limit=bounded_limit,
    )
    market_data_reconcile = get_latest_market_data_projection_reconcile(connection)
    market_data_append_only_routing = get_latest_market_data_append_only_routing_status(
        connection,
        settings=settings,
    )
    market_data_append_only_controller = (
        build_market_data_append_only_controller_status(
            connection,
            settings=settings,
        ).to_dict()
    )
    market_reference_status = build_market_reference_status(
        connection,
        settings=settings,
    )
    market_reference_reconcile = get_latest_market_reference_projection_reconcile(
        connection
    )
    market_reference_append_only_routing = (
        get_latest_market_reference_append_only_routing_status(
            connection,
            settings=settings,
        )
    )
    market_index_reconcile = get_latest_market_index_projection_reconcile(connection)
    market_index_append_only_routing = (
        get_latest_market_index_append_only_routing_status(
            connection,
            settings=settings,
        )
    )
    market_regime_reconcile = get_latest_market_regime_projection_reconcile(
        connection
    )
    market_regime_append_only_routing = (
        get_latest_market_regime_append_only_routing_status(
            connection,
            settings=settings,
        )
    )
    market_scan_status = get_market_scan_status(connection, settings=settings)
    market_scan_reconcile = get_latest_market_scan_projection_reconcile(connection)
    market_scan_append_only_routing = (
        get_latest_market_scan_append_only_routing_status(
            connection,
            settings=settings,
        )
    )
    projection_outbox_backlog = build_projection_outbox_backlog_status(
        connection,
        settings=settings,
        latest_reconcile=market_data_reconcile,
        routing_status=market_data_append_only_routing,
        sample_limit=3,
    ).to_dict()
    projection_outbox_status.update(
        projection_outbox_backlog_summary_fields(projection_outbox_backlog)
    )
    latest_observe_cycle = get_latest_market_open_observe_cycle_run(connection)
    no_buy_sentinel = build_no_buy_sentinel_snapshot(
        connection,
        settings=settings,
        manual=True,
        limit=min(bounded_limit, settings.no_buy_sentinel_top_near_miss_limit),
        write_snapshot=False,
    ).to_dict()

    latest_ticks = list_latest_ticks(connection, limit=bounded_limit)
    latest_cross_exchange = list_recent_cross_exchange_observations(
        connection,
        limit=min(bounded_limit, 10),
    )
    latest_market_index_ticks = list_latest_market_index_ticks(
        connection,
        limit=bounded_limit,
    )
    latest_theme_snapshots = _with_theme_snapshot_freshness(
        list_latest_theme_snapshots(connection, limit=bounded_limit),
        settings=settings,
    )
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
    latest_sample_state_counts = _state_counts_from_theme_rows(latest_theme_snapshots)
    top_tradable_themes = _with_theme_snapshot_freshness(
        list_top_theme_snapshots_for_dashboard(connection, limit=10),
        settings=settings,
    )
    top_leading_themes = _with_theme_snapshot_freshness(
        list_top_theme_snapshots_for_dashboard(
            connection,
            states=(ThemeState.LEADING,),
            limit=10,
        ),
        settings=settings,
    )
    top_spreading_themes = _with_theme_snapshot_freshness(
        list_top_theme_snapshots_for_dashboard(
            connection,
            states=(ThemeState.SPREADING,),
            limit=10,
        ),
        settings=settings,
    )
    theme_dashboard_warnings = _theme_dashboard_warnings(
        full_state_counts=theme_state_counts,
        latest_sample_state_counts=latest_sample_state_counts,
        top_tradable_themes=top_tradable_themes,
    )
    theme_leadership = rebuild_theme_leadership(
        connection,
        write_candidate_sources=False,
        settings=settings,
    )

    errors = build_dashboard_errors(
        connection,
        settings=settings,
        limit=bounded_limit,
        projection_retention_status=projection_retention,
    )
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
        condition_fusion_status=condition_fusion_section["status"],
        projection_outbox_status=projection_outbox_status,
        projection_outbox_backlog=projection_outbox_backlog,
        market_data_reconcile=market_data_reconcile,
        market_data_append_only_routing=market_data_append_only_routing,
        market_data_append_only_controller=market_data_append_only_controller,
        market_reference_status=market_reference_status,
        market_reference_reconcile=market_reference_reconcile,
        market_reference_append_only_routing=market_reference_append_only_routing,
        runtime_execution_locks=runtime_execution_locks,
        live_sim_order_plan_uniqueness=live_sim_order_plan_uniqueness,
        order_broker_boundaries=order_broker_boundaries,
        pipeline_coherency=pipeline_coherency,
        settings=settings,
    )

    return {
        "generated_at": generated_at,
        "detail": detail,
        "limit": bounded_limit,
        "fast_path": False,
        "requested_sections": None,
        "included_sections": list(DASHBOARD_SECTIONS),
        "skipped_sections": [],
        "section_latency_ms": {},
        "total_latency_ms": round((time.monotonic() - started_at) * 1000, 3),
        "timeout_budget_ms": None,
        "warnings": [],
        "safety": build_safety_section(settings),
        "system": _system_section(settings, generated_at),
        "gateway": gateway_status,
        "condition_fusion": condition_fusion_section,
        "market_data": {
            "status": market_data_status,
            "latest_ticks": latest_ticks,
            "cross_exchange": {
                "read_only": True,
                "enabled": settings.risk_cross_exchange_divergence_bp > 0,
                "threshold_bp": settings.risk_cross_exchange_divergence_bp,
                "latest_observations": latest_cross_exchange,
            },
        },
        "market_reference": market_reference_status,
        "market_indexes": {
            "status": market_index_status,
            "latest_ticks": latest_market_index_ticks,
            "latest_by_code": _market_index_latest_by_code(latest_market_index_ticks),
            "gateway_adapter": _market_index_gateway_adapter_section(gateway_status),
            "projection_reconcile": market_index_reconcile,
            "append_only_routing": market_index_append_only_routing,
        },
        "market_context": market_context_status,
        "market_regime": market_regime_status,
        "market_scan": market_scan_status,
        "realtime_subscription": realtime_subscription,
        "runtime_execution_locks": runtime_execution_locks,
        "live_sim_order_plan_uniqueness": live_sim_order_plan_uniqueness,
        "order_broker_boundaries": order_broker_boundaries,
        "live_sim_lifecycle_consumer": live_sim_lifecycle_consumer,
        "projection_replay": projection_replay,
        "projection_watermarks": projection_watermarks,
        "projection_retention": projection_retention,
        "projection_outbox": projection_outbox_status,
        "projection_outbox_backlog": projection_outbox_backlog,
        "incremental_evaluation": incremental_evaluation,
        "pipeline_coherency": pipeline_coherency,
        "market_data_projection_reconcile": market_data_reconcile,
        "market_data_append_only_routing": market_data_append_only_routing,
        "market_data_append_only_controller": market_data_append_only_controller,
        "market_reference_projection_reconcile": market_reference_reconcile,
        "market_reference_append_only_routing": market_reference_append_only_routing,
        "market_index_projection_reconcile": market_index_reconcile,
        "market_index_append_only_routing": market_index_append_only_routing,
        "market_regime_projection_reconcile": market_regime_reconcile,
        "market_regime_append_only_routing": market_regime_append_only_routing,
        "market_scan_projection_reconcile": market_scan_reconcile,
        "market_scan_append_only_routing": market_scan_append_only_routing,
        "themes": {
            "status": {
                **theme_status,
                "snapshot_stale_sec": settings.theme_snapshot_stale_sec,
            },
            "latest_snapshots": latest_theme_snapshots,
            "state_counts": theme_state_counts,
            "top_tradable_themes": top_tradable_themes,
            "top_leading_themes": top_leading_themes,
            "top_spreading_themes": top_spreading_themes,
            "latest_sample_state_counts": latest_sample_state_counts,
            "dashboard_warnings": theme_dashboard_warnings,
            "top_list_source": "state_filtered_strength_query",
            "leadership": {
                "status": theme_leadership.status,
                "top_themes": [
                    snapshot.to_dict(include_members=False)
                    for snapshot in theme_leadership.snapshots
                ],
                "watchset": theme_leadership.watchset.to_dict(),
                "eligible_theme_count": theme_leadership.eligible_theme_count,
                "watchset_selection_source": theme_leadership.watchset_selection_source,
                "warning": theme_leadership.warning,
            },
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


def parse_dashboard_sections(raw: str | None) -> set[str] | None:
    if raw is None:
        return None
    sections = {
        section.strip().lower().replace("-", "_")
        for section in raw.split(",")
        if section.strip()
    }
    return sections or None


def build_dashboard_snapshot_sections(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    detail: DashboardDetail = "summary",
    limit: int | None = None,
    sections: set[str],
    timeout_budget_ms: int | None = None,
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit, settings)
    generated_at = _now()
    started_at = time.monotonic()
    ordered_sections = _ordered_requested_sections(sections)
    known_sections = set(DASHBOARD_SECTIONS)
    warnings: list[str] = []
    included_sections: list[str] = []
    skipped_sections: list[dict[str, str]] = []
    section_latency_ms: dict[str, float] = {}
    context: dict[str, Any] = {"snapshot": {}}

    for section in ordered_sections:
        if section not in known_sections:
            warnings.append(f"UNKNOWN_DASHBOARD_SECTION:{section}")
            skipped_sections.append(
                {"section": section, "reason": "UNKNOWN_DASHBOARD_SECTION"}
            )
            continue
        if section not in FAST_DASHBOARD_SUPPORTED_SECTIONS:
            warnings.append(f"UNSUPPORTED_DASHBOARD_FAST_SECTION:{section}")
            skipped_sections.append(
                {
                    "section": section,
                    "reason": "UNSUPPORTED_DASHBOARD_FAST_SECTION",
                }
            )
            continue
        if _dashboard_timeout_budget_exceeded(started_at, timeout_budget_ms) and (
            section not in FAST_DASHBOARD_REQUIRED_SECTIONS
        ):
            warnings.append(f"SKIPPED_TIMEOUT_BUDGET:{section}")
            skipped_sections.append(
                {"section": section, "reason": "SKIPPED_TIMEOUT_BUDGET"}
            )
            continue

        section_started_at = time.monotonic()
        try:
            value = _build_dashboard_fast_section(
                connection,
                settings,
                section=section,
                detail=detail,
                bounded_limit=bounded_limit,
                generated_at=generated_at,
                context=context,
            )
        finally:
            section_latency_ms[section] = round(
                (time.monotonic() - section_started_at) * 1000,
                3,
            )
        context["snapshot"][section] = value
        included_sections.append(section)

    total_latency_ms = round((time.monotonic() - started_at) * 1000, 3)
    snapshot = context["snapshot"]
    snapshot.update(
        {
            "generated_at": generated_at,
            "detail": detail,
            "limit": bounded_limit,
            "fast_path": True,
            "requested_sections": ordered_sections,
            "included_sections": included_sections,
            "skipped_sections": skipped_sections,
            "section_latency_ms": section_latency_ms,
            "total_latency_ms": total_latency_ms,
            "timeout_budget_ms": timeout_budget_ms,
            "warnings": warnings,
        }
    )
    return snapshot


def build_dashboard_pipeline_summary_fast(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    gateway_status: dict[str, Any],
    market_data_status: dict[str, Any],
    projection_outbox_status: dict[str, Any],
    projection_outbox_backlog: dict[str, Any],
    market_data_reconcile: dict[str, Any],
    market_data_append_only_routing: dict[str, Any],
    market_data_append_only_controller: dict[str, Any],
    market_reference_status: dict[str, Any],
    market_reference_reconcile: dict[str, Any],
    market_reference_append_only_routing: dict[str, Any],
    pipeline_coherency: dict[str, Any],
) -> dict[str, Any]:
    command_type_counts = _command_type_counts(connection)
    order_command_count = _order_command_count(command_type_counts)
    runtime_execution_locks = get_runtime_execution_lock_status(connection)
    live_sim_order_plan_uniqueness = get_live_sim_order_plan_uniqueness_status(
        connection
    )
    order_broker_boundaries = get_order_broker_boundary_status(connection)
    return {
        "fast_path": True,
        "read_only": True,
        "no_order_side_effects": True,
        "no_trading_side_effects": True,
        "gateway": {
            "recent_event_count": int(gateway_status.get("recent_event_count") or 0),
            "queued_command_count": int(gateway_status.get("queued_command_count") or 0),
            "failed_command_count": int(gateway_status.get("failed_command_count") or 0),
        },
        "runtime_execution_locks": runtime_execution_locks,
        "live_sim_order_plan_uniqueness": live_sim_order_plan_uniqueness,
        "order_broker_boundaries": order_broker_boundaries,
        "coherency": pipeline_coherency,
        "market_data": {
            "latest_tick_count": int(market_data_status.get("latest_tick_count") or 0),
            "bar_count": int(market_data_status.get("bar_count") or 0),
            "projection_error_count": int(
                market_data_status.get("projection_error_count") or 0
            ),
        },
        "projection_outbox": {
            "enabled": bool(projection_outbox_status.get("enabled")),
            "shadow_mode": bool(projection_outbox_status.get("shadow_mode")),
            "worker_enabled": bool(projection_outbox_status.get("worker_enabled")),
            "apply_projection_enabled": bool(
                projection_outbox_status.get("apply_projection_enabled")
            ),
            "market_data_apply_enabled": bool(
                projection_outbox_status.get("market_data_apply_enabled")
            ),
            "market_reference_apply_enabled": bool(
                projection_outbox_status.get("market_reference_apply_enabled")
            ),
            "projection_side_effects_allowed": bool(
                projection_outbox_status.get("projection_side_effects_allowed")
            ),
            "total_count": int(projection_outbox_status.get("total_count") or 0),
            "pending_count": int(projection_outbox_status.get("pending_count") or 0),
            "processing_count": int(
                projection_outbox_status.get("processing_count") or 0
            ),
            "applied_count": int(projection_outbox_status.get("applied_count") or 0),
            "skipped_count": int(projection_outbox_status.get("skipped_count") or 0),
            "error_count": int(projection_outbox_status.get("error_count") or 0),
            "dead_letter_count": int(
                projection_outbox_status.get("dead_letter_count") or 0
            ),
            "oldest_pending_at": projection_outbox_status.get("oldest_pending_at"),
            "latest_error": projection_outbox_status.get("latest_error"),
            "by_projection_name": projection_outbox_status.get("by_projection_name", {}),
            "last_apply_mode": projection_outbox_status.get("last_apply_mode"),
            "warnings": list(projection_outbox_status.get("warnings") or []),
            "backlog_readiness_status": projection_outbox_backlog.get(
                "readiness_status"
            ),
            "pr11_condition_event_cutover_ready": bool(
                projection_outbox_backlog.get("pr11_condition_event_cutover_ready")
            ),
            "recent_pending_count": int(
                projection_outbox_backlog.get("recent_pending_count") or 0
            ),
            "blocking_pending_count": int(
                projection_outbox_backlog.get("blocking_pending_count") or 0
            ),
            "non_blocking_shadow_pending_count": int(
                projection_outbox_backlog.get("non_blocking_shadow_pending_count") or 0
            ),
            "bulk_retire_eligible_count": int(
                projection_outbox_backlog.get("bulk_retire_eligible_count") or 0
            ),
            "condition_event_pending_count": int(
                projection_outbox_backlog.get("condition_event_pending_count") or 0
            ),
            "condition_event_blocking_pending_count": int(
                projection_outbox_backlog.get("condition_event_blocking_pending_count")
                or 0
            ),
            "stale_processing_count": int(
                projection_outbox_backlog.get("stale_processing_count") or 0
            ),
            "operator_actions": list(
                projection_outbox_backlog.get("operator_actions") or []
            ),
            "recommended_action": _projection_outbox_backlog_recommended_action(
                projection_outbox_backlog
            ),
            "read_only": True,
        },
        "market_data_projection_reconcile": _market_data_reconcile_summary(
            market_data_reconcile
        ),
        "market_data_append_only_routing": _market_data_append_only_routing_summary(
            market_data_append_only_routing
        ),
        "market_data_append_only_controller": (
            _market_data_append_only_controller_summary(
                market_data_append_only_controller
            )
        ),
        "market_reference": _market_reference_summary(
            market_reference_status,
            market_reference_reconcile,
            market_reference_append_only_routing,
        ),
        "order_safety": {
            "order_command_count": order_command_count,
            "command_type_counts": command_type_counts,
            "order_commands_allowed": False,
            "read_only": True,
        },
        "stage_statuses": [
            {
                "stage": "Core",
                "status": "PASS",
                "endpoint": "/health",
                "reason_codes": [],
                "last_updated_at": _now(),
            },
            _gateway_stage_status(gateway_status),
            _market_data_stage_status(market_data_status),
            {
                "stage": "OrderSafety",
                "status": "PASS",
                "endpoint": "/api/gateway/commands/status",
                "count": order_command_count,
                "reason_codes": [],
                "last_updated_at": _now(),
            },
        ],
    }


def _ordered_requested_sections(sections: set[str]) -> list[str]:
    ordered = [section for section in DASHBOARD_SECTIONS if section in sections]
    extras = sorted(section for section in sections if section not in DASHBOARD_SECTIONS)
    return ordered + extras


def _dashboard_timeout_budget_exceeded(
    started_at: float,
    timeout_budget_ms: int | None,
) -> bool:
    if timeout_budget_ms is None:
        return False
    return (time.monotonic() - started_at) * 1000 >= float(timeout_budget_ms)


def _build_dashboard_fast_section(
    connection: sqlite3.Connection,
    settings: Settings,
    *,
    section: str,
    detail: DashboardDetail,
    bounded_limit: int,
    generated_at: str,
    context: dict[str, Any],
) -> Any:
    del detail

    def gateway_status() -> dict[str, Any]:
        if "gateway_status" not in context:
            gateway_events = list_recent_gateway_events(connection, limit=bounded_limit)
            context["gateway_events"] = gateway_events
            command_counts = _command_status_counts(connection)
            context["command_counts"] = command_counts
            context["command_type_counts"] = _command_type_counts(connection)
            context["gateway_status"] = _gateway_status_section(
                settings,
                get_gateway_status_values(connection),
                command_counts,
                recent_event_count=count_recent_gateway_events(connection),
                latest_heartbeat_payload=_latest_gateway_heartbeat_payload(
                    gateway_events
                ),
            )
        return context["gateway_status"]

    def gateway_events() -> list[dict[str, Any]]:
        if "gateway_events" not in context:
            context["gateway_events"] = list_recent_gateway_events(
                connection,
                limit=bounded_limit,
            )
        return context["gateway_events"]

    def market_data_status() -> dict[str, Any]:
        if "market_data_status" not in context:
            context["market_data_status"] = get_market_data_status(
                connection,
                settings=settings,
            )
        return context["market_data_status"]

    def projection_outbox_status() -> dict[str, Any]:
        if "projection_outbox_status" not in context:
            context["projection_outbox_status"] = get_projection_outbox_status(
                connection,
                settings=settings,
            )
        return context["projection_outbox_status"]

    def market_data_reconcile() -> dict[str, Any]:
        if "market_data_reconcile" not in context:
            context["market_data_reconcile"] = (
                get_latest_market_data_projection_reconcile(connection)
            )
        return context["market_data_reconcile"]

    def market_data_append_only_routing() -> dict[str, Any]:
        if "market_data_append_only_routing" not in context:
            context["market_data_append_only_routing"] = (
                get_latest_market_data_append_only_routing_status(
                    connection,
                    settings=settings,
                )
            )
        return context["market_data_append_only_routing"]

    def market_data_append_only_controller() -> dict[str, Any]:
        if "market_data_append_only_controller" not in context:
            context["market_data_append_only_controller"] = (
                build_market_data_append_only_controller_status(
                    connection,
                    settings=settings,
                ).to_dict()
            )
        return context["market_data_append_only_controller"]

    def market_reference_status() -> dict[str, Any]:
        if "market_reference_status" not in context:
            context["market_reference_status"] = build_market_reference_status(
                connection,
                settings=settings,
            )
        return context["market_reference_status"]

    def market_reference_reconcile() -> dict[str, Any]:
        if "market_reference_reconcile" not in context:
            context["market_reference_reconcile"] = (
                get_latest_market_reference_projection_reconcile(connection)
            )
        return context["market_reference_reconcile"]

    def market_reference_append_only_routing() -> dict[str, Any]:
        if "market_reference_append_only_routing" not in context:
            context["market_reference_append_only_routing"] = (
                get_latest_market_reference_append_only_routing_status(
                    connection,
                    settings=settings,
                )
            )
        return context["market_reference_append_only_routing"]

    def market_index_reconcile() -> dict[str, Any]:
        if "market_index_reconcile" not in context:
            context["market_index_reconcile"] = (
                get_latest_market_index_projection_reconcile(connection)
            )
        return context["market_index_reconcile"]

    def market_index_append_only_routing() -> dict[str, Any]:
        if "market_index_append_only_routing" not in context:
            context["market_index_append_only_routing"] = (
                get_latest_market_index_append_only_routing_status(
                    connection,
                    settings=settings,
                )
            )
        return context["market_index_append_only_routing"]

    def market_regime_reconcile() -> dict[str, Any]:
        if "market_regime_reconcile" not in context:
            context["market_regime_reconcile"] = (
                get_latest_market_regime_projection_reconcile(connection)
            )
        return context["market_regime_reconcile"]

    def market_regime_append_only_routing() -> dict[str, Any]:
        if "market_regime_append_only_routing" not in context:
            context["market_regime_append_only_routing"] = (
                get_latest_market_regime_append_only_routing_status(
                    connection,
                    settings=settings,
                )
            )
        return context["market_regime_append_only_routing"]

    def market_scan_reconcile() -> dict[str, Any]:
        if "market_scan_reconcile" not in context:
            context["market_scan_reconcile"] = (
                get_latest_market_scan_projection_reconcile(connection)
            )
        return context["market_scan_reconcile"]

    def market_scan_append_only_routing() -> dict[str, Any]:
        if "market_scan_append_only_routing" not in context:
            context["market_scan_append_only_routing"] = (
                get_latest_market_scan_append_only_routing_status(
                    connection,
                    settings=settings,
                )
            )
        return context["market_scan_append_only_routing"]

    def projection_outbox_backlog() -> dict[str, Any]:
        if "projection_outbox_backlog" not in context:
            context["projection_outbox_backlog"] = (
                build_projection_outbox_backlog_status(
                    connection,
                    settings=settings,
                    latest_reconcile=market_data_reconcile(),
                    routing_status=market_data_append_only_routing(),
                    sample_limit=3,
                ).to_dict()
            )
            projection_outbox_status().update(
                projection_outbox_backlog_summary_fields(
                    context["projection_outbox_backlog"]
                )
            )
        return context["projection_outbox_backlog"]

    def projection_retention_status() -> dict[str, Any]:
        if "projection_retention_status" not in context:
            context["projection_retention_status"] = get_event_retention_status(
                connection,
                settings=settings,
            )
        return context["projection_retention_status"]

    def pipeline_coherency() -> dict[str, Any]:
        if "pipeline_coherency" not in context:
            context["pipeline_coherency"] = build_pipeline_coherency_status(
                connection,
                max_age_sec=settings.entry_timing_stale_max_seconds,
                limit=bounded_limit,
            )
        return context["pipeline_coherency"]

    if section == "safety":
        return build_safety_section(settings)
    if section == "system":
        return _system_section(settings, generated_at)
    if section == "gateway":
        return gateway_status()
    if section == "condition_fusion":
        status = gateway_status()
        rows = list_condition_fusion(
            connection,
            settings=settings,
            registered_codes=status["realtime_registered_codes"],
            limit=bounded_limit,
        )
        profiles = get_condition_profile_metrics(
            connection,
            settings=settings,
            limit=bounded_limit,
        )
        return _condition_fusion_section(
            rows,
            profiles,
            fallback_profiles=status.get("condition_profile_metrics", []),
        )
    if section == "runtime_execution_locks":
        return get_runtime_execution_lock_status(connection)
    if section == "live_sim_order_plan_uniqueness":
        return get_live_sim_order_plan_uniqueness_status(connection)
    if section == "order_broker_boundaries":
        return get_order_broker_boundary_status(connection)
    if section == "live_sim_lifecycle_consumer":
        return build_live_sim_lifecycle_cutover_status(connection, settings=settings)
    if section == "projection_replay":
        return get_projection_replay_status()
    if section == "projection_watermarks":
        return get_projection_watermark_status(connection)
    if section == "projection_retention":
        return projection_retention_status()
    if section == "market_data":
        return {
            "status": market_data_status(),
            "latest_ticks": list_latest_ticks(connection, limit=bounded_limit),
            "cross_exchange": {
                "read_only": True,
                "enabled": settings.risk_cross_exchange_divergence_bp > 0,
                "threshold_bp": settings.risk_cross_exchange_divergence_bp,
                "latest_observations": list_recent_cross_exchange_observations(
                    connection,
                    limit=min(bounded_limit, 10),
                ),
            },
        }
    if section == "market_reference":
        return market_reference_status()
    if section == "market_indexes":
        latest_ticks = list_latest_market_index_ticks(
            connection,
            limit=bounded_limit,
        )
        return {
            "status": get_market_index_status(connection, settings=settings),
            "latest_ticks": latest_ticks,
            "latest_by_code": _market_index_latest_by_code(latest_ticks),
            "gateway_adapter": _market_index_gateway_adapter_section(gateway_status()),
            "projection_reconcile": market_index_reconcile(),
            "append_only_routing": market_index_append_only_routing(),
        }
    if section == "market_regime":
        return get_market_regime_status(connection, settings=settings)
    if section == "market_scan":
        return get_market_scan_status(connection, settings=settings)
    if section == "market_context":
        return get_market_context_status(connection, settings=settings)
    if section == "realtime_subscription":
        return build_realtime_subscription_plan(
            connection,
            settings=settings,
            registered_codes=gateway_status()["realtime_registered_codes"],
            queue_commands=False,
        ).to_dict()
    if section == "recent_events":
        return {"gateway_events": gateway_events()}
    if section == "errors":
        return build_dashboard_errors(
            connection,
            settings=settings,
            limit=min(bounded_limit, 20),
            projection_retention_status=projection_retention_status(),
        )
    if section == "projection_outbox":
        projection_outbox_backlog()
        return projection_outbox_status()
    if section == "projection_outbox_backlog":
        return projection_outbox_backlog()
    if section == "incremental_evaluation":
        return get_incremental_evaluation_status(connection, settings=settings)
    if section == "pipeline_coherency":
        return pipeline_coherency()
    if section == "market_data_projection_reconcile":
        return market_data_reconcile()
    if section == "market_data_append_only_routing":
        return market_data_append_only_routing()
    if section == "market_data_append_only_controller":
        return market_data_append_only_controller()
    if section == "market_reference_projection_reconcile":
        return market_reference_reconcile()
    if section == "market_reference_append_only_routing":
        return market_reference_append_only_routing()
    if section == "market_index_projection_reconcile":
        return market_index_reconcile()
    if section == "market_index_append_only_routing":
        return market_index_append_only_routing()
    if section == "market_regime_projection_reconcile":
        return market_regime_reconcile()
    if section == "market_regime_append_only_routing":
        return market_regime_append_only_routing()
    if section == "market_scan_projection_reconcile":
        return market_scan_reconcile()
    if section == "market_scan_append_only_routing":
        return market_scan_append_only_routing()
    if section == "pipeline_summary":
        return build_dashboard_pipeline_summary_fast(
            connection,
            settings,
            gateway_status=gateway_status(),
            market_data_status=market_data_status(),
            projection_outbox_status=projection_outbox_status(),
            projection_outbox_backlog=projection_outbox_backlog(),
            market_data_reconcile=market_data_reconcile(),
            market_data_append_only_routing=market_data_append_only_routing(),
            market_data_append_only_controller=market_data_append_only_controller(),
            market_reference_status=market_reference_status(),
            market_reference_reconcile=market_reference_reconcile(),
            market_reference_append_only_routing=(
                market_reference_append_only_routing()
            ),
            pipeline_coherency=pipeline_coherency(),
        )

    raise ValueError(f"unsupported dashboard fast section: {section}")


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
        "snapshot_sections_enabled": settings.dashboard_snapshot_sections_enabled,
        "snapshot_fast_cache_ttl_sec": settings.dashboard_snapshot_fast_cache_ttl_sec,
        "snapshot_fast_default_limit": settings.dashboard_snapshot_fast_default_limit,
        "snapshot_fast_timeout_budget_ms": (
            settings.dashboard_snapshot_fast_timeout_budget_ms
        ),
        "snapshot_warn_latency_ms": settings.dashboard_snapshot_warn_latency_ms,
        "snapshot_fail_latency_ms": settings.dashboard_snapshot_fail_latency_ms,
        "show_raw_json": settings.dashboard_show_raw_json,
        "sections": DASHBOARD_SECTIONS,
        "fast_sections": sorted(FAST_DASHBOARD_SUPPORTED_SECTIONS),
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
            "legacy LIVE flag가 켜져 있어도 capability source는 TRADING_PROFILE입니다."
        )
    return {
        "trading_profile": settings.trading_profile.value,
        "trading_mode": settings.trading_mode.value,
        "trading_capabilities": settings.trading_capabilities.to_dict(),
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
        "deprecated_flag_warnings": list(settings.deprecated_flag_warning_dicts),
        "deprecated_flag_warning_count": len(settings.deprecated_flag_warnings),
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
    projection_retention_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    bounded_limit = _bounded_limit(limit, settings)
    recent_gateway_events = list_recent_gateway_events(connection, limit=bounded_limit)
    gateway_problem_events = [
        event
        for event in recent_gateway_events
        if event.get("status") not in {"ACCEPTED", None} or event.get("error_message")
    ]
    retention = dict(projection_retention_status or {})
    if not retention:
        retention = get_event_retention_status(connection, settings=settings)
    projection_retention_rca = build_projection_retention_rca(
        connection,
        cutoff_at=str(retention["cutoff_at"]),
        limit=min(bounded_limit, 20),
        blocked_only=True,
    )
    return {
        "market_projection_errors": list_projection_errors(connection, limit=bounded_limit),
        "projection_retention_rca": projection_retention_rca,
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


def _market_index_latest_by_code(latest_ticks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for tick in latest_ticks:
        index_code = str(tick.get("index_code") or "").strip().upper()
        if index_code and index_code not in result:
            result[index_code] = dict(tick)
    return result


def _market_index_gateway_adapter_section(gateway_status: dict[str, Any]) -> dict[str, Any]:
    realtime_enabled = bool(gateway_status.get("market_index_realtime_enabled"))
    tr_bootstrap_enabled = bool(
        gateway_status.get("market_index_tr_bootstrap_enabled")
    )
    return {
        "enabled": bool(gateway_status.get("market_index_enabled")),
        "realtime_enabled": realtime_enabled,
        "tr_bootstrap_enabled": tr_bootstrap_enabled,
        "realtime_source_status": "ENABLED" if realtime_enabled else "DISABLED",
        "tr_bootstrap_source_status": (
            "CONFIGURED_NOT_IMPLEMENTED" if tr_bootstrap_enabled else "DISABLED"
        ),
        "source_contract_explicit": True,
        "parser_confidence_separate_from_data_usability": True,
        "nxt_is_not_valid_market_index_evidence": True,
        "configured_codes": gateway_status.get("market_index_codes") or [],
        "registered_codes": gateway_status.get("market_index_registered_codes") or [],
        "screen_no": gateway_status.get("market_index_screen_no") or "",
        "poll_sec": gateway_status.get("market_index_poll_sec"),
        "callback_count": gateway_status.get("market_index_callback_count") or 0,
        "parsed_tick_count": gateway_status.get("parsed_market_index_tick_count") or 0,
        "parse_error_count": gateway_status.get("market_index_parse_error_count") or 0,
        "latest_tick_at": gateway_status.get("latest_market_index_tick_at") or "",
        "latest_parse_error": gateway_status.get("latest_market_index_parse_error") or {},
        "latest_registration_result": gateway_status.get(
            "latest_market_index_registration_result"
        )
        or {},
        "latest_registration_at": gateway_status.get(
            "latest_market_index_registration_at"
        )
        or "",
        "recover_count": gateway_status.get("market_index_recover_count") or 0,
        "latest_recover_at": gateway_status.get("latest_market_index_recover_at") or "",
        "recover_error": gateway_status.get("market_index_recover_error") or "",
        "health": gateway_status.get("market_index_adapter_health") or "DISABLED",
    }


_GATEWAY_HEARTBEAT_STATUS_KEYS: tuple[str, ...] = (
    "kiwoom_logged_in",
    "login_threaded",
    "server_mode",
    "condition_load_state",
    "condition_load_requested_at",
    "condition_load_retry_count",
    "condition_load_timeout_count",
    "condition_reason_codes",
    "condition_session_profile",
    "condition_profiles",
    "condition_profile_screen_map",
    "condition_send_results",
    "condition_profile_metrics",
    "adaptive_realtime_budget",
    "latest_condition_ver_callback_at",
    "latest_condition_ver_result",
    "registered_realtime_code_count",
    "realtime_registered_codes",
    "realtime_exchange",
    "realtime_registered_kiwoom_codes",
    "realtime_registration_requested_count",
    "realtime_registration_success_count",
    "market_index_enabled",
    "market_index_realtime_enabled",
    "market_index_tr_bootstrap_enabled",
    "market_index_codes",
    "market_index_screen_no",
    "market_index_poll_sec",
    "market_index_registered_codes",
    "market_index_callback_count",
    "parsed_market_index_tick_count",
    "market_index_parse_error_count",
    "latest_market_index_tick_at",
    "latest_market_index_parse_error",
    "latest_market_index_registration_result",
    "latest_market_index_registration_at",
    "market_index_adapter_health",
    "market_index_recover_count",
    "latest_market_index_recover_at",
    "market_index_recover_error",
    "latest_realtime_registration_at",
    "latest_realtime_registration_result",
    "latest_realtime_callback_at",
    "raw_realtime_callback_count",
    "realtime_callback_count",
    "parsed_price_tick_count",
    "realtime_parse_error_count",
    "latest_realtime_parse_error",
    "realtime_subscription_health",
    "realtime_callback_real_type_counts",
    "realtime_recover_count",
    "raw_callback_counts",
    "latest_callback_at_by_method",
    "latest_active_x_thread_audit",
)


def _decode_gateway_status_value(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _gateway_heartbeat_payload_from_status_values(
    status_values: dict[str, str],
) -> dict[str, Any]:
    return {
        key: _decode_gateway_status_value(status_values[key])
        for key in _GATEWAY_HEARTBEAT_STATUS_KEYS
        if key in status_values
    }


def _gateway_status_section(
    settings: Settings,
    status_values: dict[str, str],
    command_counts: dict[str, int],
    *,
    recent_event_count: int,
    latest_heartbeat_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    heartbeat_payload = _gateway_heartbeat_payload_from_status_values(status_values)
    heartbeat_payload.update(latest_heartbeat_payload or {})
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
        "market_index_enabled": heartbeat_payload.get("market_index_enabled"),
        "market_index_realtime_enabled": heartbeat_payload.get(
            "market_index_realtime_enabled"
        ),
        "market_index_tr_bootstrap_enabled": heartbeat_payload.get(
            "market_index_tr_bootstrap_enabled"
        ),
        "market_index_codes": heartbeat_payload.get("market_index_codes") or [],
        "market_index_screen_no": heartbeat_payload.get("market_index_screen_no") or "",
        "market_index_poll_sec": heartbeat_payload.get("market_index_poll_sec"),
        "market_index_registered_codes": heartbeat_payload.get(
            "market_index_registered_codes"
        )
        or [],
        "market_index_callback_count": heartbeat_payload.get("market_index_callback_count"),
        "parsed_market_index_tick_count": heartbeat_payload.get(
            "parsed_market_index_tick_count"
        ),
        "market_index_parse_error_count": heartbeat_payload.get(
            "market_index_parse_error_count"
        ),
        "latest_market_index_tick_at": heartbeat_payload.get("latest_market_index_tick_at"),
        "latest_market_index_parse_error": heartbeat_payload.get(
            "latest_market_index_parse_error"
        )
        or {},
        "latest_market_index_registration_result": heartbeat_payload.get(
            "latest_market_index_registration_result"
        )
        or {},
        "latest_market_index_registration_at": heartbeat_payload.get(
            "latest_market_index_registration_at"
        ),
        "market_index_adapter_health": heartbeat_payload.get(
            "market_index_adapter_health"
        )
        or "DISABLED",
        "market_index_recover_count": heartbeat_payload.get("market_index_recover_count"),
        "latest_market_index_recover_at": heartbeat_payload.get(
            "latest_market_index_recover_at"
        ),
        "market_index_recover_error": heartbeat_payload.get(
            "market_index_recover_error"
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
    condition_fusion_status: dict[str, Any],
    projection_outbox_status: dict[str, Any],
    projection_outbox_backlog: dict[str, Any],
    market_data_reconcile: dict[str, Any],
    market_data_append_only_routing: dict[str, Any],
    market_data_append_only_controller: dict[str, Any],
    market_reference_status: dict[str, Any],
    market_reference_reconcile: dict[str, Any],
    market_reference_append_only_routing: dict[str, Any],
    runtime_execution_locks: dict[str, Any],
    live_sim_order_plan_uniqueness: dict[str, Any],
    order_broker_boundaries: dict[str, Any],
    pipeline_coherency: dict[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    return {
        "stage_statuses": _pipeline_stage_statuses(
            gateway_status=gateway_status,
            market_data_status=market_data_status,
            realtime_subscription=realtime_subscription,
            condition_fusion_status=condition_fusion_status,
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
        "runtime_execution_locks": runtime_execution_locks,
        "live_sim_order_plan_uniqueness": live_sim_order_plan_uniqueness,
        "order_broker_boundaries": order_broker_boundaries,
        "coherency": pipeline_coherency,
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
        "condition_fusion": {
            "profile_count": condition_fusion_status.get("profile_count", 0),
            "fused_code_count": condition_fusion_status.get("fused_code_count", 0),
            "risk_blocked_count": condition_fusion_status.get("risk_blocked_count", 0),
            "discovery_only_count": condition_fusion_status.get("discovery_only_count", 0),
            "subscribed_count": condition_fusion_status.get("subscribed_count", 0),
            "not_buy_signal_count": condition_fusion_status.get("not_buy_signal_count", 0),
            "not_buy_signal": bool(condition_fusion_status.get("not_buy_signal")),
            "read_only": True,
        },
        "projection_outbox": {
            "enabled": projection_outbox_status["enabled"],
            "shadow_mode": projection_outbox_status["shadow_mode"],
            "worker_enabled": projection_outbox_status["worker_enabled"],
            "apply_projection_enabled": projection_outbox_status[
                "apply_projection_enabled"
            ],
            "market_data_apply_enabled": projection_outbox_status[
                "market_data_apply_enabled"
            ],
            "market_reference_apply_enabled": projection_outbox_status[
                "market_reference_apply_enabled"
            ],
            "projection_side_effects_allowed": projection_outbox_status[
                "projection_side_effects_allowed"
            ],
            "total_count": projection_outbox_status["total_count"],
            "pending_count": projection_outbox_status["pending_count"],
            "processing_count": projection_outbox_status["processing_count"],
            "applied_count": projection_outbox_status["applied_count"],
            "skipped_count": projection_outbox_status["skipped_count"],
            "error_count": projection_outbox_status["error_count"],
            "dead_letter_count": projection_outbox_status["dead_letter_count"],
            "oldest_pending_at": projection_outbox_status["oldest_pending_at"],
            "latest_error": projection_outbox_status["latest_error"],
            "by_projection_name": projection_outbox_status["by_projection_name"],
            "last_apply_mode": projection_outbox_status["last_apply_mode"],
            "warnings": projection_outbox_status["warnings"],
            "backlog_readiness_status": projection_outbox_backlog.get(
                "readiness_status"
            ),
            "pr11_condition_event_cutover_ready": bool(
                projection_outbox_backlog.get("pr11_condition_event_cutover_ready")
            ),
            "recent_pending_count": int(
                projection_outbox_backlog.get("recent_pending_count") or 0
            ),
            "blocking_pending_count": int(
                projection_outbox_backlog.get("blocking_pending_count") or 0
            ),
            "non_blocking_shadow_pending_count": int(
                projection_outbox_backlog.get("non_blocking_shadow_pending_count") or 0
            ),
            "bulk_retire_eligible_count": int(
                projection_outbox_backlog.get("bulk_retire_eligible_count") or 0
            ),
            "condition_event_pending_count": int(
                projection_outbox_backlog.get("condition_event_pending_count") or 0
            ),
            "condition_event_blocking_pending_count": int(
                projection_outbox_backlog.get("condition_event_blocking_pending_count")
                or 0
            ),
            "stale_processing_count": int(
                projection_outbox_backlog.get("stale_processing_count") or 0
            ),
            "operator_actions": list(
                projection_outbox_backlog.get("operator_actions") or []
            ),
            "recommended_action": _projection_outbox_backlog_recommended_action(
                projection_outbox_backlog
            ),
            "read_only": True,
        },
        "market_data_projection_reconcile": _market_data_reconcile_summary(
            market_data_reconcile
        ),
        "market_data_append_only_routing": _market_data_append_only_routing_summary(
            market_data_append_only_routing
        ),
        "market_data_append_only_controller": (
            _market_data_append_only_controller_summary(
                market_data_append_only_controller
            )
        ),
        "market_reference": _market_reference_summary(
            market_reference_status,
            market_reference_reconcile,
            market_reference_append_only_routing,
        ),
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


def _market_data_reconcile_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    latest = payload.get("latest_run")
    if not isinstance(latest, Mapping):
        return {
            "latest_status": None,
            "append_only_ready": False,
            "checked_event_count": 0,
            "missing_projection_count": 0,
            "outbox_error_count": 0,
            "dead_letter_count": 0,
            "watermark_risk_count": 0,
            "synthetic_child_event_issue_count": 0,
            "reason_codes": [],
            "latest_run_id": None,
            "latest_run_created_at": None,
            "warnings": [
                "market_data append-only 전환 보류",
                "Gateway inline projection remains enabled",
            ],
            "read_only": True,
            "no_trading_side_effects": True,
        }
    append_only_ready = bool(latest.get("append_only_ready"))
    warnings = ["Gateway inline projection remains enabled"]
    if not append_only_ready:
        warnings.insert(0, "market_data append-only 전환 보류")
    return {
        "latest_status": latest.get("status"),
        "append_only_ready": append_only_ready,
        "checked_event_count": int(latest.get("checked_event_count") or 0),
        "missing_projection_count": int(latest.get("missing_projection_count") or 0),
        "outbox_error_count": int(latest.get("outbox_error_count") or 0),
        "dead_letter_count": int(latest.get("outbox_dead_letter_count") or 0),
        "watermark_risk_count": int(latest.get("watermark_risk_count") or 0),
        "synthetic_child_event_issue_count": int(
            latest.get("synthetic_child_event_issue_count") or 0
        ),
        "reason_codes": list(latest.get("reason_codes") or []),
        "latest_run_id": latest.get("run_id"),
        "latest_run_created_at": latest.get("created_at"),
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _market_reference_summary(
    status_payload: Mapping[str, Any],
    reconcile_payload: Mapping[str, Any],
    routing_payload: Mapping[str, Any],
) -> dict[str, Any]:
    latest_run = (
        reconcile_payload.get("latest_run")
        if isinstance(reconcile_payload, Mapping)
        else None
    )
    outbox = status_payload.get("outbox") if isinstance(status_payload, Mapping) else {}
    if not isinstance(outbox, Mapping):
        outbox = {}
    return {
        "pr": "PR-14",
        "controller_status": routing_payload.get("status"),
        "health": status_payload.get("latest_reconcile_status")
        or (latest_run.get("status") if isinstance(latest_run, Mapping) else None),
        "append_only_ready": bool(status_payload.get("append_only_ready")),
        "membership_count": int(status_payload.get("membership_count") or 0),
        "missing_membership_count": int(
            status_payload.get("missing_membership_count") or 0
        ),
        "latest_market_symbols_event_id": status_payload.get(
            "latest_market_symbols_event_id"
        ),
        "latest_market_symbols_event_ts": status_payload.get(
            "latest_market_symbols_event_ts"
        ),
        "outbox_error_count": int(outbox.get("error_count") or 0),
        "outbox_dead_letter_count": int(outbox.get("dead_letter_count") or 0),
        "would_skip_inline_count": int(
            routing_payload.get("would_skip_inline_count") or 0
        ),
        "effective_skip_inline_count": int(
            routing_payload.get("effective_skip_inline_count") or 0
        ),
        "global_kill_switch": bool(routing_payload.get("global_kill_switch")),
        "cutover_enabled": bool(routing_payload.get("cutover_enabled")),
        "worker_apply_enabled": bool(routing_payload.get("worker_apply_enabled")),
        "skip_budget_limit": int(routing_payload.get("skip_budget_limit") or 0),
        "skip_budget_used_current_minute": int(
            routing_payload.get("skip_budget_used_current_minute") or 0
        ),
        "skip_budget_remaining_current_minute": int(
            routing_payload.get("skip_budget_remaining_current_minute") or 0
        ),
        "rollback_required": bool(routing_payload.get("rollback_required")),
        "rollback_reason_codes": list(
            routing_payload.get("rollback_reason_codes") or []
        ),
        "effective_skip_health": dict(
            routing_payload.get("effective_skip_health") or {}
        ),
        "rollback_hint": routing_payload.get("rollback_hint"),
        "warnings": list(status_payload.get("warnings") or []),
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _market_data_append_only_routing_summary(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    effective_skip_count = int(payload.get("effective_skip_inline_count") or 0)
    warnings = list(payload.get("warnings") or [])
    if "PR-11 condition_event limited cutover is feature-flagged" not in warnings:
        warnings.append("PR-11 condition_event limited cutover is feature-flagged")
    if "PR-9 tr_response limited cutover is feature-flagged" not in warnings:
        warnings.append("PR-9 tr_response limited cutover is feature-flagged")
    if "candidate ingest remains outside projection_outbox worker" not in warnings:
        warnings.append("candidate ingest remains outside projection_outbox worker")
    if "LIVE_REAL/order behavior unchanged" not in warnings:
        warnings.append("LIVE_REAL/order behavior unchanged")
    if (
        "rollback: disable gateway_market_data_append_only_condition_event_cutover_enabled"
        not in warnings
    ):
        warnings.append(
            "rollback: disable gateway_market_data_append_only_condition_event_cutover_enabled"
        )
    rollback_hint = payload.get("rollback_hint") or (
        "disable gateway_market_data_append_only_condition_event_cutover_enabled"
    )
    latest_reconcile = payload.get("latest_reconcile")
    latest_reconcile_run = (
        latest_reconcile.get("latest_run")
        if isinstance(latest_reconcile, Mapping)
        else None
    )
    latest_decision = payload.get("latest_decision")
    return {
        "pr": "PR-11",
        "price_tick_pr": "PR-7",
        "tr_response_pr": "PR-9",
        "condition_event_pr": "PR-11",
        "cutover_status": "price_tick + tr_response + condition_event limited cutover",
        "pr9_tr_response_limited_cutover": True,
        "pr11_condition_event_limited_cutover": True,
        "pr11_condition_event_cutover_status": "PR-11 condition_event limited cutover",
        "price_tick_cutover_status": "UNCHANGED_PR7",
        "tr_response_cutover_status": "LIMITED_PR9",
        "condition_event_cutover_status": (
            "LIMITED_PR11_ENABLED"
            if bool(payload.get("condition_event_cutover_enabled"))
            else "DISABLED_INLINE_DEFAULT"
        ),
        "condition_event_side_effect_migration_status": "WORKER_DEFERRED_READY",
        "tr_response_side_effect_migration_status": "WORKER_DEFERRED_READY",
        "dry_run_enabled": bool(payload.get("dry_run_enabled")),
        "cutover_enabled": bool(payload.get("cutover_enabled")),
        "price_tick_cutover_enabled": bool(payload.get("price_tick_cutover_enabled")),
        "tr_response_dry_run_enabled": bool(
            payload.get("tr_response_dry_run_enabled")
        ),
        "tr_response_cutover_enabled": bool(
            payload.get("tr_response_cutover_enabled")
        ),
        "tr_response_worker_side_effect_ready": bool(
            payload.get("tr_response_worker_side_effect_ready")
        ),
        "condition_event_dry_run_enabled": bool(
            payload.get("condition_event_dry_run_enabled")
        ),
        "condition_event_cutover_enabled": bool(
            payload.get("condition_event_cutover_enabled")
        ),
        "condition_event_worker_side_effect_ready": bool(
            payload.get("condition_event_worker_side_effect_ready")
        ),
        "condition_event_fusion_enabled": bool(
            payload.get("condition_event_fusion_enabled")
        ),
        "condition_event_backlog_ready": bool(
            payload.get("condition_event_backlog_ready")
        ),
        "cutover_global_enabled": bool(payload.get("cutover_enabled")),
        "cutover_scope": payload.get("cutover_scope") or "price_tick_only",
        "worker_apply_enabled": bool(payload.get("worker_apply_enabled")),
        "latest_reconcile_status": (
            latest_reconcile_run.get("status")
            if isinstance(latest_reconcile_run, Mapping)
            else None
        ),
        "append_only_ready": bool(payload.get("append_only_ready")),
        "skip_budget_limit_per_minute": int(
            payload.get("skip_budget_limit_per_minute") or 0
        ),
        "skip_budget_used_current_minute": int(
            payload.get("skip_budget_used_current_minute") or 0
        ),
        "skip_budget_remaining": int(
            payload.get("skip_budget_remaining_current_minute") or 0
        ),
        "tr_response_skip_budget_limit_per_minute": int(
            payload.get("tr_response_skip_budget_limit_per_minute") or 0
        ),
        "tr_response_skip_budget_used_current_minute": int(
            payload.get("tr_response_skip_budget_used_current_minute") or 0
        ),
        "tr_response_skip_budget_remaining": int(
            payload.get("tr_response_skip_budget_remaining_current_minute") or 0
        ),
        "condition_event_skip_budget_limit_per_minute": int(
            payload.get("condition_event_skip_budget_limit_per_minute") or 0
        ),
        "condition_event_skip_budget_used_current_minute": int(
            payload.get("condition_event_skip_budget_used_current_minute") or 0
        ),
        "condition_event_skip_budget_remaining": int(
            payload.get("condition_event_skip_budget_remaining_current_minute") or 0
        ),
        "would_skip_inline_count": int(payload.get("would_skip_inline_count") or 0),
        "effective_skip_inline_count": effective_skip_count,
        "effective_price_tick_skip_count": int(
            payload.get("effective_price_tick_skip_count") or 0
        ),
        "condition_event_effective_skip_count": int(
            payload.get("condition_event_effective_skip_count") or 0
        ),
        "condition_event_would_skip_inline_count": int(
            payload.get("condition_event_would_skip_inline_count") or 0
        ),
        "condition_event_deferred_fusion_refresh_count": int(
            payload.get("condition_event_deferred_fusion_refresh_count") or 0
        ),
        "condition_event_deferred_fusion_refresh_error_count": int(
            payload.get("condition_event_deferred_fusion_refresh_error_count") or 0
        ),
        "condition_event_candidate_ingest_status": "NOT_IN_WORKER",
        "condition_event_candidate_ingest_executed_count": int(
            payload.get("condition_event_candidate_ingest_executed_count") or 0
        ),
        "condition_event_pending_worker_count": int(
            payload.get("condition_event_pending_worker_count") or 0
        ),
        "condition_event_worker_applied_count": int(
            payload.get("condition_event_worker_applied_count") or 0
        ),
        "condition_event_artifact_missing_after_worker_count": int(
            payload.get("condition_event_artifact_missing_after_worker_count") or 0
        ),
        "condition_event_side_effect_duplicate_count": int(
            payload.get("condition_event_side_effect_duplicate_count") or 0
        ),
        "tr_response_effective_skip_count": int(
            payload.get("tr_response_effective_skip_count") or 0
        ),
        "tr_response_would_skip_inline_count": int(
            payload.get("tr_response_would_skip_inline_count") or 0
        ),
        "tr_response_deferred_enqueue_count": int(
            payload.get("tr_response_deferred_side_effect_count") or 0
        ),
        "tr_response_deferred_enqueue_error_count": int(
            payload.get("tr_response_deferred_side_effect_error_count") or 0
        ),
        "tr_response_pending_worker_count": int(
            payload.get("tr_response_pending_worker_count") or 0
        ),
        "tr_response_worker_applied_count": int(
            payload.get("tr_response_worker_applied_count") or 0
        ),
        "tr_response_deferred_quote_refresh_count": int(
            payload.get("tr_response_deferred_quote_refresh_count") or 0
        ),
        "tr_response_deferred_quote_refresh_error_count": int(
            payload.get("tr_response_deferred_quote_refresh_error_count") or 0
        ),
        "synthetic_child_guard_status": payload.get("synthetic_child_guard_status"),
        "condition_event_inline_status": (
            "WORKER_DEFERRED_ON_EFFECTIVE_SKIP"
            if bool(payload.get("condition_event_cutover_enabled"))
            else "INLINE_DEFAULT"
        ),
        "invalid_effective_skip_count": int(
            payload.get("invalid_effective_skip_count") or 0
        ),
        "deferred_incremental_enqueue_count": int(
            payload.get("deferred_incremental_enqueue_count") or 0
        ),
        "blocked_count": int(payload.get("blocked_count") or 0),
        "blocked_reason_code_counts": dict(
            payload.get("blocked_reason_code_counts") or {}
        ),
        "fail_closed_reason_counts": dict(payload.get("fail_closed_reason_counts") or {}),
        "latest_decision": latest_decision if isinstance(latest_decision, Mapping) else None,
        "rollback_hint": rollback_hint,
        "failures": list(payload.get("failures") or []),
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
    }


def _projection_outbox_backlog_recommended_action(
    backlog: Mapping[str, Any],
) -> str:
    if bool(backlog.get("pr11_condition_event_cutover_ready")):
        return "READY_FOR_PR11"
    if int(backlog.get("bulk_retire_eligible_count") or 0) > 0:
        return "RUN_BULK_RETIRE_DRY_RUN"
    if int(backlog.get("blocking_pending_count") or 0) > 0:
        return "DRAIN_REQUIRED"
    return "RUN_BULK_RETIRE_APPLY_AFTER_GATEWAY_STOP"


def _market_data_append_only_controller_summary(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    warnings: list[str] = []
    operating_mode = str(payload.get("operating_mode") or "OFF")
    global_kill_switch = bool(payload.get("global_kill_switch"))
    auto_rollback_required = bool(payload.get("auto_rollback_required"))
    if operating_mode == "OFF":
        warnings.append("MarketData append-only controller is OFF")
    if global_kill_switch:
        warnings.append("Global kill switch active")
    if auto_rollback_required:
        warnings.append("Auto rollback required: inline fallback enforced")
    warnings.append("LIVE_REAL/order behavior unchanged")
    for warning in payload.get("warnings") or []:
        if warning not in warnings:
            warnings.append(str(warning))
    return {
        "status": payload.get("status"),
        "operating_mode": operating_mode,
        "global_kill_switch": global_kill_switch,
        "effective_cutover_enabled": bool(payload.get("effective_cutover_enabled")),
        "auto_rollback_required": auto_rollback_required,
        "auto_rollback_reason_codes": list(
            payload.get("auto_rollback_reason_codes") or []
        ),
        "allowed_event_types": list(payload.get("allowed_event_types") or []),
        "global_budget_limit": int(payload.get("global_skip_budget_limit") or 0),
        "global_budget_used": int(payload.get("global_skip_budget_used") or 0),
        "global_budget_remaining": int(
            payload.get("global_skip_budget_remaining") or 0
        ),
        "price_tick_gate": dict(payload.get("price_tick_gate") or {}),
        "tr_response_gate": dict(payload.get("tr_response_gate") or {}),
        "condition_event_gate": dict(payload.get("condition_event_gate") or {}),
        "latest_reconcile_status": payload.get("latest_reconcile_status"),
        "latest_reconcile_append_only_ready": bool(
            payload.get("latest_reconcile_append_only_ready")
        ),
        "backlog_readiness_status": payload.get("backlog_readiness_status"),
        "pr11_condition_event_cutover_ready": bool(
            payload.get("pr11_condition_event_cutover_ready")
        ),
        "projection_outbox_pending_count": int(
            payload.get("projection_outbox_pending_count") or 0
        ),
        "projection_outbox_error_count": int(
            payload.get("projection_outbox_error_count") or 0
        ),
        "projection_outbox_dead_letter_count": int(
            payload.get("projection_outbox_dead_letter_count") or 0
        ),
        "invalid_effective_skip_count": int(
            payload.get("invalid_effective_skip_count") or 0
        ),
        "rollback_hint": payload.get("rollback_hint"),
        "warnings": warnings,
        "read_only": True,
        "no_trading_side_effects": True,
        "order_behavior_changed": bool(payload.get("order_behavior_changed")),
        "live_real_allowed": bool(payload.get("live_real_allowed")),
    }


def _condition_fusion_section(
    rows: list[dict[str, Any]],
    profiles: list[dict[str, Any]],
    *,
    fallback_profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    risk_blocked_codes = [row for row in rows if row.get("risk_blocked")]
    discovery_only_codes = [
        row
        for row in rows
        if set(str(role).upper() for role in row.get("active_roles", ())) == {"DISCOVERY"}
    ]
    top_priority_codes = [
        row
        for row in rows
        if not row.get("risk_blocked") and row not in discovery_only_codes
    ][:10]
    not_buy_signal_count = sum(1 for row in rows if _condition_fusion_not_buy_signal(row))
    profile_rows = profiles or fallback_profiles
    return {
        "status": {
            "profile_count": len(profile_rows),
            "fused_code_count": len(rows),
            "risk_blocked_count": len(risk_blocked_codes),
            "discovery_only_count": len(discovery_only_codes),
            "subscribed_count": sum(1 for row in rows if row.get("subscribed")),
            "top_priority_count": len(top_priority_codes),
            "not_buy_signal_count": not_buy_signal_count,
            "read_only": True,
            "not_buy_signal": not_buy_signal_count > 0,
            "notice": "조건식 hit는 센서 증거이며 role별 admission에서 판단",
        },
        "summary": {
            "label": "조건검색 센서",
            "top_priority_label": "우선 관찰",
            "risk_blocked_label": "위험 차단",
            "discovery_only_label": "넓은 후보",
            "notice": "조건식 hit는 센서 증거이며 role별 admission에서 판단",
        },
        "profiles": profile_rows,
        "codes": rows,
        "top_priority_codes": top_priority_codes,
        "risk_blocked_codes": risk_blocked_codes,
        "discovery_only_codes": discovery_only_codes,
    }


def _condition_fusion_not_buy_signal(row: Mapping[str, Any]) -> bool:
    metadata = row.get("metadata")
    return bool(metadata.get("not_buy_signal")) if isinstance(metadata, Mapping) else False


def _pipeline_stage_statuses(
    *,
    gateway_status: dict[str, Any],
    market_data_status: dict[str, Any],
    realtime_subscription: dict[str, Any],
    condition_fusion_status: dict[str, Any],
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
        _condition_fusion_stage_status(condition_fusion_status),
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


def _condition_fusion_stage_status(status: dict[str, Any]) -> dict[str, Any]:
    fused_count = int(status.get("fused_code_count") or 0)
    profile_count = int(status.get("profile_count") or 0)
    risk_count = int(status.get("risk_blocked_count") or 0)
    reason_codes: list[str] = []
    stage_status = "PASS"
    if fused_count <= 0:
        stage_status = "WARN"
        reason_codes.append(
            "CONDITION_PROFILE_EMPTY" if profile_count <= 0 else "CONDITION_FUSION_EMPTY"
        )
    if risk_count:
        reason_codes.append("CONDITION_RISK_BLOCK_PRESENT")
    return _stage_status(
        "ConditionFusion",
        stage_status,
        f"profiles={profile_count}, fused={fused_count}, risk={risk_count}",
        count=fused_count,
        updated_at=None,
        reason_codes=reason_codes,
    )


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
    heartbeat_at = gateway_status.get("last_heartbeat_at")
    if not heartbeat_at:
        return _stage_status(
            "Gateway",
            "BLOCK",
            "Gateway heartbeat is missing.",
            count=gateway_status.get("recent_event_count"),
            updated_at=None,
            reason_codes=["GATEWAY_HEARTBEAT_MISSING"],
        )
    heartbeat_age_sec = _age_seconds(heartbeat_at)
    if heartbeat_age_sec is None or heartbeat_age_sec > GATEWAY_HEARTBEAT_STALE_SEC:
        reason_codes = ["GATEWAY_HEARTBEAT_STALE"]
        if heartbeat_age_sec is None:
            reason_codes.append("GATEWAY_HEARTBEAT_INVALID")
        return _stage_status(
            "Gateway",
            "WARN",
            "Gateway heartbeat is stale.",
            count=gateway_status.get("recent_event_count"),
            updated_at=heartbeat_at,
            reason_codes=reason_codes,
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
    def with_market_regime(item: dict[str, Any]) -> dict[str, Any]:
        candidate_id = item.get("candidate_instance_id")
        if not candidate_id:
            return item
        regime = _candidate_market_regime_summary(connection, str(candidate_id))
        if regime:
            item = dict(item)
            item["market_regime"] = regime
        return item

    if not include_detail:
        return [with_market_regime(dict(candidate)) for candidate in candidates]
    detailed = []
    for candidate in candidates:
        item = get_candidate(
            connection,
            candidate["candidate_instance_id"],
            include_context=True,
            include_sources=True,
            include_transitions=True,
        )
        detailed.append(with_market_regime(item or candidate))
    return detailed


def _candidate_market_regime_summary(
    connection: sqlite3.Connection,
    candidate_instance_id: str,
) -> dict[str, Any] | None:
    row = connection.execute(
        """
        SELECT market_context_json
        FROM candidate_context_latest
        WHERE candidate_instance_id = ?
        """,
        (candidate_instance_id,),
    ).fetchone()
    if row is None:
        return None
    try:
        market_context = json.loads(row["market_context_json"])
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(market_context, dict):
        return None
    regime = market_context.get("market_regime")
    if not isinstance(regime, dict):
        return None
    return {
        "primary_index_code": regime.get("primary_index_code"),
        "secondary_index_code": regime.get("secondary_index_code"),
        "primary_return_5m": regime.get("primary_return_5m"),
        "primary_drawdown_15m": regime.get("primary_drawdown_15m"),
        "regime_status": regime.get("regime_status"),
        "quality_status": regime.get("quality_status"),
        "reason_codes": regime.get("reason_codes", []),
    }


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


def _state_counts_from_theme_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {state.value: 0 for state in ThemeState}
    for row in rows:
        state = str(row.get("state") or "UNKNOWN")
        counts[state] = counts.get(state, 0) + 1
    return counts


def _theme_dashboard_warnings(
    *,
    full_state_counts: dict[str, int],
    latest_sample_state_counts: dict[str, int],
    top_tradable_themes: list[dict[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    full_tradable_count = int(full_state_counts.get("LEADING") or 0) + int(
        full_state_counts.get("SPREADING") or 0
    )
    if full_tradable_count > 0 and not top_tradable_themes:
        warnings.append("DASHBOARD_TOP_THEME_QUERY_MISMATCH")

    sample_nonzero_states = {
        state for state, count in latest_sample_state_counts.items() if int(count or 0) > 0
    }
    if full_tradable_count > 0 and sample_nonzero_states == {"DATA_WAIT"}:
        warnings.append("DASHBOARD_SAMPLE_LIMIT_HIDES_TRADABLE_THEME")
    return warnings


def _with_theme_snapshot_freshness(
    rows: list[dict[str, Any]],
    *,
    settings: Settings,
) -> list[dict[str, Any]]:
    resolved = []
    for row in rows:
        item = dict(row)
        age_sec = _age_seconds(item.get("calculated_at"))
        item["age_sec"] = age_sec
        item["stale"] = age_sec is None or age_sec > settings.theme_snapshot_stale_sec
        item["stale_sec"] = settings.theme_snapshot_stale_sec
        resolved.append(item)
    return resolved


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


def _age_seconds(value: object) -> float | None:
    try:
        return max((utc_now() - parse_timestamp(value, "timestamp")).total_seconds(), 0.0)
    except Exception:
        return None
