from __future__ import annotations

import sqlite3
from typing import Any

from domain.broker.utils import datetime_to_wire, utc_now
from storage.event_store import get_gateway_status_values
from storage.live_sim_order_plan_uniqueness import (
    get_live_sim_order_plan_uniqueness_status,
)

from services.ai_advisory.storage import build_status as build_ai_advisory_status
from services.candidate_service import get_candidate_status
from services.config import Settings, load_settings
from services.dashboard_service import build_safety_section
from services.entry_timing.service import get_entry_timing_status
from services.live_sim.live_sim_service import (
    get_latest_live_sim_reconcile,
    get_live_sim_status,
    list_live_sim_errors,
    list_live_sim_lifecycle_events,
)
from services.market_data_service import get_market_data_status
from services.operator.no_buy_sentinel import (
    build_no_buy_sentinel_snapshot,
    get_latest_no_buy_sentinel_snapshot,
)
from services.realtime_subscription import build_realtime_subscription_plan
from services.runtime.evaluation_run_guard import get_runtime_execution_lock_status
from services.runtime.live_sim_pilot_pipeline import list_live_sim_pilot_runs
from services.theme_diagnostics import build_theme_data_wait_diagnostics
from services.theme_service import get_theme_status


def build_operator_status(
    connection: sqlite3.Connection,
    *,
    settings: Settings | None = None,
    trade_date: str | None = None,
    include_no_buy_rebuild: bool = False,
) -> dict[str, Any]:
    resolved_settings = settings or load_settings()
    generated_at = datetime_to_wire(utc_now())
    gateway_values = get_gateway_status_values(connection)
    live_sim_status = get_live_sim_status(connection, resolved_settings)
    latest_reconcile = get_latest_live_sim_reconcile(connection)
    no_buy_latest = get_latest_no_buy_sentinel_snapshot(connection, trade_date=trade_date)
    if no_buy_latest is None and include_no_buy_rebuild:
        no_buy_latest = build_no_buy_sentinel_snapshot(
            connection,
            settings=resolved_settings,
            trade_date=trade_date,
            manual=True,
            write_snapshot=False,
        ).to_dict()

    realtime_plan = build_realtime_subscription_plan(
        connection,
        settings=resolved_settings,
        trade_date=trade_date,
        queue_commands=False,
    ).to_dict()
    theme_diagnostics = build_theme_data_wait_diagnostics(
        connection,
        settings=resolved_settings,
        limit=20,
    )

    return {
        "generated_at": generated_at,
        "read_only": True,
        "no_order_side_effects": True,
        "execution_controls_available": False,
        "core": {
            "api_health": "ok",
            "trading_profile": resolved_settings.trading_profile.value,
            "trading_mode": resolved_settings.trading_mode.value,
            "token_required": bool(resolved_settings.trading_core_token),
            "database_path": str(resolved_settings.trading_db_path),
        },
        "safety": build_safety_section(resolved_settings),
        "gateway": {
            "last_event_received_at": gateway_values.get("last_event_received_at"),
            "last_heartbeat_at": gateway_values.get("last_heartbeat_at"),
            "gateway_orderable": gateway_values.get("gateway_orderable"),
            "command_queue_healthy": gateway_values.get("command_queue_healthy"),
            "account_mode": gateway_values.get("account_mode"),
            "broker_env": gateway_values.get("broker_env"),
            "server_mode": gateway_values.get("server_mode"),
        },
        "runtime_execution_locks": get_runtime_execution_lock_status(connection),
        "live_sim_order_plan_uniqueness": (
            get_live_sim_order_plan_uniqueness_status(connection)
        ),
        "live_sim": {
            "status": live_sim_status,
            "kill_switch": live_sim_status.get("kill_switch"),
            "account_mode": live_sim_status.get("account_mode"),
            "broker_env": live_sim_status.get("broker_env"),
            "server_mode": live_sim_status.get("server_mode"),
            "pilot": {
                "enabled": resolved_settings.live_sim_pilot_pipeline_enabled,
                "auto_queue_command": resolved_settings.live_sim_pilot_auto_queue_command,
                "order_plan_routing_enabled": (
                    resolved_settings.live_sim_order_plan_routing_enabled
                ),
                "latest_runs": list_live_sim_pilot_runs(connection, limit=5),
            },
            "execution_lifecycle": {
                "recent_events": list_live_sim_lifecycle_events(connection, limit=5),
                "recent_errors": list_live_sim_errors(connection, limit=5),
            },
            "reconcile_latest": latest_reconcile,
        },
        "market_data": get_market_data_status(connection, settings=resolved_settings),
        "theme_leadership": get_theme_status(connection, settings=resolved_settings),
        "theme_data_wait_diagnostics": {
            "state_quality_distribution": theme_diagnostics["state_quality_distribution"],
            "data_wait_reason_counts": theme_diagnostics["data_wait_reason_counts"],
            "root_cause_summary": theme_diagnostics["root_cause_summary"],
            "subscription_capacity": theme_diagnostics["subscription_capacity"],
            "top_data_wait_themes": theme_diagnostics["top_data_wait_themes"],
        },
        "realtime_subscription_warmup": {
            "status": realtime_plan.get("status"),
            "counts": realtime_plan.get("counts", {}),
            "pending_register_count": int(
                (realtime_plan.get("counts") or {}).get("planned_register_count") or 0
            ),
            "registered_count": int(
                (realtime_plan.get("counts") or {}).get("registered_count") or 0
            ),
            "missing_subscription_count": int(
                (realtime_plan.get("counts") or {}).get(
                    "missing_candidate_subscription_count",
                    0,
                )
                or 0
            ),
            "queue_commands": False,
            "read_only": True,
            "observe_only": True,
        },
        "candidates": get_candidate_status(connection, settings=resolved_settings),
        "entry_timing": get_entry_timing_status(connection, settings=resolved_settings),
        "ai_advisory": build_ai_advisory_status(connection, settings=resolved_settings),
        "no_buy_sentinel": no_buy_latest,
    }
