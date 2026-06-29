from __future__ import annotations

import json
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import BrokerValidationError
from fastapi import APIRouter, Depends, HTTPException, Query, status
from services.config import load_settings
from services.live_sim.live_sim_service import handle_live_sim_gateway_event
from services.market_data_service import MARKET_DATA_EVENT_TYPES, process_gateway_event
from storage.event_store import (
    append_gateway_event,
    count_recent_gateway_events,
    get_gateway_status_values,
    list_recent_gateway_events,
)
from storage.gateway_command_store import (
    FORBIDDEN_ORDER_COMMAND_TYPES,
    GatewayCommandStatus,
    get_command_type_counts,
    get_command_status_counts,
    poll_commands,
)
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/gateway")


@router.post("/events", dependencies=[Depends(require_local_token)])
def post_gateway_event(body: dict[str, Any]) -> dict[str, Any]:
    try:
        event = GatewayEvent.from_dict(body)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    projection_status: str | None = None
    try:
        result = append_gateway_event(connection, event)
        if (
            result.status == "ACCEPTED"
            and not result.duplicate
            and event.event_type.strip().lower() in MARKET_DATA_EVENT_TYPES
        ):
            projection_result = process_gateway_event(connection, event, settings=settings)
            projection_status = projection_result.status
        if result.status == "ACCEPTED" and not result.duplicate:
            handle_live_sim_gateway_event(connection, event, settings=settings)
    finally:
        connection.close()

    if result.status == "CONFLICT":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=result.error_message,
        )
    if result.status == "REJECTED":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=result.error_message,
        )

    response = {
        "accepted": result.accepted,
        "event_id": result.event_id,
        "duplicate": result.duplicate,
        "status": result.status,
    }
    if projection_status is not None:
        response["projection_status"] = projection_status
    return response


@router.get("/commands", dependencies=[Depends(require_local_token)])
def get_gateway_commands(
    limit: int = Query(default=20, ge=1, le=100),
    wait_sec: float = Query(default=0, ge=0, le=5),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        commands = poll_commands(connection, limit=limit, wait_sec=wait_sec)
    finally:
        connection.close()

    return {"commands": [command.to_dict() for command in commands]}


@router.get("/status")
def get_gateway_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        status_values = get_gateway_status_values(connection)
        command_counts = get_command_status_counts(connection)
        recent_event_count = count_recent_gateway_events(connection)
    finally:
        connection.close()

    return {
        "last_event_received_at": status_values.get("last_event_received_at"),
        "last_heartbeat_at": status_values.get("last_heartbeat_at"),
        "kiwoom_logged_in": _json_value(status_values.get("kiwoom_logged_in")),
        "login_threaded": _json_value(status_values.get("login_threaded")),
        "condition_load_state": status_values.get("condition_load_state"),
        "condition_load_requested_at": status_values.get("condition_load_requested_at"),
        "condition_load_retry_count": _json_value(
            status_values.get("condition_load_retry_count")
        ),
        "condition_load_timeout_count": _json_value(
            status_values.get("condition_load_timeout_count")
        ),
        "latest_condition_ver_callback_at": status_values.get(
            "latest_condition_ver_callback_at"
        ),
        "latest_condition_ver_result": _json_value(
            status_values.get("latest_condition_ver_result")
        ),
        "server_mode": status_values.get("server_mode"),
        "registered_realtime_code_count": _json_value(
            status_values.get("registered_realtime_code_count")
        ),
        "realtime_registered_codes": _json_value(
            status_values.get("realtime_registered_codes")
        ),
        "realtime_exchange": status_values.get("realtime_exchange"),
        "realtime_registered_kiwoom_codes": _json_value(
            status_values.get("realtime_registered_kiwoom_codes")
        ),
        "realtime_registration_requested_count": _json_value(
            status_values.get("realtime_registration_requested_count")
        ),
        "realtime_registration_success_count": _json_value(
            status_values.get("realtime_registration_success_count")
        ),
        "latest_realtime_registration_at": status_values.get(
            "latest_realtime_registration_at"
        ),
        "latest_realtime_registration_result": _json_value(
            status_values.get("latest_realtime_registration_result")
        ),
        "realtime_callback_count": _json_value(status_values.get("realtime_callback_count")),
        "raw_realtime_callback_count": _json_value(
            status_values.get("raw_realtime_callback_count")
        ),
        "latest_realtime_callback_at": status_values.get("latest_realtime_callback_at"),
        "parsed_price_tick_count": _json_value(status_values.get("parsed_price_tick_count")),
        "realtime_parse_error_count": _json_value(
            status_values.get("realtime_parse_error_count")
        ),
        "latest_realtime_parse_error": _json_value(
            status_values.get("latest_realtime_parse_error")
        ),
        "realtime_subscription_health": status_values.get("realtime_subscription_health"),
        "raw_callback_counts": _json_value(status_values.get("raw_callback_counts")),
        "latest_callback_at_by_method": _json_value(
            status_values.get("latest_callback_at_by_method")
        ),
        "latest_active_x_thread_audit": _json_value(
            status_values.get("latest_active_x_thread_audit")
        ),
        "queued_command_count": command_counts[GatewayCommandStatus.QUEUED.value],
        "dispatched_command_count": command_counts[GatewayCommandStatus.DISPATCHED.value],
        "acked_command_count": command_counts[GatewayCommandStatus.ACKED.value],
        "failed_command_count": command_counts[GatewayCommandStatus.FAILED.value],
        "recent_event_count": recent_event_count,
        "token_required": bool(settings.trading_core_token),
        "order_commands_allowed": False,
    }


def _json_value(value: str | None) -> Any:
    if value is None or value == "":
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


@router.get("/events/recent")
def get_recent_gateway_events(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        events = list_recent_gateway_events(connection, limit=limit)
    finally:
        connection.close()

    return {"events": events}


@router.get("/commands/status")
def get_gateway_command_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        counts = get_command_status_counts(connection)
        command_type_counts = get_command_type_counts(connection)
    finally:
        connection.close()

    order_command_count = sum(
        count
        for command_type, count in command_type_counts.items()
        if command_type in FORBIDDEN_ORDER_COMMAND_TYPES
    )
    return {
        "counts": counts,
        "command_type_counts": command_type_counts,
        "order_command_count": order_command_count,
        "read_only": True,
        "order_commands_allowed": False,
    }


@router.get("/auth/probe", dependencies=[Depends(require_local_token)])
def gateway_auth_probe() -> dict[str, Any]:
    return {
        "authenticated": True,
        "read_only": True,
        "no_order_side_effects": True,
    }
