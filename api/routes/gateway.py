from __future__ import annotations

import json
import logging
import threading
from typing import Any

from domain.broker.conditions import BrokerConditionEvent
from domain.broker.events import GatewayEvent
from domain.broker.utils import BrokerValidationError
from fastapi import APIRouter, Depends, HTTPException, Query, status
from services.condition_fusion import rebuild_condition_fusion_for_code
from services.config import load_settings
from services.live_sim.live_sim_service import handle_live_sim_gateway_event
from services.market_data_service import MARKET_DATA_EVENT_TYPES, process_gateway_event
from services.market_index_service import MARKET_INDEX_EVENT_TYPES, process_market_index_event
from services.market_reference_service import (
    MARKET_SYMBOL_EVENT_TYPES,
    process_market_symbols_event,
)
from services.market_regime_service import (
    rebuild_market_regime_snapshot,
    should_rebuild_market_regime_snapshot,
)
from services.market_scan_service import SCAN_EVENT_TYPES, process_market_scan_event
from services.runtime.gateway_projection_routing import (
    MarketDataAppendOnlyRoutingDecision,
    decide_market_data_projection_routing,
)
from services.runtime.market_data_projection_side_effects import (
    enqueue_incremental_for_candidate_quote_refresh_tr_response,
    enqueue_incremental_for_price_tick_projection,
    legacy_gateway_candidate_quote_refresh_status,
)
from storage.event_store import (
    append_gateway_event,
    count_recent_gateway_events,
    get_gateway_status_values,
    list_recent_gateway_events,
)
from storage.gateway_command_store import (
    FORBIDDEN_ORDER_COMMAND_TYPES,
    GatewayCommandStatus,
    get_command_status_counts,
    get_command_type_counts,
    poll_commands,
)
from storage.projection_outbox import enqueue_projection_jobs_for_gateway_event
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/gateway")
logger = logging.getLogger(__name__)
_gateway_event_write_lock = threading.RLock()


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
    projection_statuses: dict[str, str] = {}
    market_data_routing: dict[str, Any] | None = None
    try:
        with _gateway_event_write_lock:
            result = append_gateway_event(connection, event)
            if result.status == "ACCEPTED" and not result.duplicate:
                event_type = event.event_type.strip().lower()
                outbox_status = _enqueue_projection_outbox_jobs(connection, event)
                if outbox_status is not None:
                    projection_statuses["projection_outbox"] = outbox_status
                if event_type in MARKET_DATA_EVENT_TYPES:
                    routing_decision = _decide_market_data_projection_routing(
                        connection,
                        event,
                        settings=settings,
                        outbox_status=outbox_status,
                    )
                    if routing_decision is None:
                        projection_statuses["market_data_append_only_dry_run"] = "ERROR"
                        projection_statuses["market_data_effective_skip_inline"] = "FALSE"
                    else:
                        projection_statuses["market_data_append_only_dry_run"] = (
                            _market_data_append_only_dry_run_status(routing_decision)
                        )
                        projection_statuses["market_data_effective_skip_inline"] = (
                            "TRUE" if routing_decision.effective_skip_inline else "FALSE"
                        )
                        market_data_routing = routing_decision.to_dict()
                    if routing_decision is not None and routing_decision.effective_skip_inline:
                        projection_status = "SKIPPED_INLINE_APPEND_ONLY_PRICE_TICK"
                        projection_statuses["market_data"] = projection_status
                        projection_statuses["market_data_effective_skip_inline"] = "TRUE"
                        projection_statuses["incremental_evaluation"] = (
                            "DEFERRED_TO_PROJECTION_OUTBOX_WORKER"
                        )
                    else:
                        projection_result = process_gateway_event(
                            connection,
                            event,
                            settings=settings,
                        )
                        projection_status = projection_result.status
                        projection_statuses["market_data"] = projection_result.status
                        if (
                            event_type == "price_tick"
                            and projection_result.status == "APPLIED"
                        ):
                            projection_statuses["incremental_evaluation"] = (
                                _enqueue_incremental_evaluation_for_price_tick(
                                    connection,
                                    event,
                                    settings=settings,
                                )
                            )
                        if (
                            event_type == "tr_response"
                            and projection_result.status == "APPLIED"
                        ):
                            quote_refresh_status = (
                                _enqueue_incremental_evaluation_for_candidate_quote_refresh(
                                    connection,
                                    event,
                                    settings=settings,
                                )
                            )
                            if quote_refresh_status is not None:
                                projection_statuses["incremental_evaluation"] = (
                                    quote_refresh_status
                                )
                        if (
                            event_type == "condition_event"
                            and projection_result.status == "APPLIED"
                            and settings.condition_fusion_event_incremental_enabled
                        ):
                            projection_statuses["condition_fusion"] = (
                                _refresh_condition_fusion_for_condition_event(
                                    connection,
                                    event,
                                    settings=settings,
                                )
                            )
                if event_type in MARKET_SYMBOL_EVENT_TYPES:
                    reference_result = process_market_symbols_event(connection, event)
                    projection_statuses["market_reference"] = reference_result.status
                if event_type in MARKET_INDEX_EVENT_TYPES:
                    index_result = process_market_index_event(
                        connection,
                        event,
                        settings=settings,
                    )
                    projection_statuses["market_index"] = index_result.status
                    if index_result.status == "APPLIED" and settings.market_regime_enabled:
                        if should_rebuild_market_regime_snapshot(connection):
                            regime = rebuild_market_regime_snapshot(
                                connection,
                                settings=settings,
                            )
                            projection_statuses["market_regime"] = regime["regime_status"]
                        else:
                            projection_statuses["market_regime"] = "SKIPPED_RECENT"
                if event_type in SCAN_EVENT_TYPES:
                    scan_result = process_market_scan_event(
                        connection,
                        event,
                        settings=settings,
                    )
                    if scan_result.status != "IGNORED":
                        projection_statuses["market_scan"] = scan_result.status
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
    if projection_statuses:
        response["projection_statuses"] = projection_statuses
    if market_data_routing is not None:
        response["market_data_append_only_routing"] = market_data_routing
    return response


def _enqueue_projection_outbox_jobs(connection, event: GatewayEvent) -> str | None:
    try:
        result = enqueue_projection_jobs_for_gateway_event(connection, event)
    except Exception:
        logger.exception("projection outbox enqueue failed")
        return "ERROR"
    if result.job_count <= 0:
        return None
    return result.status


def _decide_market_data_projection_routing(
    connection,
    event: GatewayEvent,
    *,
    settings,
    outbox_status: str | None,
) -> MarketDataAppendOnlyRoutingDecision | None:
    try:
        return decide_market_data_projection_routing(
            connection,
            event,
            settings=settings,
            outbox_status=outbox_status,
        )
    except Exception:
        logger.exception("market_data append-only dry-run routing decision failed")
        return None


def _market_data_append_only_dry_run_status(
    decision: MarketDataAppendOnlyRoutingDecision,
) -> str:
    reasons = set(decision.blocked_reason_codes)
    if decision.would_skip_inline:
        return "WOULD_SKIP_INLINE"
    if "DRY_RUN_DISABLED" in reasons:
        return "DISABLED"
    return "BLOCKED"


def _enqueue_incremental_evaluation_for_price_tick(
    connection,
    event: GatewayEvent,
    *,
    settings,
) -> str:
    result = enqueue_incremental_for_price_tick_projection(
        connection,
        event,
        settings=settings,
        source="gateway_inline_price_tick",
    )
    return result.status


def _enqueue_incremental_evaluation_for_candidate_quote_refresh(
    connection,
    event: GatewayEvent,
    *,
    settings,
) -> str | None:
    result = enqueue_incremental_for_candidate_quote_refresh_tr_response(
        connection,
        event,
        settings=settings,
        source="gateway_inline_candidate_quote_refresh",
    )
    return legacy_gateway_candidate_quote_refresh_status(result)


def _refresh_condition_fusion_for_condition_event(
    connection,
    event: GatewayEvent,
    *,
    settings,
) -> str:
    try:
        condition = BrokerConditionEvent.from_dict(event.payload)
        result = rebuild_condition_fusion_for_code(
            connection,
            condition.code,
            settings=settings,
        )
    except Exception:
        logger.exception("condition fusion incremental refresh failed")
        return "ERROR"
    return "APPLIED" if result.fused_code_count else "IGNORED_NO_PROFILE"


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
        "comm_connect_state": status_values.get("comm_connect_state"),
        "latest_comm_connect_call_at": status_values.get("latest_comm_connect_call_at"),
        "latest_comm_connect_result_at": status_values.get("latest_comm_connect_result_at"),
        "latest_comm_connect_result_code": _json_value(
            status_values.get("latest_comm_connect_result_code")
        ),
        "latest_on_event_connect_timeout_at": status_values.get(
            "latest_on_event_connect_timeout_at"
        ),
        "login_block_reason_codes": _json_value(
            status_values.get("login_block_reason_codes")
        ),
        "core_io_enabled": _json_value(status_values.get("core_io_enabled")),
        "command_polling_enabled": _json_value(
            status_values.get("command_polling_enabled")
        ),
        "event_posting_enabled": _json_value(status_values.get("event_posting_enabled")),
        "core_io_worker_enabled": _json_value(status_values.get("core_io_worker_enabled")),
        "core_io_worker_running": _json_value(status_values.get("core_io_worker_running")),
        "core_io_worker_thread_id": _json_value(
            status_values.get("core_io_worker_thread_id")
        ),
        "core_io_worker_event_queue_size": _json_value(
            status_values.get("core_io_worker_event_queue_size")
        ),
        "core_io_worker_command_queue_size": _json_value(
            status_values.get("core_io_worker_command_queue_size")
        ),
        "core_io_worker_last_error": status_values.get("core_io_worker_last_error"),
        "core_io_worker_coalesced_event_count": _json_value(
            status_values.get("core_io_worker_coalesced_event_count")
        ),
        "core_io_worker_coalesce_after_size": _json_value(
            status_values.get("core_io_worker_coalesce_after_size")
        ),
        "local_event_count": _json_value(status_values.get("local_event_count")),
        "condition_load_state": status_values.get("condition_load_state"),
        "condition_load_requested_at": status_values.get("condition_load_requested_at"),
        "condition_load_retry_count": _json_value(
            status_values.get("condition_load_retry_count")
        ),
        "condition_load_timeout_count": _json_value(
            status_values.get("condition_load_timeout_count")
        ),
        "condition_callback_health": status_values.get("condition_callback_health"),
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
        "market_index_enabled": _json_value(status_values.get("market_index_enabled")),
        "market_index_realtime_enabled": _json_value(
            status_values.get("market_index_realtime_enabled")
        ),
        "market_index_tr_bootstrap_enabled": _json_value(
            status_values.get("market_index_tr_bootstrap_enabled")
        ),
        "market_index_codes": _json_value(status_values.get("market_index_codes")),
        "market_index_screen_no": status_values.get("market_index_screen_no"),
        "market_index_poll_sec": _json_value(status_values.get("market_index_poll_sec")),
        "market_index_registered_codes": _json_value(
            status_values.get("market_index_registered_codes")
        ),
        "market_index_callback_count": _json_value(
            status_values.get("market_index_callback_count")
        ),
        "parsed_market_index_tick_count": _json_value(
            status_values.get("parsed_market_index_tick_count")
        ),
        "market_index_parse_error_count": _json_value(
            status_values.get("market_index_parse_error_count")
        ),
        "latest_market_index_tick_at": status_values.get("latest_market_index_tick_at"),
        "latest_market_index_parse_error": _json_value(
            status_values.get("latest_market_index_parse_error")
        ),
        "latest_market_index_registration_result": _json_value(
            status_values.get("latest_market_index_registration_result")
        ),
        "latest_market_index_registration_at": status_values.get(
            "latest_market_index_registration_at"
        ),
        "market_index_adapter_health": status_values.get("market_index_adapter_health"),
        "market_index_recover_count": _json_value(
            status_values.get("market_index_recover_count")
        ),
        "latest_market_index_recover_at": status_values.get(
            "latest_market_index_recover_at"
        ),
        "market_index_recover_error": status_values.get("market_index_recover_error"),
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
