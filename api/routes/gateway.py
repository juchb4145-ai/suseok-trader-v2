from __future__ import annotations

import json
import logging
import threading
from typing import Any

from domain.broker.events import GatewayEvent
from domain.broker.utils import BrokerValidationError
from fastapi import APIRouter, Depends, HTTPException, Query, status
from services.config import load_settings
from services.market_context_service import (
    rebuild_market_context_snapshots,
    should_rebuild_market_context_snapshots,
)
from services.market_data_service import MARKET_DATA_EVENT_TYPES, process_gateway_event
from services.market_index_service import (
    is_market_index_projection_event,
    process_market_index_event,
)
from services.market_index_tr_bootstrap import is_market_index_tr_bootstrap_event
from services.market_reference_service import (
    MARKET_SYMBOL_EVENT_TYPES,
    process_market_symbols_event,
)
from services.market_scan_service import SCAN_EVENT_TYPES, process_market_scan_event
from services.runtime.gateway_live_sim_lifecycle_routing import (
    route_live_sim_lifecycle_gateway_event,
)
from services.runtime.gateway_market_index_routing import (
    MarketIndexAppendOnlyRoutingDecision,
    decide_market_index_append_only_routing,
)
from services.runtime.gateway_market_reference_routing import (
    MarketReferenceAppendOnlyRoutingDecision,
    decide_market_reference_append_only_routing,
)
from services.runtime.gateway_market_regime_routing import (
    MarketRegimeAppendOnlyRoutingDecision,
    decide_market_regime_append_only_routing,
)
from services.runtime.gateway_market_scan_routing import (
    MarketScanAppendOnlyRoutingDecision,
    decide_market_scan_append_only_routing,
)
from services.runtime.gateway_projection_routing import (
    MarketDataAppendOnlyRoutingDecision,
    decide_market_data_projection_routing,
)
from services.runtime.live_sim_lifecycle_consumer import (
    is_live_sim_lifecycle_event,
)
from services.runtime.market_data_projection_side_effects import (
    enqueue_incremental_for_candidate_quote_refresh_tr_response,
    enqueue_incremental_for_price_tick_projection,
    legacy_gateway_candidate_quote_refresh_status,
    refresh_condition_fusion_for_condition_event_projection,
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
from storage.gateway_order_broker_boundary import get_order_broker_boundary
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
    market_reference_routing: dict[str, Any] | None = None
    market_index_routing: dict[str, Any] | None = None
    market_regime_routing: dict[str, Any] | None = None
    market_scan_routing: dict[str, Any] | None = None
    live_sim_lifecycle: dict[str, Any] | None = None
    broker_boundary: dict[str, Any] | None = None
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
                        if event_type == "tr_response":
                            projection_status = (
                                "SKIPPED_INLINE_APPEND_ONLY_TR_RESPONSE"
                            )
                            incremental_status = (
                                "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_TR_RESPONSE"
                            )
                        elif event_type == "condition_event":
                            projection_status = (
                                "SKIPPED_INLINE_APPEND_ONLY_CONDITION_EVENT"
                            )
                            incremental_status = None
                        elif event_type == "price_tick":
                            projection_status = "SKIPPED_INLINE_APPEND_ONLY_PRICE_TICK"
                            incremental_status = "DEFERRED_TO_PROJECTION_OUTBOX_WORKER"
                        else:
                            projection_status = "SKIPPED_INLINE_APPEND_ONLY_MARKET_DATA"
                            incremental_status = "DEFERRED_TO_PROJECTION_OUTBOX_WORKER"
                        projection_statuses["market_data"] = projection_status
                        projection_statuses["market_data_effective_skip_inline"] = "TRUE"
                        if incremental_status is not None:
                            projection_statuses["incremental_evaluation"] = (
                                incremental_status
                            )
                        if event_type == "condition_event":
                            projection_statuses["condition_fusion"] = (
                                "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_CONDITION_EVENT"
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
                    reference_routing = _decide_market_reference_append_only_routing(
                        connection,
                        event,
                        settings=settings,
                        outbox_status=outbox_status,
                    )
                    if reference_routing is None:
                        projection_statuses[
                            "market_reference_append_only_dry_run"
                        ] = "ERROR"
                        projection_statuses[
                            "market_reference_effective_skip_inline"
                        ] = "FALSE"
                    else:
                        projection_statuses[
                            "market_reference_append_only_dry_run"
                        ] = _market_reference_append_only_dry_run_status(
                            reference_routing
                        )
                        market_reference_routing = reference_routing.to_dict()
                        projection_statuses[
                            "market_reference_effective_skip_inline"
                        ] = (
                            "TRUE"
                            if reference_routing.effective_skip_inline
                            else "FALSE"
                        )
                    if (
                        reference_routing is not None
                        and reference_routing.effective_skip_inline
                    ):
                        projection_statuses["market_reference"] = (
                            "SKIPPED_INLINE_APPEND_ONLY_MARKET_REFERENCE"
                        )
                    else:
                        reference_result = process_market_symbols_event(connection, event)
                        projection_statuses["market_reference"] = reference_result.status
                if is_market_index_projection_event(event, settings=settings):
                    index_bootstrap = is_market_index_tr_bootstrap_event(
                        event,
                        settings=settings,
                    )
                    if index_bootstrap:
                        index_routing = None
                        projection_statuses["market_index_append_only_dry_run"] = (
                            "TR_BOOTSTRAP_INLINE_ONLY"
                        )
                        projection_statuses["market_index_effective_skip_inline"] = (
                            "FALSE"
                        )
                    else:
                        index_routing = _decide_market_index_append_only_routing(
                            connection,
                            event,
                            settings=settings,
                            outbox_status=outbox_status,
                        )
                        if index_routing is None:
                            projection_statuses[
                                "market_index_append_only_dry_run"
                            ] = "ERROR"
                            projection_statuses[
                                "market_index_effective_skip_inline"
                            ] = "FALSE"
                        else:
                            market_index_routing = index_routing.to_dict()
                            projection_statuses[
                                "market_index_append_only_dry_run"
                            ] = _market_index_append_only_dry_run_status(
                                index_routing
                            )
                            projection_statuses[
                                "market_index_effective_skip_inline"
                            ] = (
                                "TRUE"
                                if index_routing.effective_skip_inline
                                else "FALSE"
                            )
                    if index_routing is not None and index_routing.effective_skip_inline:
                        projection_statuses["market_index"] = (
                            "SKIPPED_INLINE_APPEND_ONLY_MARKET_INDEX"
                        )
                        projection_statuses["market_regime"] = (
                            "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_MARKET_INDEX"
                        )
                    else:
                        index_result = process_market_index_event(
                            connection,
                            event,
                            settings=settings,
                        )
                        projection_statuses["market_index"] = index_result.status
                        if (
                            index_result.status == "APPLIED"
                            and settings.market_regime_enabled
                        ):
                            if index_bootstrap:
                                regime_routing = None
                                projection_statuses[
                                    "market_regime_append_only_dry_run"
                                ] = "TR_BOOTSTRAP_INLINE_ONLY"
                                projection_statuses[
                                    "market_regime_effective_skip_inline"
                                ] = "FALSE"
                            else:
                                regime_routing = (
                                    _decide_market_regime_append_only_routing(
                                        connection,
                                        event,
                                        settings=settings,
                                        outbox_status=outbox_status,
                                    )
                                )
                                if regime_routing is None:
                                    projection_statuses[
                                        "market_regime_append_only_dry_run"
                                    ] = "ERROR"
                                    projection_statuses[
                                        "market_regime_effective_skip_inline"
                                    ] = "FALSE"
                                else:
                                    market_regime_routing = regime_routing.to_dict()
                                    projection_statuses[
                                        "market_regime_append_only_dry_run"
                                    ] = (
                                        "WOULD_SKIP_INLINE"
                                        if regime_routing.would_skip_inline
                                        else "WOULD_KEEP_INLINE"
                                    )
                                    projection_statuses[
                                        "market_regime_effective_skip_inline"
                                    ] = (
                                        "TRUE"
                                        if regime_routing.effective_skip_inline
                                        else "FALSE"
                                    )
                            if (
                                regime_routing is not None
                                and regime_routing.effective_skip_inline
                            ):
                                projection_statuses["market_regime"] = (
                                    "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_MARKET_REGIME"
                                )
                                projection_statuses["market_context"] = (
                                    "DEFERRED_TO_PROJECTION_OUTBOX_WORKER_MARKET_REGIME"
                                )
                            elif should_rebuild_market_context_snapshots(
                                connection,
                                settings=settings,
                            ):
                                market_context = rebuild_market_context_snapshots(
                                    connection,
                                    settings=settings,
                                    source_event_id=event.event_id,
                                    source_projection="market_index",
                                    generated_by="gateway_inline",
                                )
                                regime = market_context.get("global_regime") or {}
                                projection_statuses["market_regime"] = str(
                                    regime.get("regime_status") or "DATA_WAIT"
                                )
                                projection_statuses["market_context"] = str(
                                    market_context["status"]
                                )
                            else:
                                projection_statuses["market_regime"] = "SKIPPED_RECENT"
                                projection_statuses["market_context"] = "SKIPPED_RECENT"
                if event_type in SCAN_EVENT_TYPES:
                    scan_routing = _decide_market_scan_append_only_routing(
                        connection,
                        event,
                        settings=settings,
                        outbox_status=outbox_status,
                    )
                    if scan_routing is None:
                        projection_statuses["market_scan_append_only_dry_run"] = "ERROR"
                        projection_statuses["market_scan_effective_skip_inline"] = "FALSE"
                    else:
                        market_scan_routing = scan_routing.to_dict()
                        projection_statuses["market_scan_append_only_dry_run"] = (
                            _market_scan_append_only_dry_run_status(scan_routing)
                        )
                        projection_statuses["market_scan_effective_skip_inline"] = (
                            "TRUE" if scan_routing.effective_skip_inline else "FALSE"
                        )
                    if scan_routing is not None and scan_routing.effective_skip_inline:
                        projection_statuses["market_scan"] = (
                            "SKIPPED_INLINE_APPEND_ONLY_MARKET_SCAN"
                        )
                    else:
                        scan_result = process_market_scan_event(
                            connection,
                            event,
                            settings=settings,
                        )
                        if scan_result.status != "IGNORED":
                            projection_statuses["market_scan"] = scan_result.status
            if result.status == "ACCEPTED" and is_live_sim_lifecycle_event(event.event_type):
                live_sim_lifecycle = route_live_sim_lifecycle_gateway_event(
                    connection,
                    event,
                    settings=settings,
                )
            if event.command_id:
                broker_boundary = get_order_broker_boundary(
                    connection,
                    event.command_id,
                )
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
    if event.event_type.strip().lower() == "order_pre_ack":
        response["broker_boundary_state"] = (
            broker_boundary.get("state") if broker_boundary is not None else None
        )
        response["durable_pre_ack_recorded"] = bool(
            broker_boundary
            and broker_boundary.get("durable_pre_ack_recorded") is True
        )
    if projection_status is not None:
        response["projection_status"] = projection_status
    if projection_statuses:
        response["projection_statuses"] = projection_statuses
    if market_data_routing is not None:
        response["market_data_append_only_routing"] = market_data_routing
    if market_reference_routing is not None:
        response["market_reference_append_only_routing"] = market_reference_routing
    if market_index_routing is not None:
        response["market_index_append_only_routing"] = market_index_routing
    if market_regime_routing is not None:
        response["market_regime_append_only_routing"] = market_regime_routing
    if market_scan_routing is not None:
        response["market_scan_append_only_routing"] = market_scan_routing
    if live_sim_lifecycle is not None:
        response["live_sim_lifecycle"] = live_sim_lifecycle
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


def _decide_market_regime_append_only_routing(
    connection,
    event: GatewayEvent,
    *,
    settings,
    outbox_status: str | None,
) -> MarketRegimeAppendOnlyRoutingDecision | None:
    try:
        return decide_market_regime_append_only_routing(
            connection,
            event,
            settings=settings,
            outbox_status=outbox_status,
        )
    except Exception:
        logger.exception("market_regime append-only routing decision failed")
        return None


def _decide_market_reference_append_only_routing(
    connection,
    event: GatewayEvent,
    *,
    settings,
    outbox_status: str | None,
) -> MarketReferenceAppendOnlyRoutingDecision | None:
    try:
        return decide_market_reference_append_only_routing(
            connection,
            event,
            settings=settings,
            outbox_status=outbox_status,
        )
    except Exception:
        logger.exception("market_reference append-only routing decision failed")
        return None


def _decide_market_scan_append_only_routing(
    connection,
    event: GatewayEvent,
    *,
    settings,
    outbox_status: str | None,
) -> MarketScanAppendOnlyRoutingDecision | None:
    try:
        return decide_market_scan_append_only_routing(
            connection,
            event,
            settings=settings,
            outbox_status=outbox_status,
        )
    except Exception:
        logger.exception("market_scan append-only dry-run routing decision failed")
        return None


def _decide_market_index_append_only_routing(
    connection,
    event: GatewayEvent,
    *,
    settings,
    outbox_status: str | None,
) -> MarketIndexAppendOnlyRoutingDecision | None:
    try:
        return decide_market_index_append_only_routing(
            connection,
            event,
            settings=settings,
            outbox_status=outbox_status,
        )
    except Exception:
        logger.exception("market_index append-only routing decision failed")
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


def _market_reference_append_only_dry_run_status(
    decision: MarketReferenceAppendOnlyRoutingDecision,
) -> str:
    if decision.effective_skip_inline:
        return "EFFECTIVE_SKIP_INLINE_PR14_LIMITED"
    if decision.would_skip_inline:
        return "WOULD_SKIP_INLINE_WITH_INLINE_FALLBACK"
    if not decision.dry_run_enabled:
        return "DISABLED"
    return "BLOCKED"


def _market_index_append_only_dry_run_status(
    decision: MarketIndexAppendOnlyRoutingDecision,
) -> str:
    if decision.effective_skip_inline:
        return "EFFECTIVE_SKIP_INLINE"
    if decision.would_skip_inline:
        return "WOULD_SKIP_INLINE_WITH_INLINE_FALLBACK"
    if not decision.dry_run_enabled:
        return "DISABLED"
    return "BLOCKED"


def _market_scan_append_only_dry_run_status(
    decision: MarketScanAppendOnlyRoutingDecision,
) -> str:
    if decision.effective_skip_inline:
        return "EFFECTIVE_SKIP_INLINE"
    if decision.would_skip_inline:
        return "WOULD_SKIP_INLINE_WITH_INLINE_FALLBACK"
    if not decision.dry_run_enabled:
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
    result = refresh_condition_fusion_for_condition_event_projection(
        connection,
        event,
        settings=settings,
        source="gateway_inline_condition_event",
    )
    if result.status == "ERROR":
        return "ERROR"
    return result.status


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
        "durable_pre_ack_posted_count": _json_value(
            status_values.get("durable_pre_ack_posted_count")
        ),
        "last_durable_pre_ack_at": status_values.get("last_durable_pre_ack_at"),
        "last_durable_pre_ack_error": status_values.get(
            "last_durable_pre_ack_error"
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
        "market_index_tr_bootstrap_adapter_status": status_values.get(
            "market_index_tr_bootstrap_adapter_status"
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
        "claimed_command_count": command_counts[GatewayCommandStatus.CLAIMED.value],
        "gateway_started_command_count": command_counts[
            GatewayCommandStatus.GATEWAY_STARTED.value
        ],
        "pre_ack_recorded_command_count": command_counts[
            GatewayCommandStatus.PRE_ACK_RECORDED.value
        ],
        "broker_accepted_command_count": command_counts[
            GatewayCommandStatus.BROKER_ACCEPTED.value
        ],
        "chejan_confirmed_command_count": command_counts[
            GatewayCommandStatus.CHEJAN_CONFIRMED.value
        ],
        "unconfirmed_command_count": command_counts[
            GatewayCommandStatus.UNCONFIRMED.value
        ],
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
