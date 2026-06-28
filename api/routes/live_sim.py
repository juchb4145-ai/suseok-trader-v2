from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from domain.live_sim.status import LiveSimIntentStatus, LiveSimOrderStatus
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from services.config import load_settings
from services.live_sim.live_sim_service import (
    create_live_sim_intent,
    evaluate_live_sim_candidates,
    evaluate_live_sim_eligibility,
    get_live_sim_intent,
    get_live_sim_order,
    get_live_sim_status,
    list_live_sim_errors,
    list_live_sim_executions,
    list_live_sim_intents,
    list_live_sim_orders,
    list_live_sim_reconcile_snapshots,
    list_live_sim_rejections,
    queue_live_sim_order_command,
    reconcile_live_sim,
)
from services.live_sim.order_plan_eligibility import (
    evaluate_live_sim_order_plan_eligibility,
)
from services.live_sim.order_plan_intent import (
    create_live_sim_intent_from_order_plan,
    queue_live_sim_order_command_from_order_plan,
)
from services.runtime.live_sim_pilot_pipeline import (
    list_live_sim_pilot_runs,
    run_live_sim_pilot_pipeline_once,
)
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/live-sim")


@router.get("/status")
def live_sim_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_live_sim_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/intents")
def live_sim_intents(
    trade_date: str | None = Query(default=None),
    status: LiveSimIntentStatus | None = None,
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intents = list_live_sim_intents(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"intents": intents}


@router.get("/intents/{live_sim_intent_id}")
def live_sim_intent(live_sim_intent_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intent = get_live_sim_intent(connection, live_sim_intent_id)
    finally:
        connection.close()
    if intent is None:
        raise _not_found("LIVE_SIM intent", live_sim_intent_id)
    return {"intent": intent}


@router.get("/orders")
def live_sim_orders(
    trade_date: str | None = Query(default=None),
    status: LiveSimOrderStatus | None = None,
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        orders = list_live_sim_orders(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"orders": orders}


@router.get("/orders/{live_sim_order_id}")
def live_sim_order(live_sim_order_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        order = get_live_sim_order(connection, live_sim_order_id)
    finally:
        connection.close()
    if order is None:
        raise _not_found("LIVE_SIM order", live_sim_order_id)
    return {"order": order}


@router.get("/executions")
def live_sim_executions(
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        executions = list_live_sim_executions(
            connection,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"executions": executions}


@router.get("/rejections")
def live_sim_rejections(
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rejections = list_live_sim_rejections(
            connection,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"rejections": rejections}


@router.get("/reconcile")
def live_sim_reconcile_snapshots(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshots = list_live_sim_reconcile_snapshots(connection, limit=limit)
    finally:
        connection.close()
    return {"reconcile_snapshots": snapshots}


@router.get("/errors")
def live_sim_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_live_sim_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


@router.get("/order-plan-eligibility")
def live_sim_order_plan_eligibility(order_plan_id: str = Query(...)) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        eligibility = evaluate_live_sim_order_plan_eligibility(
            connection,
            order_plan_id,
            settings=settings,
        )
    finally:
        connection.close()
    return {"eligibility": eligibility.to_dict(), **_live_sim_response_flags()}


@router.get("/pilot/runs")
def live_sim_pilot_runs(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        runs = list_live_sim_pilot_runs(connection, limit=limit)
    finally:
        connection.close()
    return {"runs": runs, **_live_sim_response_flags()}


@router.post("/evaluate", dependencies=[Depends(require_local_token)])
def live_sim_evaluate(
    trade_date: str | None = Query(default=None),
    candidate_instance_id: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        if candidate_instance_id is not None:
            eligibility = evaluate_live_sim_eligibility(
                connection,
                candidate_instance_id,
                settings=settings,
            )
            return {"eligibility": eligibility.to_dict(), **_live_sim_response_flags()}
        result = evaluate_live_sim_candidates(
            connection,
            trade_date=trade_date,
            limit=limit,
            settings=settings,
        )
        return result.to_dict() | _live_sim_response_flags()
    finally:
        connection.close()


@router.post(
    "/intents/from-candidate/{candidate_instance_id}",
    dependencies=[Depends(require_local_token)],
)
def live_sim_intent_from_candidate(candidate_instance_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intent = create_live_sim_intent(
            connection,
            candidate_instance_id,
            settings=settings,
            source="manual_api",
        )
    finally:
        connection.close()
    return {"intent": intent.to_dict(), **_live_sim_response_flags()}


@router.post(
    "/intents/from-order-plan/{order_plan_id}",
    dependencies=[Depends(require_local_token)],
)
def live_sim_intent_from_order_plan(order_plan_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intent = create_live_sim_intent_from_order_plan(
            connection,
            order_plan_id,
            settings=settings,
            source="manual_api_order_plan",
        )
    finally:
        connection.close()
    return {"intent": intent.to_dict(), **_live_sim_response_flags()}


@router.post(
    "/orders/from-intent/{live_sim_intent_id}",
    dependencies=[Depends(require_local_token)],
)
def live_sim_order_from_intent(live_sim_intent_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            order = queue_live_sim_order_command(
                connection,
                live_sim_intent_id,
                settings=settings,
            )
        except ValueError as exc:
            raise _bad_request(str(exc)) from exc
    finally:
        connection.close()
    return {
        "order": order.to_dict(),
        "gateway_command_id": order.gateway_command_id,
        "idempotency_key": order.idempotency_key,
        **_live_sim_response_flags(),
    }


@router.post(
    "/orders/from-order-plan/{order_plan_id}",
    dependencies=[Depends(require_local_token)],
)
def live_sim_order_from_order_plan(order_plan_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            order = queue_live_sim_order_command_from_order_plan(
                connection,
                order_plan_id,
                settings=settings,
                source="manual_api_order_plan",
            )
        except ValueError as exc:
            raise _bad_request(str(exc)) from exc
    finally:
        connection.close()
    return {
        "order": order.to_dict(),
        "gateway_command_id": order.gateway_command_id,
        "idempotency_key": order.idempotency_key,
        **_live_sim_response_flags(),
    }


@router.post("/pilot/run-once", dependencies=[Depends(require_local_token)])
def live_sim_pilot_run_once(
    trade_date: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
    queue_commands: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = run_live_sim_pilot_pipeline_once(
            connection,
            settings=settings,
            trade_date=trade_date,
            limit=limit,
            queue_commands=queue_commands,
        )
    finally:
        connection.close()
    return result.to_dict() | _live_sim_response_flags()


@router.post("/reconcile", dependencies=[Depends(require_local_token)])
def live_sim_reconcile() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = reconcile_live_sim(connection, settings=settings)
    finally:
        connection.close()
    return {"reconcile": snapshot.to_dict(), **_live_sim_response_flags()}


def _live_sim_response_flags() -> dict[str, Any]:
    return {
        "live_sim_only": True,
        "live_real_allowed": False,
        "broker_order_path": "LIVE_SIM_ONLY",
        "real_order_allowed": False,
    }


def _normalize_code_or_422(code: str) -> str:
    try:
        return validate_stock_code(code)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


def _not_found(entity: str, entity_id: str) -> HTTPException:
    return HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"{entity} not found: {entity_id}",
    )


def _bad_request(message: str) -> HTTPException:
    return HTTPException(
        status_code=http_status.HTTP_400_BAD_REQUEST,
        detail=message,
    )
