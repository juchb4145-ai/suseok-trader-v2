from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from domain.oms.status import DryRunIntentStatus, DryRunOrderStatus
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from services.config import load_settings
from services.oms.dry_run_service import (
    convert_intent_to_dry_run_order,
    create_dry_run_intent,
    evaluate_dry_run_candidates,
    evaluate_dry_run_eligibility,
    get_dry_run_intent,
    get_dry_run_order,
    get_dry_run_status,
    list_dry_run_eligibility_checks,
    list_dry_run_errors,
    list_dry_run_executions,
    list_dry_run_intents,
    list_dry_run_ledger,
    list_dry_run_orders,
    list_dry_run_positions,
    simulate_fill_dry_run_order,
    update_dry_run_positions_mark_to_market,
)
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/dry-run")


@router.get("/status")
def dry_run_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_dry_run_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/eligibility")
def dry_run_eligibility_checks(
    trade_date: str | None = Query(default=None),
    code: str | None = Query(default=None),
    eligible: bool | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        checks = list_dry_run_eligibility_checks(
            connection,
            trade_date=trade_date,
            code=normalized_code,
            eligible=eligible,
            limit=limit,
        )
    finally:
        connection.close()
    return {"eligibility_checks": checks}


@router.get("/intents")
def dry_run_intents(
    trade_date: str | None = Query(default=None),
    status: DryRunIntentStatus | None = None,
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intents = list_dry_run_intents(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"intents": intents}


@router.get("/intents/{dry_run_intent_id}")
def dry_run_intent(dry_run_intent_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intent = get_dry_run_intent(connection, dry_run_intent_id)
    finally:
        connection.close()
    if intent is None:
        raise _not_found("dry-run intent", dry_run_intent_id)
    return {"intent": intent}


@router.get("/orders")
def dry_run_orders(
    trade_date: str | None = Query(default=None),
    status: DryRunOrderStatus | None = None,
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        orders = list_dry_run_orders(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"orders": orders}


@router.get("/orders/{dry_run_order_id}")
def dry_run_order(dry_run_order_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        order = get_dry_run_order(connection, dry_run_order_id)
    finally:
        connection.close()
    if order is None:
        raise _not_found("dry-run order", dry_run_order_id)
    return {"order": order}


@router.get("/executions")
def dry_run_executions(
    trade_date: str | None = Query(default=None),
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        executions = list_dry_run_executions(
            connection,
            trade_date=trade_date,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"executions": executions}


@router.get("/positions")
def dry_run_positions(
    trade_date: str | None = Query(default=None),
    status: str | None = Query(default=None),
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        positions = list_dry_run_positions(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"positions": positions}


@router.get("/ledger")
def dry_run_ledger(
    trade_date: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        ledger = list_dry_run_ledger(
            connection,
            trade_date=trade_date,
            event_type=event_type,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"ledger": ledger}


@router.get("/errors")
def dry_run_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_dry_run_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


@router.post("/evaluate", dependencies=[Depends(require_local_token)])
def dry_run_evaluate(
    trade_date: str | None = Query(default=None),
    candidate_instance_id: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        if candidate_instance_id is not None:
            eligibility = evaluate_dry_run_eligibility(
                connection,
                candidate_instance_id,
                settings=settings,
            )
            return {"eligibility": eligibility.to_dict(), **_safe_response_flags()}
        result = evaluate_dry_run_candidates(
            connection,
            trade_date=trade_date,
            limit=limit,
            settings=settings,
        )
        return result.to_dict()
    finally:
        connection.close()


@router.post(
    "/intents/from-candidate/{candidate_instance_id}",
    dependencies=[Depends(require_local_token)],
)
def dry_run_intent_from_candidate(candidate_instance_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intent = create_dry_run_intent(
            connection,
            candidate_instance_id,
            settings=settings,
            source="manual_api",
        )
    finally:
        connection.close()
    return {"intent": intent.to_dict(), **_safe_response_flags()}


@router.post("/orders/from-intent/{dry_run_intent_id}", dependencies=[Depends(require_local_token)])
def dry_run_order_from_intent(dry_run_intent_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            order = convert_intent_to_dry_run_order(
                connection,
                dry_run_intent_id,
                settings=settings,
            )
        except ValueError as exc:
            raise _bad_request(str(exc)) from exc
    finally:
        connection.close()
    return {"order": order.to_dict(), **_safe_response_flags()}


@router.post(
    "/orders/{dry_run_order_id}/simulate-fill",
    dependencies=[Depends(require_local_token)],
)
def dry_run_order_simulate_fill(dry_run_order_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            execution = simulate_fill_dry_run_order(
                connection,
                dry_run_order_id,
                settings=settings,
            )
        except ValueError as exc:
            raise _bad_request(str(exc)) from exc
    finally:
        connection.close()
    return {"execution": execution.to_dict(), **_safe_response_flags()}


@router.post("/positions/mark-to-market", dependencies=[Depends(require_local_token)])
def dry_run_mark_to_market() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = update_dry_run_positions_mark_to_market(connection, settings=settings)
        except ValueError as exc:
            raise _bad_request(str(exc)) from exc
    finally:
        connection.close()
    return result | _safe_response_flags()


def _safe_response_flags() -> dict[str, bool]:
    return {
        "dry_run_only": True,
        "live_order_allowed": False,
        "gateway_command_allowed": False,
        "broker_order_sent": False,
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
