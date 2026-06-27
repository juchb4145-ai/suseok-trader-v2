from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from domain.exit.status import (
    DryRunExitEvaluationStatus,
    DryRunExitIntentStatus,
    DryRunExitOrderStatus,
    DryRunExitSignalType,
)
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from services.config import load_settings
from services.exit_engine import (
    convert_exit_intent_to_dry_run_order,
    create_dry_run_exit_intent,
    evaluate_all_dry_run_exits,
    evaluate_dry_run_exit_for_position,
    get_exit_evaluation,
    get_exit_intent,
    get_exit_order,
    get_exit_status,
    list_exit_errors,
    list_exit_evaluations,
    list_exit_executions,
    list_exit_intents,
    list_exit_orders,
    list_exit_runs,
    list_exit_signals,
    simulate_fill_dry_run_exit_order,
)
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/dry-run/exits")


@router.get("/status")
def dry_run_exit_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_exit_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/evaluations")
def dry_run_exit_evaluations(
    trade_date: str | None = Query(default=None),
    status: DryRunExitEvaluationStatus | None = None,
    code: str | None = Query(default=None),
    dry_run_position_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        evaluations = list_exit_evaluations(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            dry_run_position_id=dry_run_position_id,
            limit=limit,
        )
    finally:
        connection.close()
    return {"evaluations": evaluations, **_safe_response_flags()}


@router.get("/evaluations/{exit_evaluation_id}")
def dry_run_exit_evaluation(exit_evaluation_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        evaluation = get_exit_evaluation(connection, exit_evaluation_id, include_signals=True)
    finally:
        connection.close()
    if evaluation is None:
        raise _not_found("dry-run exit evaluation", exit_evaluation_id)
    return {"evaluation": evaluation, **_safe_response_flags()}


@router.get("/signals")
def dry_run_exit_signals(
    exit_evaluation_id: str | None = Query(default=None),
    dry_run_position_id: str | None = Query(default=None),
    signal_type: DryRunExitSignalType | None = None,
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        signals = list_exit_signals(
            connection,
            exit_evaluation_id=exit_evaluation_id,
            dry_run_position_id=dry_run_position_id,
            signal_type=signal_type,
            status=status,
            limit=limit,
        )
    finally:
        connection.close()
    return {"signals": signals, **_safe_response_flags()}


@router.get("/intents")
def dry_run_exit_intents(
    trade_date: str | None = Query(default=None),
    status: DryRunExitIntentStatus | None = None,
    code: str | None = Query(default=None),
    dry_run_position_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intents = list_exit_intents(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            dry_run_position_id=dry_run_position_id,
            limit=limit,
        )
    finally:
        connection.close()
    return {"intents": intents, **_safe_response_flags()}


@router.get("/intents/{dry_run_exit_intent_id}")
def dry_run_exit_intent(dry_run_exit_intent_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intent = get_exit_intent(connection, dry_run_exit_intent_id)
    finally:
        connection.close()
    if intent is None:
        raise _not_found("dry-run exit intent", dry_run_exit_intent_id)
    return {"intent": intent, **_safe_response_flags()}


@router.get("/orders")
def dry_run_exit_orders(
    trade_date: str | None = Query(default=None),
    status: DryRunExitOrderStatus | None = None,
    code: str | None = Query(default=None),
    dry_run_position_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        orders = list_exit_orders(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            dry_run_position_id=dry_run_position_id,
            limit=limit,
        )
    finally:
        connection.close()
    return {"orders": orders, **_safe_response_flags()}


@router.get("/orders/{dry_run_exit_order_id}")
def dry_run_exit_order(dry_run_exit_order_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        order = get_exit_order(connection, dry_run_exit_order_id)
    finally:
        connection.close()
    if order is None:
        raise _not_found("dry-run exit order", dry_run_exit_order_id)
    return {"order": order, **_safe_response_flags()}


@router.get("/executions")
def dry_run_exit_executions(
    trade_date: str | None = Query(default=None),
    code: str | None = Query(default=None),
    dry_run_position_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        executions = list_exit_executions(
            connection,
            trade_date=trade_date,
            code=normalized_code,
            dry_run_position_id=dry_run_position_id,
            limit=limit,
        )
    finally:
        connection.close()
    return {"executions": executions, **_safe_response_flags()}


@router.get("/runs")
def dry_run_exit_runs(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        runs = list_exit_runs(connection, limit=limit)
    finally:
        connection.close()
    return {"runs": runs, **_safe_response_flags()}


@router.get("/errors")
def dry_run_exit_errors(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_exit_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors, **_safe_response_flags()}


@router.post("/evaluate", dependencies=[Depends(require_local_token)])
def dry_run_exit_evaluate(
    trade_date: str | None = Query(default=None),
    dry_run_position_id: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        if dry_run_position_id is not None:
            evaluation = evaluate_dry_run_exit_for_position(
                connection,
                dry_run_position_id,
                settings=settings,
            )
            return {"evaluation": evaluation.to_dict(), **_safe_response_flags()}
        result = evaluate_all_dry_run_exits(
            connection,
            trade_date=trade_date,
            limit=limit,
            settings=settings,
        )
        return result.to_dict()
    finally:
        connection.close()


@router.post(
    "/intents/from-position/{dry_run_position_id}",
    dependencies=[Depends(require_local_token)],
)
def dry_run_exit_intent_from_position(dry_run_position_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        intent = create_dry_run_exit_intent(
            connection,
            dry_run_position_id,
            settings=settings,
        )
    finally:
        connection.close()
    return {"intent": intent.to_dict(), **_safe_response_flags()}


@router.post(
    "/orders/from-intent/{dry_run_exit_intent_id}",
    dependencies=[Depends(require_local_token)],
)
def dry_run_exit_order_from_intent(dry_run_exit_intent_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            order = convert_exit_intent_to_dry_run_order(
                connection,
                dry_run_exit_intent_id,
                settings=settings,
            )
        except ValueError as exc:
            raise _bad_request(str(exc)) from exc
    finally:
        connection.close()
    return {"order": order.to_dict(), **_safe_response_flags()}


@router.post(
    "/orders/{dry_run_exit_order_id}/simulate-fill",
    dependencies=[Depends(require_local_token)],
)
def dry_run_exit_order_simulate_fill(dry_run_exit_order_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            execution = simulate_fill_dry_run_exit_order(
                connection,
                dry_run_exit_order_id,
                settings=settings,
            )
        except ValueError as exc:
            raise _bad_request(str(exc)) from exc
    finally:
        connection.close()
    return {"execution": execution.to_dict(), **_safe_response_flags()}


def _safe_response_flags() -> dict[str, bool]:
    return {
        "dry_run_only": True,
        "close_only": True,
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
