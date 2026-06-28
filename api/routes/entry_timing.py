from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from services.config import load_settings
from services.entry_timing.models import OrderPlanStatus
from services.entry_timing.service import (
    evaluate_entry_timing,
    get_entry_timing_status,
    get_order_plan_draft,
    list_entry_timing_errors,
    list_entry_timing_evaluations,
    list_latest_order_plan_drafts,
)
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/entry-timing")


@router.get("/status")
def entry_timing_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_entry_timing_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/plans/latest")
def latest_order_plan_drafts(
    trade_date: str | None = Query(default=None),
    status: OrderPlanStatus | None = None,
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        plans = list_latest_order_plan_drafts(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"order_plan_drafts": plans}


@router.get("/plans/{order_plan_id}")
def order_plan_draft_detail(order_plan_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        plan = get_order_plan_draft(connection, order_plan_id)
    finally:
        connection.close()
    if plan is None:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"order plan draft not found: {order_plan_id}",
        )
    return {"order_plan_draft": plan}


@router.get("/evaluations")
def entry_timing_evaluations(
    candidate_instance_id: str | None = Query(default=None),
    trade_date: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        evaluations = list_entry_timing_evaluations(
            connection,
            candidate_instance_id=candidate_instance_id,
            trade_date=trade_date,
            limit=limit,
        )
    finally:
        connection.close()
    return {"evaluations": evaluations}


@router.get("/errors")
def entry_timing_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_entry_timing_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


@router.post("/evaluate", dependencies=[Depends(require_local_token)])
def entry_timing_evaluate(
    trade_date: str | None = Query(default=None),
    candidate_instance_id: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
    write_order_plan_drafts: bool | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = evaluate_entry_timing(
                connection,
                trade_date=trade_date,
                candidate_instance_id=candidate_instance_id,
                limit=limit,
                write_order_plan_drafts=write_order_plan_drafts,
                settings=settings,
            )
        except (BrokerValidationError, ValueError) as exc:
            raise HTTPException(
                status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc
        return result.to_dict()
    finally:
        connection.close()


def _normalize_code_or_422(code: str) -> str:
    try:
        return validate_stock_code(code)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
