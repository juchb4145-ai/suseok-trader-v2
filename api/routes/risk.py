from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from domain.risk.status import RiskObservationStatus, RiskSeverity
from domain.strategy.status import StrategyObservationStatus
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from services.config import load_settings
from services.risk_gate import (
    evaluate_risk_observations,
    get_latest_risk_observation,
    get_risk_observation,
    get_risk_status,
    list_latest_risk_observations,
    list_risk_check_observations,
    list_risk_errors,
    list_risk_observations_for_candidate,
    list_risk_runs,
)
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/risk")


@router.get("/status")
def risk_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_risk_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/observations/latest")
def risk_latest_observations(
    trade_date: str | None = Query(default=None),
    status: RiskObservationStatus | None = None,
    code: str | None = Query(default=None),
    max_severity: RiskSeverity | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        observations = list_latest_risk_observations(
            connection,
            trade_date=trade_date,
            status=status,
            code=normalized_code,
            max_severity=max_severity,
            limit=limit,
        )
    finally:
        connection.close()
    return {"observations": observations}


@router.get("/candidates/{candidate_instance_id}")
def risk_candidate_latest(
    candidate_instance_id: str,
    include_checks: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        observation = get_latest_risk_observation(
            connection,
            candidate_instance_id,
            include_checks=include_checks,
        )
    finally:
        connection.close()
    if observation is None:
        raise _risk_observation_404(candidate_instance_id)
    return {"observation": observation}


@router.get("/candidates/{candidate_instance_id}/history")
def risk_candidate_history(
    candidate_instance_id: str,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        observations = list_risk_observations_for_candidate(
            connection,
            candidate_instance_id,
            limit=limit,
        )
    finally:
        connection.close()
    return {"observations": observations}


@router.get("/observations/{risk_observation_id}")
def risk_observation(
    risk_observation_id: str,
    include_checks: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        observation = get_risk_observation(
            connection,
            risk_observation_id,
            include_checks=include_checks,
        )
    finally:
        connection.close()
    if observation is None:
        raise HTTPException(
            status_code=http_status.HTTP_404_NOT_FOUND,
            detail=f"risk observation not found: {risk_observation_id}",
        )
    return {"observation": observation}


@router.get("/observations/{risk_observation_id}/checks")
def risk_observation_checks(risk_observation_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        checks = list_risk_check_observations(connection, risk_observation_id)
    finally:
        connection.close()
    return {"risk_observation_id": risk_observation_id, "check_observations": checks}


@router.get("/runs")
def risk_runs(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        runs = list_risk_runs(connection, limit=limit)
    finally:
        connection.close()
    return {"runs": runs}


@router.get("/errors")
def risk_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_risk_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


@router.post("/evaluate", dependencies=[Depends(require_local_token)])
def risk_evaluate(
    trade_date: str | None = Query(default=None),
    candidate_instance_id: str | None = Query(default=None),
    strategy_observation_id: str | None = Query(default=None),
    strategy_status: StrategyObservationStatus | None = (
        StrategyObservationStatus.MATCHED_OBSERVATION
    ),
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = evaluate_risk_observations(
            connection,
            trade_date=trade_date,
            candidate_instance_id=candidate_instance_id,
            strategy_observation_id=strategy_observation_id,
            strategy_status=(
                None if candidate_instance_id or strategy_observation_id else strategy_status
            ),
            limit=limit,
            settings=settings,
        )
    finally:
        connection.close()
    return result.to_dict()


def _normalize_code_or_422(code: str) -> str:
    try:
        return validate_stock_code(code)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=http_status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


def _risk_observation_404(candidate_instance_id: str) -> HTTPException:
    return HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"risk observation not found for candidate={candidate_instance_id}",
    )
