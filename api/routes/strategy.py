from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from domain.candidate.state import CandidateState
from domain.strategy.setup import StrategySetupType
from domain.strategy.status import StrategyObservationStatus
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from services.config import load_settings
from services.runtime.evaluation_run_guard import EvaluationRunLockError
from services.strategy_engine import (
    evaluate_candidates,
    get_latest_strategy_observation,
    get_strategy_status,
    list_latest_strategy_observations,
    list_strategy_errors,
    list_strategy_observations_for_candidate,
    list_strategy_runs,
    list_strategy_setup_observations,
)
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/strategy")


@router.get("/status")
def strategy_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_strategy_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/observations/latest")
def strategy_latest_observations(
    trade_date: str | None = Query(default=None),
    status: StrategyObservationStatus | None = None,
    setup_type: StrategySetupType | None = None,
    code: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        observations = list_latest_strategy_observations(
            connection,
            trade_date=trade_date,
            status=status,
            setup_type=setup_type,
            code=normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"observations": observations}


@router.get("/candidates/{candidate_instance_id}")
def strategy_candidate_latest(
    candidate_instance_id: str,
    include_setups: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        observation = get_latest_strategy_observation(
            connection,
            candidate_instance_id,
            include_setups=include_setups,
        )
    finally:
        connection.close()
    if observation is None:
        raise _strategy_observation_404(candidate_instance_id)
    return {"observation": observation}


@router.get("/candidates/{candidate_instance_id}/history")
def strategy_candidate_history(
    candidate_instance_id: str,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        observations = list_strategy_observations_for_candidate(
            connection,
            candidate_instance_id,
            limit=limit,
        )
    finally:
        connection.close()
    return {"observations": observations}


@router.get("/observations/{strategy_observation_id}/setups")
def strategy_observation_setups(strategy_observation_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        setups = list_strategy_setup_observations(connection, strategy_observation_id)
    finally:
        connection.close()
    return {"strategy_observation_id": strategy_observation_id, "setup_observations": setups}


@router.get("/runs")
def strategy_runs(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        runs = list_strategy_runs(connection, limit=limit)
    finally:
        connection.close()
    return {"runs": runs}


@router.get("/errors")
def strategy_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_strategy_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


@router.post("/evaluate", dependencies=[Depends(require_local_token)])
def strategy_evaluate(
    trade_date: str | None = Query(default=None),
    candidate_instance_id: str | None = Query(default=None),
    state: CandidateState | None = CandidateState.CONTEXT_READY,
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = evaluate_candidates(
                connection,
                trade_date=trade_date,
                candidate_state=None if candidate_instance_id is not None else state,
                limit=limit,
                settings=settings,
                candidate_instance_id=candidate_instance_id,
            )
        except EvaluationRunLockError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail=exc.to_dict(),
            ) from exc
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


def _strategy_observation_404(candidate_instance_id: str) -> HTTPException:
    return HTTPException(
        status_code=http_status.HTTP_404_NOT_FOUND,
        detail=f"strategy observation not found for candidate={candidate_instance_id}",
    )
