from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from domain.candidate.state import CandidateState
from fastapi import APIRouter, Depends, HTTPException, Query, status
from services.candidate_service import (
    get_candidate,
    get_candidate_status,
    list_candidate_projection_errors,
    list_candidate_sources,
    list_candidate_transitions,
    list_candidates,
    rebuild_candidates_from_observations,
)
from services.config import load_settings
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/candidates")


@router.get("/status")
def candidates_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_candidate_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("")
def candidates_list(
    trade_date: str | None = Query(default=None),
    state: CandidateState | None = None,
    code: str | None = Query(default=None),
    theme_id: str | None = Query(default=None),
    active_only: bool = Query(default=True),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    normalized_code = _normalize_code_or_422(code) if code is not None else None
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        candidates = list_candidates(
            connection,
            trade_date=trade_date,
            state=state,
            code=normalized_code,
            theme_id=theme_id,
            active_only=active_only,
            limit=limit,
        )
    finally:
        connection.close()
    return {"candidates": candidates}


@router.get("/by-code/{code}")
def candidates_by_code(
    code: str,
    trade_date: str | None = Query(default=None),
    active_only: bool = Query(default=True),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        candidates = list_candidates(
            connection,
            trade_date=trade_date,
            code=normalized_code,
            active_only=active_only,
            limit=500,
        )
    finally:
        connection.close()
    return {"code": normalized_code, "candidates": candidates}


@router.get("/projection-errors")
def candidate_projection_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_candidate_projection_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


@router.post("/rebuild", dependencies=[Depends(require_local_token)])
def candidates_rebuild(
    trade_date: str | None = Query(default=None),
) -> dict[str, int]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = rebuild_candidates_from_observations(
            connection,
            trade_date=trade_date,
            settings=settings,
        )
        return result.to_dict()
    finally:
        connection.close()


@router.get("/{candidate_instance_id}/sources")
def candidate_sources(candidate_instance_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        candidate = get_candidate(connection, candidate_instance_id)
        if candidate is None:
            raise _candidate_404(candidate_instance_id)
        sources = list_candidate_sources(connection, candidate_instance_id)
    finally:
        connection.close()
    return {"candidate": candidate, "sources": sources}


@router.get("/{candidate_instance_id}/transitions")
def candidate_transitions(candidate_instance_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        candidate = get_candidate(connection, candidate_instance_id)
        if candidate is None:
            raise _candidate_404(candidate_instance_id)
        transitions = list_candidate_transitions(connection, candidate_instance_id)
    finally:
        connection.close()
    return {"candidate": candidate, "transitions": transitions}


@router.get("/{candidate_instance_id}")
def candidate_detail(
    candidate_instance_id: str,
    include_context: bool = Query(default=False),
    include_sources: bool = Query(default=False),
    include_transitions: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        candidate = get_candidate(
            connection,
            candidate_instance_id,
            include_context=include_context,
            include_sources=include_sources,
            include_transitions=include_transitions,
        )
    finally:
        connection.close()
    if candidate is None:
        raise _candidate_404(candidate_instance_id)
    return {"candidate": candidate}


def _normalize_code_or_422(code: str) -> str:
    try:
        return validate_stock_code(code)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


def _candidate_404(candidate_instance_id: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"candidate not found: {candidate_instance_id}",
    )
