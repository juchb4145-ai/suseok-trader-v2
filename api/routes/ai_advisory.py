from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from services.ai_advisory.service import score_ai_candidates
from services.ai_advisory.storage import (
    build_status,
    get_run,
    list_errors,
    list_latest_scores,
    list_runs,
)
from services.config import load_settings
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/ai-advisory")


@router.get("/status")
def status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/candidate-scores/latest")
def latest_candidate_scores(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        payload = list_latest_scores(connection, limit=limit)
    finally:
        connection.close()
    return payload | {"advisory_only": True, "no_order_side_effects": True}


@router.get("/runs")
def runs(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = list_runs(connection, limit=limit)
    finally:
        connection.close()
    return {
        "runs": rows,
        "advisory_only": True,
        "no_order_side_effects": True,
    }


@router.get("/runs/{run_id}")
def run_detail(run_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        row = get_run(connection, run_id)
    finally:
        connection.close()
    if row is None:
        raise HTTPException(status_code=404, detail="AI advisory run not found")
    return row | {"advisory_only": True, "no_order_side_effects": True}


@router.get("/errors")
def errors(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        rows = list_errors(connection, limit=limit)
    finally:
        connection.close()
    return {
        "errors": rows,
        "advisory_only": True,
        "no_order_side_effects": True,
    }


@router.post("/score-candidates", dependencies=[Depends(require_local_token)])
def score_candidates(
    trade_date: str | None = None,
    limit: int | None = Query(default=None, ge=1, le=100),
    provider: str | None = Query(default=None, pattern="^(mock|external|external_http|openai)$"),
    dry_run: bool = Query(default=False),
    store_raw_response: bool = Query(default=False),
    allow_external: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = score_ai_candidates(
            connection,
            trade_date=trade_date,
            limit=limit,
            dry_run=dry_run,
            provider_name=provider,
            store_raw_response=store_raw_response,
            allow_external=allow_external,
            settings=settings,
        )
    finally:
        connection.close()
    return result.to_dict()
