from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from fastapi import APIRouter, HTTPException, Query, status
from services.config import load_settings
from services.market_reference_service import (
    get_market_for_code,
    list_market_symbol_memberships,
)
from storage.sqlite import open_connection

router = APIRouter(prefix="/api/market-reference")


@router.get("/symbols")
def market_reference_symbols(
    market: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=1000),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        symbols = list_market_symbol_memberships(
            connection,
            market=market,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
    finally:
        connection.close()
    return {"symbols": symbols}


@router.get("/symbols/{code}")
def market_reference_symbol(code: str) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        membership = get_market_for_code(connection, normalized_code)
    finally:
        connection.close()
    if membership is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"market membership not found for code={normalized_code}",
        )
    return {"found": True, "symbol": membership}


def _normalize_code_or_422(code: str) -> str:
    try:
        return validate_stock_code(code)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
