from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from fastapi import APIRouter, HTTPException, status
from services.config import load_settings
from services.market_context_service import (
    get_latest_market_context,
    get_market_context_for_code,
    get_market_context_status,
)
from services.market_regime_service import (
    get_latest_market_regime,
    get_market_regime_status,
    rebuild_market_regime_snapshot,
)
from storage.sqlite import open_connection

router = APIRouter(prefix="/api/market-regime")


@router.get("/latest")
def market_regime_latest() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        latest = get_latest_market_regime(connection)
        if latest is None:
            latest = rebuild_market_regime_snapshot(connection, settings=settings)
        status_payload = get_market_regime_status(connection, settings=settings)
    finally:
        connection.close()
    return {"latest": latest, "status": status_payload}


@router.get("/for-code/{code}")
def market_regime_for_code(code: str) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        context = get_market_context_for_code(
            connection,
            normalized_code,
            settings=settings,
        )
    finally:
        connection.close()
    return {
        "code": normalized_code,
        "market_context_snapshot_id": context.get("snapshot_id"),
        "market_regime": context["market_regime"],
        "read_only": True,
    }


@router.get("/context/status")
def market_context_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_market_context_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/context/latest/{market}")
def market_context_latest(market: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            snapshot = get_latest_market_context(connection, market)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=str(exc),
            ) from exc
    finally:
        connection.close()
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="market context snapshot not found",
        )
    return {"market_context": snapshot}


@router.get("/context/for-code/{code}")
def market_context_for_code(code: str) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        context = get_market_context_for_code(
            connection,
            normalized_code,
            settings=settings,
        )
    finally:
        connection.close()
    return {"code": normalized_code, "market_context": context}


def _normalize_code_or_422(code: str) -> str:
    try:
        return validate_stock_code(code)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
