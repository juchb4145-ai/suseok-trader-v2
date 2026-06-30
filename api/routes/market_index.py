from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from services.config import load_settings
from services.market_index_service import (
    get_latest_market_index_tick,
    get_market_index_readiness,
    get_market_index_status,
    list_latest_market_index_ticks,
    list_market_index_bars,
    normalize_index_code,
)
from storage.sqlite import open_connection

router = APIRouter(prefix="/api/market-indexes")


@router.get("/status")
def market_indexes_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_market_index_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/latest")
def market_indexes_latest(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        ticks = list_latest_market_index_ticks(connection, limit=limit)
    finally:
        connection.close()
    return {"ticks": ticks}


@router.get("/{index_code}")
def market_index_latest(index_code: str) -> dict[str, Any]:
    normalized_code = _normalize_index_or_422(index_code)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        tick = get_latest_market_index_tick(connection, normalized_code)
        readiness = get_market_index_readiness(connection, normalized_code, settings=settings)
    finally:
        connection.close()
    if tick is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"latest market index tick not found for index_code={normalized_code}",
        )
    return {"found": True, "tick": tick, "readiness": readiness}


@router.get("/{index_code}/bars")
def market_index_bars(
    index_code: str,
    interval_sec: int = Query(default=60, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_index_or_422(index_code)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        bars = list_market_index_bars(
            connection,
            normalized_code,
            interval_sec=interval_sec,
            limit=limit,
        )
    finally:
        connection.close()
    return {"index_code": normalized_code, "interval_sec": interval_sec, "bars": bars}


def _normalize_index_or_422(index_code: str) -> str:
    try:
        return normalize_index_code(index_code)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc
