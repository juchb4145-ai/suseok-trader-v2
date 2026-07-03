from __future__ import annotations

from datetime import date
from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from fastapi import APIRouter, HTTPException, Query, status
from services.config import load_settings
from services.market_data_service import (
    get_latest_tick,
    get_market_data_readiness,
    get_market_data_status,
    list_bars,
    list_cross_exchange_observations,
    list_latest_ticks,
    list_latest_ticks_for_code,
    list_premarket_snapshots,
    list_projection_errors,
    list_recent_condition_signals,
    list_recent_tr_snapshots,
    normalize_market_data_exchange_filter,
)
from storage.sqlite import open_connection

router = APIRouter(prefix="/api/market-data")


@router.get("/status")
def market_data_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_market_data_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("/ticks/latest")
def market_data_latest_ticks(
    limit: int = Query(default=100, ge=1, le=500),
    exchange: str = Query(default="KRX"),
) -> dict[str, list[dict[str, Any]]]:
    normalized_exchange = _normalize_exchange_or_422(exchange)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        ticks = list_latest_ticks(connection, limit=limit, exchange=normalized_exchange)
    finally:
        connection.close()
    return {"ticks": ticks}


@router.get("/ticks/{code}")
def market_data_tick(
    code: str,
    exchange: str = Query(default="KRX"),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    normalized_exchange = _normalize_exchange_or_422(exchange)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        if normalized_exchange == "ALL":
            ticks = list_latest_ticks_for_code(
                connection,
                normalized_code,
                exchange=normalized_exchange,
            )
            tick = None
        else:
            tick = get_latest_tick(connection, normalized_code, exchange=normalized_exchange)
            ticks = []
    finally:
        connection.close()
    if normalized_exchange == "ALL":
        if not ticks:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"latest tick not found for code={normalized_code}",
            )
        return {
            "found": True,
            "code": normalized_code,
            "exchange": normalized_exchange,
            "ticks": ticks,
        }
    if tick is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"latest tick not found for code={normalized_code}",
        )
    return {"found": True, "tick": tick}


@router.get("/bars/{code}")
def market_data_bars(
    code: str,
    exchange: str = Query(default="KRX"),
    interval_sec: int = Query(default=60, ge=1),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    normalized_exchange = _normalize_exchange_or_422(exchange)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        bars = list_bars(
            connection,
            normalized_code,
            exchange=normalized_exchange,
            interval_sec=interval_sec,
            limit=limit,
        )
    finally:
        connection.close()
    return {
        "code": normalized_code,
        "exchange": normalized_exchange,
        "interval_sec": interval_sec,
        "bars": bars,
    }


@router.get("/readiness/{code}")
def market_data_readiness(
    code: str,
    exchange: str = Query(default="KRX"),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    normalized_exchange = _normalize_exchange_or_422(exchange)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        if normalized_exchange == "ALL":
            return {
                "code": normalized_code,
                "exchange": normalized_exchange,
                "readiness": [
                    get_market_data_readiness(
                        connection,
                        normalized_code,
                        exchange=item_exchange,
                        settings=settings,
                    )
                    for item_exchange in ("KRX", "NXT")
                ],
            }
        return get_market_data_readiness(
            connection,
            normalized_code,
            exchange=normalized_exchange,
            settings=settings,
        )
    finally:
        connection.close()


@router.get("/premarket/{trade_date}")
def market_data_premarket_snapshots(
    trade_date: str,
    limit: int = Query(default=500, ge=1, le=500),
) -> dict[str, Any]:
    normalized_trade_date = _normalize_trade_date_or_422(trade_date)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshots = list_premarket_snapshots(
            connection,
            normalized_trade_date,
            limit=limit,
        )
    finally:
        connection.close()
    return {"trade_date": normalized_trade_date, "snapshots": snapshots}


@router.get("/cross-exchange/{code}")
def market_data_cross_exchange_observations(
    code: str,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        observations = list_cross_exchange_observations(
            connection,
            normalized_code,
            limit=limit,
        )
    finally:
        connection.close()
    return {"code": normalized_code, "observations": observations}


@router.get("/conditions/recent")
def market_data_conditions_recent(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        signals = list_recent_condition_signals(connection, limit=limit)
    finally:
        connection.close()
    return {"signals": signals}


@router.get("/tr-snapshots/recent")
def market_data_tr_snapshots_recent(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshots = list_recent_tr_snapshots(connection, limit=limit)
    finally:
        connection.close()
    return {"snapshots": snapshots}


@router.get("/projection-errors")
def market_data_projection_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_projection_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


def _normalize_code_or_422(code: str) -> str:
    try:
        return validate_stock_code(code)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


def _normalize_exchange_or_422(exchange: str) -> str:
    try:
        return normalize_market_data_exchange_filter(exchange)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


def _normalize_trade_date_or_422(trade_date: str) -> str:
    try:
        return date.fromisoformat(str(trade_date).strip()).isoformat()
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="trade_date must be YYYY-MM-DD",
        ) from exc
