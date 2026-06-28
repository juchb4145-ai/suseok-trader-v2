from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from services.config import load_settings
from services.operator.no_buy_sentinel import (
    build_no_buy_sentinel_snapshot,
    get_latest_no_buy_sentinel_snapshot,
    rebuild_no_buy_sentinel_snapshot,
)
from services.operator.operator_status import build_operator_status
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/operator")


@router.get("/status")
def operator_status(
    trade_date: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_operator_status(
            connection,
            settings=settings,
            trade_date=trade_date,
            include_no_buy_rebuild=True,
        )
    finally:
        connection.close()


@router.get("/no-buy")
def operator_no_buy(
    trade_date: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=100),
    include_ai: bool | None = Query(default=None),
    include_debug: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = build_no_buy_sentinel_snapshot(
            connection,
            settings=settings,
            trade_date=trade_date,
            manual=True,
            limit=limit,
            include_ai=include_ai,
            include_debug=include_debug,
            write_snapshot=False,
        )
        return snapshot.to_dict()
    finally:
        connection.close()


@router.get("/no-buy/latest")
def operator_no_buy_latest(
    trade_date: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = get_latest_no_buy_sentinel_snapshot(connection, trade_date=trade_date)
        return {
            "snapshot": snapshot,
            "read_only": True,
            "no_order_side_effects": True,
        }
    finally:
        connection.close()


@router.post("/no-buy/rebuild", dependencies=[Depends(require_local_token)])
def operator_no_buy_rebuild(
    trade_date: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=100),
    include_ai: bool | None = Query(default=None),
    include_debug: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = rebuild_no_buy_sentinel_snapshot(
            connection,
            settings=settings,
            trade_date=trade_date,
            limit=limit,
            include_ai=include_ai,
            include_debug=include_debug,
        )
        return snapshot.to_dict()
    finally:
        connection.close()
