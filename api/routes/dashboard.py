from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Query
from services.config import load_settings
from services.dashboard_service import (
    build_dashboard_errors,
    build_dashboard_snapshot,
    build_dashboard_status,
)
from storage.sqlite import open_connection

router = APIRouter(prefix="/api/dashboard")


@router.get("/snapshot")
def dashboard_snapshot(
    detail: Literal["summary", "full"] = "summary",
    limit: int | None = Query(default=None, ge=1, le=200),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_dashboard_snapshot(
            connection,
            settings,
            detail=detail,
            limit=limit,
        )
    finally:
        connection.close()


@router.get("/status")
def dashboard_status() -> dict[str, Any]:
    settings = load_settings()
    return build_dashboard_status(settings)


@router.get("/funnel")
def dashboard_funnel() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = build_dashboard_snapshot(connection, settings, detail="summary", limit=1)
    finally:
        connection.close()
    return snapshot["pipeline_summary"]


@router.get("/errors")
def dashboard_errors(
    limit: int | None = Query(default=None, ge=1, le=200),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_dashboard_errors(connection, settings=settings, limit=limit)
    finally:
        connection.close()
