from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError
from fastapi import APIRouter, Depends, HTTPException, Query, status
from services.config import load_settings
from services.theme_leadership import rebuild_theme_leadership
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/theme-leadership")


@router.get("/snapshots/latest")
def latest_theme_leadership_snapshots(
    include_members: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = rebuild_theme_leadership(
            connection,
            write_candidate_sources=False,
            settings=settings,
        )
        return {
            "status": result.status,
            "observe_only": True,
            "snapshots": [
                snapshot.to_dict(include_members=include_members) for snapshot in result.snapshots
            ],
        }
    finally:
        connection.close()


@router.get("/watchset")
def theme_leadership_watchset() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        result = rebuild_theme_leadership(
            connection,
            write_candidate_sources=False,
            settings=settings,
        )
        return {
            "status": result.status,
            "observe_only": True,
            "watchset": result.watchset.to_dict(),
        }
    finally:
        connection.close()


@router.post("/rebuild", dependencies=[Depends(require_local_token)])
def rebuild_theme_leadership_endpoint(
    trade_date: str | None = Query(default=None),
    write_candidate_sources: bool | None = Query(default=None),
    include_members: bool = Query(default=True),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = rebuild_theme_leadership(
                connection,
                trade_date=trade_date,
                write_candidate_sources=write_candidate_sources,
                settings=settings,
            )
        except (BrokerValidationError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc
        return result.to_dict(include_members=include_members)
    finally:
        connection.close()
