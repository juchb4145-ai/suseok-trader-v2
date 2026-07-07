from __future__ import annotations

from typing import Any

from domain.broker.utils import BrokerValidationError, validate_stock_code
from domain.theme.state import ThemeState
from fastapi import APIRouter, Depends, HTTPException, Query, status
from services.config import clear_settings_cache, load_settings
from services.runtime.evaluation_run_guard import EvaluationRunLockError
from services.runtime.theme_refresh_cycle import (
    get_latest_theme_refresh_cycle_run,
    run_theme_refresh_cycle_once,
)
from services.theme_diagnostics import (
    build_naver_leading_theme_overlap_report,
    build_theme_data_wait_diagnostics,
)
from services.theme_service import (
    calculate_all_theme_snapshots,
    calculate_theme_snapshot,
    get_latest_theme_snapshot,
    get_theme,
    get_theme_status,
    import_theme_memberships,
    list_latest_theme_snapshots,
    list_theme_members,
    list_theme_projection_errors,
    list_theme_snapshots,
    list_themes,
    list_themes_for_code,
)
from storage.sqlite import open_connection

from api.dependencies.auth import require_local_token

router = APIRouter(prefix="/api/themes")


@router.get("/status")
def themes_status() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return get_theme_status(connection, settings=settings)
    finally:
        connection.close()


@router.get("")
def themes_list(
    active_only: bool = Query(default=True),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        themes = list_themes(connection, active_only=active_only)
    finally:
        connection.close()
    return {"themes": themes}


@router.get("/by-code/{code}")
def themes_by_code(code: str) -> dict[str, Any]:
    normalized_code = _normalize_code_or_422(code)
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        themes = list_themes_for_code(connection, normalized_code)
    finally:
        connection.close()
    return {"code": normalized_code, "themes": themes}


@router.get("/snapshots/latest")
def latest_theme_snapshots(
    limit: int = Query(default=100, ge=1, le=500),
    state: ThemeState | None = None,
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshots = list_latest_theme_snapshots(connection, limit=limit, state=state)
    finally:
        connection.close()
    return {"snapshots": snapshots}


@router.get("/projection-errors")
def theme_projection_errors(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, list[dict[str, Any]]]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        errors = list_theme_projection_errors(connection, limit=limit)
    finally:
        connection.close()
    return {"errors": errors}


@router.get("/diagnostics/data-wait")
def theme_data_wait_diagnostics(
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_theme_data_wait_diagnostics(
            connection,
            settings=settings,
            limit=limit,
        )
    finally:
        connection.close()


@router.get("/diagnostics/naver-overlap")
def theme_naver_overlap_diagnostics(
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        return build_naver_leading_theme_overlap_report(connection, limit=limit)
    finally:
        connection.close()


@router.post("/import", dependencies=[Depends(require_local_token)])
def themes_import(
    body: dict[str, Any],
    replace: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = import_theme_memberships(connection, body, replace=replace)
        except (BrokerValidationError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc
        return result.to_dict()
    finally:
        connection.close()


@router.post("/snapshots/rebuild", dependencies=[Depends(require_local_token)])
def rebuild_theme_snapshots(
    theme_id: str | None = Query(default=None),
    calculated_at: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            if theme_id is not None:
                snapshot = calculate_theme_snapshot(
                    connection,
                    theme_id,
                    calculated_at=calculated_at,
                    settings=settings,
                )
                return {
                    "processed_theme_count": 1,
                    "snapshot_count": 1,
                    "error_count": int(snapshot.metadata.get("member_error_count", 0)),
                    "snapshot": snapshot.to_dict(include_members=False),
                }
            result = calculate_all_theme_snapshots(
                connection,
                calculated_at=calculated_at,
                settings=settings,
            )
        except (BrokerValidationError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc
        return result.to_dict()
    finally:
        connection.close()


@router.post("/refresh-cycle/run-once", dependencies=[Depends(require_local_token)])
def run_theme_refresh_cycle(
    trade_date: str | None = Query(default=None),
    queue_market_scan_commands: bool | None = Query(default=None),
    queue_realtime_commands: bool | None = Query(default=None),
) -> dict[str, Any]:
    clear_settings_cache()
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = run_theme_refresh_cycle_once(
                connection,
                trade_date=trade_date,
                settings=settings,
                queue_market_scan_commands=queue_market_scan_commands,
                queue_realtime_commands=queue_realtime_commands,
            )
        except EvaluationRunLockError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=exc.to_dict()) from exc
        return result.to_dict()
    finally:
        connection.close()


@router.get("/refresh-cycle/latest")
def latest_theme_refresh_cycle() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        run = get_latest_theme_refresh_cycle_run(connection)
    finally:
        connection.close()
    return {
        "run": run,
        "read_only": True,
        "no_order_side_effects": True,
        "live_real_allowed": False,
    }


@router.get("/{theme_id}/members")
def theme_members(theme_id: str) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        theme = get_theme(connection, theme_id)
        if theme is None:
            raise _theme_404(theme_id)
        members = list_theme_members(connection, theme_id)
    finally:
        connection.close()
    return {"theme": theme, "members": members}


@router.get("/{theme_id}/snapshot/latest")
def latest_theme_snapshot(
    theme_id: str,
    include_members: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        snapshot = get_latest_theme_snapshot(
            connection,
            theme_id,
            include_members=include_members,
        )
    finally:
        connection.close()
    if snapshot is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"latest theme snapshot not found for theme_id={theme_id}",
        )
    return {"snapshot": snapshot}


@router.get("/{theme_id}/snapshots")
def theme_snapshot_history(
    theme_id: str,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        theme = get_theme(connection, theme_id)
        if theme is None:
            raise _theme_404(theme_id)
        snapshots = list_theme_snapshots(connection, theme_id, limit=limit)
    finally:
        connection.close()
    return {"theme": theme, "snapshots": snapshots}


@router.get("/{theme_id}")
def theme_detail(
    theme_id: str,
    include_members: bool = Query(default=False),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        theme = get_theme(connection, theme_id)
        if theme is None:
            raise _theme_404(theme_id)
        response: dict[str, Any] = {"theme": theme}
        if include_members:
            response["members"] = list_theme_members(connection, theme_id)
    finally:
        connection.close()
    return response


def _normalize_code_or_422(code: str) -> str:
    try:
        return validate_stock_code(code)
    except (BrokerValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


def _theme_404(theme_id: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"theme not found: {theme_id}",
    )
