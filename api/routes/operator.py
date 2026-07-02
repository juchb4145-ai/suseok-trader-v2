from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import status as http_status
from services.config import load_settings
from services.operator.no_buy_sentinel import (
    build_no_buy_sentinel_snapshot,
    get_latest_no_buy_sentinel_snapshot,
    rebuild_no_buy_sentinel_snapshot,
)
from services.operator.operator_status import build_operator_status
from services.realtime_subscription import (
    build_realtime_subscription_plan,
    run_realtime_subscription_once,
)
from services.runtime.evaluation_run_guard import EvaluationRunLockError
from services.runtime.market_open_observe_cycle import (
    get_latest_market_open_observe_cycle_run,
    list_market_open_observe_cycle_runs,
    run_market_open_observe_cycle_once,
)
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


@router.post("/observe-cycle/run-once", dependencies=[Depends(require_local_token)])
def operator_observe_cycle_run_once(
    trade_date: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        try:
            result = run_market_open_observe_cycle_once(
                connection,
                settings=settings,
                trade_date=trade_date,
                limit=limit,
            )
        except EvaluationRunLockError as exc:
            raise HTTPException(
                status_code=http_status.HTTP_409_CONFLICT,
                detail=exc.to_dict(),
            ) from exc
        return result.to_dict()
    finally:
        connection.close()


@router.get("/observe-cycle/runs")
def operator_observe_cycle_runs(
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        runs = list_market_open_observe_cycle_runs(connection, limit=limit)
    finally:
        connection.close()
    return {
        "runs": runs,
        "read_only": True,
        "observe_only": True,
        "no_order_controls": True,
    }


@router.get("/observe-cycle/runs/latest")
def operator_observe_cycle_run_latest() -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        run = get_latest_market_open_observe_cycle_run(connection)
    finally:
        connection.close()
    return {
        "run": run,
        "read_only": True,
        "observe_only": True,
        "no_order_controls": True,
    }


@router.get("/realtime-subscriptions/plan")
def operator_realtime_subscriptions_plan(
    trade_date: str | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        plan = build_realtime_subscription_plan(
            connection,
            settings=settings,
            trade_date=trade_date,
            queue_commands=False,
        )
        payload = plan.to_dict()
        payload["read_only"] = True
        payload["queue_commands"] = False
        payload["command_count"] = 0
        return payload
    finally:
        connection.close()


@router.post("/realtime-subscriptions/run-once", dependencies=[Depends(require_local_token)])
def operator_realtime_subscriptions_run_once(
    trade_date: str | None = Query(default=None),
    queue_commands: bool | None = Query(default=None),
) -> dict[str, Any]:
    settings = load_settings()
    connection = open_connection(settings.trading_db_path)
    try:
        plan = run_realtime_subscription_once(
            connection,
            settings=settings,
            trade_date=trade_date,
            queue_commands=queue_commands,
        )
        return plan.to_dict()
    finally:
        connection.close()
