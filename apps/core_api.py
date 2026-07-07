from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from api.dependencies.auth import enforce_local_token_middleware, ensure_auth_token_configured
from api.routes.ai_advisory import router as ai_advisory_router
from api.routes.ai_codex import router as ai_codex_router
from api.routes.ai_live_sim_review import router as ai_live_sim_review_router
from api.routes.ai_rca import router as ai_rca_router
from api.routes.ai_sidecar import router as ai_sidecar_router
from api.routes.candidates import router as candidates_router
from api.routes.dashboard import router as dashboard_router
from api.routes.dashboard_page import router as dashboard_page_router
from api.routes.dry_run_exit import router as dry_run_exit_router
from api.routes.dry_run_oms import router as dry_run_oms_router
from api.routes.entry_timing import router as entry_timing_router
from api.routes.gateway import router as gateway_router
from api.routes.health import router as health_router
from api.routes.live_sim import router as live_sim_router
from api.routes.market_data import router as market_data_router
from api.routes.market_index import router as market_index_router
from api.routes.market_reference import router as market_reference_router
from api.routes.market_regime import router as market_regime_router
from api.routes.operator import router as operator_router
from api.routes.risk import router as risk_router
from api.routes.strategy import router as strategy_router
from api.routes.theme_leadership import router as theme_leadership_router
from api.routes.themes import router as themes_router
from domain.broker.utils import market_is_weekday, market_time_str
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from services.condition_fusion import rebuild_condition_fusion
from services.config import Settings, clear_settings_cache, load_settings
from services.runtime.evaluation_run_guard import (
    EvaluationRunLockError,
    clear_runtime_execution_locks,
)
from services.runtime.incremental_evaluation import process_incremental_evaluation_batch
from services.runtime.live_sim_operating_orchestrator import run_live_sim_operating_cycle_once
from storage.event_retention import prune_event_store_events
from storage.sqlite import initialize_database, open_connection

ROOT_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = ROOT_DIR / "web" / "static"
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    ensure_auth_token_configured()
    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    try:
        _clear_startup_runtime_execution_locks(connection)
    finally:
        connection.close()
    condition_fusion_sweep_task = (
        asyncio.create_task(_condition_fusion_sweep_loop(settings))
        if settings.condition_fusion_sweep_enabled
        else None
    )
    incremental_evaluation_task = (
        asyncio.create_task(_incremental_evaluation_loop(settings))
        if settings.incremental_evaluation_enabled
        and settings.incremental_evaluation_worker_enabled
        else None
    )
    live_sim_operating_cycle_task = _maybe_create_live_sim_operating_cycle_task(settings)
    event_retention_task = (
        asyncio.create_task(_event_retention_loop(settings))
        if settings.event_store_retention_enabled
        else None
    )
    try:
        yield
    finally:
        if condition_fusion_sweep_task is not None:
            condition_fusion_sweep_task.cancel()
            with suppress(asyncio.CancelledError):
                await condition_fusion_sweep_task
        if incremental_evaluation_task is not None:
            incremental_evaluation_task.cancel()
            with suppress(asyncio.CancelledError):
                await incremental_evaluation_task
        if live_sim_operating_cycle_task is not None:
            live_sim_operating_cycle_task.cancel()
            with suppress(asyncio.CancelledError):
                await live_sim_operating_cycle_task
        if event_retention_task is not None:
            event_retention_task.cancel()
            with suppress(asyncio.CancelledError):
                await event_retention_task


async def _condition_fusion_sweep_loop(settings: Settings) -> None:
    interval_sec = max(int(settings.condition_fusion_sweep_interval_sec), 1)
    while True:
        await asyncio.sleep(interval_sec)
        try:
            await asyncio.to_thread(_run_condition_fusion_sweep_once, settings)
        except Exception:
            logger.exception("condition fusion periodic sweep failed")


def _run_condition_fusion_sweep_once(settings: Settings) -> None:
    connection = _open_runtime_database_connection(settings.trading_db_path)
    try:
        rebuild_condition_fusion(connection, settings=settings)
    finally:
        connection.close()


def _clear_startup_runtime_execution_locks(connection) -> int:
    deleted_count = clear_runtime_execution_locks(connection)
    logger.info("cleared runtime execution locks on startup: count=%s", deleted_count)
    return deleted_count


async def _incremental_evaluation_loop(settings: Settings) -> None:
    interval_sec = max(float(settings.incremental_evaluation_worker_interval_sec), 0.1)
    while True:
        await asyncio.sleep(interval_sec)
        try:
            await asyncio.to_thread(_run_incremental_evaluation_once, settings)
        except EvaluationRunLockError:
            logger.debug("incremental evaluation skipped because evaluation lock is held")
        except Exception:
            logger.exception("incremental evaluation worker failed")


def _run_incremental_evaluation_once(settings: Settings) -> None:
    connection = _open_runtime_database_connection(settings.trading_db_path)
    try:
        process_incremental_evaluation_batch(connection, settings=settings)
    finally:
        connection.close()


def _maybe_create_live_sim_operating_cycle_task(settings: Settings) -> asyncio.Task[None] | None:
    if not settings.live_sim_operating_loop_enabled:
        return None
    return asyncio.create_task(_live_sim_operating_cycle_loop(settings))


async def _live_sim_operating_cycle_loop(settings: Settings) -> None:
    interval_sec = max(int(settings.live_sim_operating_loop_interval_sec), 5)
    while True:
        await asyncio.sleep(interval_sec)
        await _live_sim_operating_cycle_tick(settings)


async def _live_sim_operating_cycle_tick(settings: Settings) -> None:
    try:
        await asyncio.to_thread(_run_live_sim_operating_cycle_once, settings)
    except EvaluationRunLockError:
        logger.debug("LIVE_SIM operating cycle skipped because evaluation lock is held")
    except Exception:
        logger.exception("LIVE_SIM operating cycle worker failed")


def _run_live_sim_operating_cycle_once(_startup_settings: Settings) -> None:
    clear_settings_cache()
    fresh_settings = load_settings()
    if not _is_live_sim_operating_market_time(fresh_settings):
        logger.debug("LIVE_SIM operating cycle skipped outside market hours")
        return

    connection = _open_runtime_database_connection(fresh_settings.trading_db_path)
    try:
        run_live_sim_operating_cycle_once(
            connection,
            settings=fresh_settings,
            mode=None,
            queue_commands=fresh_settings.live_sim_operating_loop_queue_commands,
        )
    finally:
        connection.close()


def _is_live_sim_operating_market_time(settings: Settings) -> bool:
    if not market_is_weekday():
        return False
    current_time = market_time_str()
    return (
        settings.live_sim_operating_loop_market_open_time
        <= current_time
        <= settings.live_sim_operating_loop_market_close_time
    )


async def _event_retention_loop(settings: Settings) -> None:
    interval_sec = max(int(settings.event_store_retention_interval_sec), 60)
    while True:
        await asyncio.sleep(interval_sec)
        try:
            await asyncio.to_thread(_run_event_retention_once, settings)
        except Exception:
            logger.exception("event retention worker failed")


def _run_event_retention_once(settings: Settings) -> None:
    connection = _open_runtime_database_connection(settings.trading_db_path)
    try:
        prune_event_store_events(connection, settings=settings, dry_run=False)
    finally:
        connection.close()


def _open_runtime_database_connection(db_path):
    return open_connection(db_path)


def create_app() -> FastAPI:
    application = FastAPI(
        title="suseok-trader-v2 Core API",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.middleware("http")(enforce_local_token_middleware)
    application.include_router(health_router)
    application.include_router(ai_advisory_router)
    application.include_router(ai_sidecar_router)
    application.include_router(ai_rca_router)
    application.include_router(ai_codex_router)
    application.include_router(ai_live_sim_review_router)
    application.include_router(gateway_router)
    application.include_router(market_data_router)
    application.include_router(market_reference_router)
    application.include_router(market_index_router)
    application.include_router(market_regime_router)
    application.include_router(themes_router)
    application.include_router(theme_leadership_router)
    application.include_router(candidates_router)
    application.include_router(strategy_router)
    application.include_router(risk_router)
    application.include_router(entry_timing_router)
    application.include_router(dry_run_oms_router)
    application.include_router(dry_run_exit_router)
    application.include_router(live_sim_router)
    application.include_router(operator_router)
    application.include_router(dashboard_router)
    application.include_router(dashboard_page_router)
    application.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return application


app = create_app()
