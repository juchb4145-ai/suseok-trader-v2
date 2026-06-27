from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from api.routes.ai_codex import router as ai_codex_router
from api.routes.ai_live_sim_review import router as ai_live_sim_review_router
from api.routes.ai_rca import router as ai_rca_router
from api.routes.ai_sidecar import router as ai_sidecar_router
from api.routes.candidates import router as candidates_router
from api.routes.dashboard import router as dashboard_router
from api.routes.dashboard_page import router as dashboard_page_router
from api.routes.dry_run_exit import router as dry_run_exit_router
from api.routes.dry_run_oms import router as dry_run_oms_router
from api.routes.gateway import router as gateway_router
from api.routes.health import router as health_router
from api.routes.live_sim import router as live_sim_router
from api.routes.market_data import router as market_data_router
from api.routes.risk import router as risk_router
from api.routes.strategy import router as strategy_router
from api.routes.themes import router as themes_router
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from services.config import load_settings
from storage.sqlite import initialize_database

ROOT_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = ROOT_DIR / "web" / "static"


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    settings = load_settings()
    connection = initialize_database(settings.trading_db_path)
    connection.close()
    yield


def create_app() -> FastAPI:
    application = FastAPI(
        title="suseok-trader-v2 Core API",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.include_router(health_router)
    application.include_router(ai_sidecar_router)
    application.include_router(ai_rca_router)
    application.include_router(ai_codex_router)
    application.include_router(ai_live_sim_review_router)
    application.include_router(gateway_router)
    application.include_router(market_data_router)
    application.include_router(themes_router)
    application.include_router(candidates_router)
    application.include_router(strategy_router)
    application.include_router(risk_router)
    application.include_router(dry_run_oms_router)
    application.include_router(dry_run_exit_router)
    application.include_router(live_sim_router)
    application.include_router(dashboard_router)
    application.include_router(dashboard_page_router)
    application.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
    return application


app = create_app()
