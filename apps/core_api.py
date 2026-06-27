from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from api.routes.ai_sidecar import router as ai_sidecar_router
from api.routes.gateway import router as gateway_router
from api.routes.health import router as health_router
from api.routes.market_data import router as market_data_router
from api.routes.themes import router as themes_router
from fastapi import FastAPI
from services.config import load_settings
from storage.sqlite import initialize_database


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
    application.include_router(gateway_router)
    application.include_router(market_data_router)
    application.include_router(themes_router)
    return application


app = create_app()
