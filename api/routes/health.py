from __future__ import annotations

from fastapi import APIRouter
from services.config import load_settings

router = APIRouter()


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/api/status")
def status() -> dict[str, bool | str]:
    settings = load_settings()
    return {
        "status": "ok",
        "profile": settings.trading_profile.value,
        "mode": settings.trading_mode.value,
        "live_sim_allowed": settings.live_sim_allowed,
        "live_real_allowed": settings.live_real_allowed,
        "database_path": str(settings.trading_db_path),
    }
