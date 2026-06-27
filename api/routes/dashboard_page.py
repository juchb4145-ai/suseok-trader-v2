from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from services.config import load_settings

router = APIRouter()

ROOT_DIR = Path(__file__).resolve().parents[2]
DASHBOARD_HTML = ROOT_DIR / "web" / "templates" / "dashboard.html"


@router.get("/", response_class=HTMLResponse)
@router.get("/dashboard", response_class=HTMLResponse)
def dashboard_page() -> HTMLResponse:
    settings = load_settings()
    if not settings.dashboard_route_enabled:
        raise HTTPException(status_code=404, detail="dashboard route is disabled")
    return HTMLResponse(DASHBOARD_HTML.read_text(encoding="utf-8"))
