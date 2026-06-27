from __future__ import annotations

from pathlib import Path

from apps.core_api import app
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]


def test_dashboard_page_and_static_assets_are_served(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "dashboard-page.sqlite3"))

    with TestClient(app) as client:
        root = client.get("/")
        dashboard = client.get("/dashboard")
        css = client.get("/static/dashboard.css")
        js = client.get("/static/dashboard.js")

    assert root.status_code == 200
    assert dashboard.status_code == 200
    assert css.status_code == 200
    assert js.status_code == 200
    assert "현재 Dashboard는 읽기 전용이며 주문 기능이 없습니다" in root.text
    assert "data-dashboard-root" in root.text
    assert "safety-panel" in root.text
    assert "pipeline-section" in root.text
    assert "dry-run-section" in root.text
    assert "DRY_RUN Exit" in root.text
    assert "MATCHED_OBSERVATION은 매수 신호가 아닙니다." in root.text
    assert "OBSERVE_PASS는 주문 승인이 아닙니다." in root.text


def test_dashboard_static_sources_have_no_execution_controls() -> None:
    html = (ROOT / "web" / "templates" / "dashboard.html").read_text(encoding="utf-8")
    js = (ROOT / "web" / "static" / "dashboard.js").read_text(encoding="utf-8")

    assert "<button" not in html.lower()
    assert "AI 실행" not in html
    assert "매수</button" not in html
    assert "매도</button" not in html
    assert "취소</button" not in html
    assert "정정</button" not in html
    assert "POST" not in js
    assert "/api/orders" not in js
    assert "/api/dry-run/exits" not in js
    assert "simulate-fill" not in js
    assert "send_order" not in js
    assert "cancel_order" not in js
    assert "modify_order" not in js
    assert "gateway_commands enqueue" not in js
