from __future__ import annotations

from pathlib import Path

from apps.core_api import app

ROOT = Path(__file__).resolve().parents[1]


def test_dashboard_surface_has_no_write_or_execution_routes() -> None:
    paths = {route.path for route in app.routes}
    dashboard_routes = [route for route in app.routes if route.path.startswith("/api/dashboard")]

    assert "/api/dashboard/status" in paths
    assert "/api/dashboard/snapshot" in paths
    assert "/api/dashboard/funnel" in paths
    assert "/api/dashboard/errors" in paths
    assert "/api/orders/enqueue" not in paths
    assert all("POST" not in route.methods for route in dashboard_routes)
    assert all("send_order" not in path for path in paths)
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)


def test_dashboard_code_has_no_trading_or_ai_execution_path() -> None:
    dashboard_source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in [
            ROOT / "services" / "dashboard_service.py",
            ROOT / "api" / "routes" / "dashboard.py",
            ROOT / "api" / "routes" / "dashboard_page.py",
            ROOT / "web" / "templates" / "dashboard.html",
            ROOT / "web" / "static" / "dashboard.js",
        ]
    )

    assert "PyQt5" not in dashboard_source
    assert "QAxWidget" not in dashboard_source
    assert "send_order" not in dashboard_source
    assert "cancel_order" not in dashboard_source
    assert "modify_order" not in dashboard_source
    assert "class OrderIntent" not in dashboard_source
    assert "class EntryPlan" not in dashboard_source
    assert "class PositionSizing" not in dashboard_source
    assert "class OMS" not in dashboard_source
    assert "GatewayCommand(" not in dashboard_source
    assert "/api/risk/evaluate" not in dashboard_source
    assert "/api/strategy/evaluate" not in dashboard_source
    assert "/api/candidates/rebuild" not in dashboard_source
    assert "/api/themes/snapshots/rebuild" not in dashboard_source
    assert "import openai" not in dashboard_source
    assert "from openai" not in dashboard_source
    assert "OpenAI(" not in dashboard_source


def test_dashboard_javascript_has_no_ai_run_post() -> None:
    dashboard_js = (ROOT / "web" / "static" / "dashboard.js").read_text(encoding="utf-8")

    assert 'method: "POST"' not in dashboard_js
    assert "method: 'POST'" not in dashboard_js
    assert "/api/ai-sidecar/run" not in dashboard_js
