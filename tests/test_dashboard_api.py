from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from tests.support_fastapi_routes import iter_app_routes


def test_dashboard_api_endpoints_are_get_read_only_without_token(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(tmp_path / "dashboard-api.sqlite3"))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")

    with TestClient(app) as client:
        status = client.get("/api/dashboard/status")
        snapshot = client.get("/api/dashboard/snapshot")
        full_snapshot = client.get("/api/dashboard/snapshot?detail=full")
        funnel = client.get("/api/dashboard/funnel")
        errors = client.get("/api/dashboard/errors")

    assert status.status_code == 200
    assert status.json()["read_only"] is True
    assert status.json()["order_controls_available"] is False
    assert status.json()["ai_execution_available"] is False
    assert snapshot.status_code == 200
    assert snapshot.json()["safety"]["order_routing_enabled"] is False
    assert snapshot.json()["safety"]["dry_run_order_controls_available"] is False
    assert snapshot.json()["safety"]["broker_order_sent"] is False
    assert snapshot.json()["dry_run"]["order_routing_enabled"] is False
    assert snapshot.json()["dry_run"]["gateway_command_enabled"] is False
    assert snapshot.json()["dry_run"]["live_order_allowed"] is False
    assert snapshot.json()["dry_run"]["broker_order_sent"] is False
    assert snapshot.json()["safety"]["ai_context_builder_available"] is True
    assert snapshot.json()["ai_sidecar"]["status"]["context_builder_available"] is True
    assert snapshot.json()["ai_sidecar"]["status"]["openai_client_available"] is False
    assert snapshot.json()["ai_sidecar"]["status"]["execution_api_available"] is True
    assert snapshot.json()["ai_sidecar"]["status"]["tools_enabled"] is False
    assert snapshot.json()["ai_sidecar"]["status"]["order_tools_enabled"] is False
    assert "request_status_counts" in snapshot.json()["ai_sidecar"]
    assert full_snapshot.status_code == 200
    assert full_snapshot.json()["detail"] == "full"
    assert funnel.status_code == 200
    assert "gateway" in funnel.json()
    assert errors.status_code == 200
    assert "market_projection_errors" in errors.json()


def test_dashboard_router_exposes_no_post_routes() -> None:
    dashboard_routes = [
        route
        for route in iter_app_routes(app)
        if route.path.startswith("/api/dashboard")
    ]

    assert dashboard_routes
    assert all("POST" not in route.methods for route in dashboard_routes)
