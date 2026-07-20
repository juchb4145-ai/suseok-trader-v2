from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from services.dashboard_service import build_dashboard_snapshot
from services.live_sim.live_sim_service import create_live_sim_intent
from services.oms.dry_run_service import create_dry_run_intent
from storage.sqlite import initialize_database
from tests.support_fastapi_routes import iter_app_routes
from tests.test_live_sim import _live_sim_settings, _mark_gateway_ready
from tests.test_oms_dry_run import _prepared_connection
from tests.test_oms_dry_run import _settings as _dry_run_settings


def test_live_sim_api_get_post_token_and_queue(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "live-sim-api.sqlite3"
    connection, candidate_id = _prepared_connection(db_path)
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    connection.close()
    _set_live_sim_api_env(monkeypatch, db_path)

    with TestClient(app) as client:
        status = client.get("/api/live-sim/status")
        intents = client.get("/api/live-sim/intents")
        orders = client.get("/api/live-sim/orders")
        executions = client.get("/api/live-sim/executions")
        rejections = client.get("/api/live-sim/rejections")
        reconcile = client.get("/api/live-sim/reconcile")
        errors = client.get("/api/live-sim/errors")
        unauthorized = client.post(
            f"/api/live-sim/intents/from-candidate/{candidate_id}",
        )
        evaluated = client.post(
            "/api/live-sim/evaluate",
            params={"candidate_instance_id": candidate_id},
            headers={"X-Local-Token": "secret-token"},
        )
        created = client.post(
            f"/api/live-sim/intents/from-candidate/{candidate_id}",
            headers={"X-Local-Token": "secret-token"},
        )
        intent_id = created.json()["intent"]["live_sim_intent_id"]
        queued = client.post(
            f"/api/live-sim/orders/from-intent/{intent_id}",
            headers={"X-Local-Token": "secret-token"},
        )
        reconciled = client.post(
            "/api/live-sim/reconcile",
            headers={"X-Local-Token": "secret-token"},
        )
        broker_reconciled = client.post(
            "/api/live-sim/reconcile/broker-snapshot",
            headers={"X-Local-Token": "secret-token"},
            json={"account_id": "SIM-12345678", "open_orders": [], "positions": []},
        )

    assert status.status_code == 200
    assert status.json()["enabled"] is True
    assert intents.status_code == 200
    assert orders.status_code == 200
    assert executions.status_code == 200
    assert rejections.status_code == 200
    assert reconcile.status_code == 200
    assert errors.status_code == 200
    assert unauthorized.status_code == 401
    assert evaluated.status_code == 200
    assert evaluated.json()["eligibility"]["eligible"] is True
    assert created.status_code == 200
    assert created.json()["live_sim_only"] is True
    assert created.json()["live_real_allowed"] is False
    assert queued.status_code == 200
    assert queued.json()["gateway_command_id"]
    assert queued.json()["real_order_allowed"] is False
    assert reconciled.status_code == 200
    assert broker_reconciled.status_code == 200
    assert broker_reconciled.json()["reconcile"]["snapshot_json"]["broker_snapshot_available"]


def test_dashboard_snapshot_includes_live_sim_read_only(tmp_path) -> None:
    connection, candidate_id = _prepared_connection(tmp_path / "live-sim-dashboard.sqlite3")
    create_dry_run_intent(connection, candidate_id, settings=_dry_run_settings())
    _mark_gateway_ready(connection)
    settings = _live_sim_settings()
    create_live_sim_intent(connection, candidate_id, settings=settings)

    snapshot = build_dashboard_snapshot(connection, settings=settings)
    connection.close()

    assert "live_sim" in snapshot
    assert snapshot["live_sim"]["read_only"] is True
    assert snapshot["live_sim"]["order_controls_available"] is False
    assert snapshot["live_sim"]["live_real_allowed"] is False
    assert snapshot["live_sim"]["intent_count"] == 1
    assert snapshot["pipeline_summary"]["live_sim"]["intent_count"] == 1


def test_live_sim_api_queues_masked_read_only_broker_snapshot_request(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "live-sim-broker-snapshot-api.sqlite3"
    initialize_database(db_path).close()
    _set_live_sim_api_env(monkeypatch, db_path)
    monkeypatch.setenv(
        "LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED",
        "true",
    )

    with TestClient(app) as client:
        response = client.post(
            "/api/live-sim/reconcile/broker-snapshot/request",
            params={"snapshot_id": "api-snapshot-1"},
            headers={"X-Local-Token": "secret-token"},
        )

    assert response.status_code == 200
    request = response.json()["broker_snapshot_request"]
    assert request["snapshot_status"] == "REQUESTED"
    assert request["account_id_masked"] == "***5678"
    assert "SIM-12345678" not in response.text
    connection = initialize_database(db_path)
    command = connection.execute(
        "SELECT command_type, payload_json FROM gateway_commands"
    ).fetchone()
    connection.close()
    assert command["command_type"] == "broker_snapshot_request"
    assert '"read_only":true' in command["payload_json"]


def test_live_sim_routes_do_not_add_generic_order_surface() -> None:
    paths = {route.path for route in iter_app_routes(app)}

    assert "/api/live-sim/status" in paths
    assert "/api/live-sim/orders/from-intent/{live_sim_intent_id}" in paths
    assert "/api/live-sim/reconcile/broker-snapshot/request" in paths
    assert "/api/live-sim/automation/canary/status" in paths
    assert "/api/live-sim/automation/canary/run-once" in paths
    assert "/api/orders/enqueue" not in paths
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)


def _set_live_sim_api_env(monkeypatch, db_path) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("TRADING_MODE", "LIVE_SIM")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "true")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("LIVE_SIM_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_ROUTING_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_GATEWAY_COMMAND_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ACCOUNT_ID", "SIM-12345678")
    monkeypatch.setenv("LIVE_SIM_KILL_SWITCH", "false")
    monkeypatch.setenv("LIVE_SIM_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("LIVE_SIM_ENTRY_WINDOW_START", "00:00:00")
    monkeypatch.setenv("LIVE_SIM_ENTRY_WINDOW_END", "23:59:58")
    monkeypatch.setenv("LIVE_SIM_EXIT_EOD_FLATTEN_TIME", "23:59:59")
