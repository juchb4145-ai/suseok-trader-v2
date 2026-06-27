from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from services.config import Settings
from services.risk_gate import evaluate_risk_for_candidate, save_risk_observation
from services.strategy_engine import evaluate_candidate_strategy, save_strategy_observation
from storage.sqlite import initialize_database
from tests.test_oms_dry_run import _settings
from tests.test_strategy_service import _insert_strategy_fixture


def test_dry_run_api_get_endpoints_and_post_token_safety(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "dry-run-api.sqlite3"
    candidate_id = _prepare_db(db_path)
    _set_api_env(monkeypatch, db_path)

    with TestClient(app) as client:
        status = client.get("/api/dry-run/status")
        eligibility = client.get("/api/dry-run/eligibility")
        intents = client.get("/api/dry-run/intents")
        orders = client.get("/api/dry-run/orders")
        executions = client.get("/api/dry-run/executions")
        positions = client.get("/api/dry-run/positions")
        ledger = client.get("/api/dry-run/ledger")
        errors = client.get("/api/dry-run/errors")
        unauthorized = client.post(f"/api/dry-run/intents/from-candidate/{candidate_id}")
        evaluated = client.post(
            "/api/dry-run/evaluate",
            params={"candidate_instance_id": candidate_id},
            headers={"X-Local-Token": "secret-token"},
        )
        created = client.post(
            f"/api/dry-run/intents/from-candidate/{candidate_id}",
            headers={"X-Local-Token": "secret-token"},
        )

    assert status.status_code == 200
    assert status.json()["enabled"] is True
    assert status.json()["order_routing_enabled"] is False
    assert status.json()["gateway_command_enabled"] is False
    assert eligibility.status_code == 200
    assert intents.status_code == 200
    assert orders.status_code == 200
    assert executions.status_code == 200
    assert positions.status_code == 200
    assert ledger.status_code == 200
    assert errors.status_code == 200
    assert unauthorized.status_code == 401
    assert evaluated.status_code == 200
    assert evaluated.json()["eligibility"]["eligible"] is True
    assert created.status_code == 200
    assert created.json()["dry_run_only"] is True
    assert created.json()["live_order_allowed"] is False
    assert created.json()["gateway_command_allowed"] is False
    assert created.json()["broker_order_sent"] is False
    assert created.json()["intent"]["status"] == "CREATED"


def test_dry_run_order_and_fill_api_are_token_protected_and_simulation_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "dry-run-api-fill.sqlite3"
    candidate_id = _prepare_db(db_path)
    _set_api_env(monkeypatch, db_path, simulated_fill="true")

    with TestClient(app) as client:
        created = client.post(
            f"/api/dry-run/intents/from-candidate/{candidate_id}",
            headers={"X-Local-Token": "secret-token"},
        )
        intent_id = created.json()["intent"]["dry_run_intent_id"]
        unauthorized = client.post(f"/api/dry-run/orders/from-intent/{intent_id}")
        order = client.post(
            f"/api/dry-run/orders/from-intent/{intent_id}",
            headers={"X-Local-Token": "secret-token"},
        )
        order_id = order.json()["order"]["dry_run_order_id"]
        fill = client.post(
            f"/api/dry-run/orders/{order_id}/simulate-fill",
            headers={"X-Local-Token": "secret-token"},
        )
        mtm = client.post(
            "/api/dry-run/positions/mark-to-market",
            headers={"X-Local-Token": "secret-token"},
        )

    assert unauthorized.status_code == 401
    assert order.status_code == 200
    assert order.json()["broker_order_sent"] is False
    assert order.json()["order"]["status"] == "CREATED"
    assert fill.status_code == 200
    assert fill.json()["execution"]["execution_type"] == "SIMULATED"
    assert fill.json()["broker_order_sent"] is False
    assert mtm.status_code == 200
    assert mtm.json()["broker_order_sent"] is False


def test_dry_run_routes_do_not_add_order_enqueue_surface() -> None:
    paths = {route.path for route in app.routes}
    dry_run_routes = [route for route in app.routes if route.path.startswith("/api/dry-run")]

    assert "/api/dry-run/status" in paths
    assert "/api/dry-run/intents/from-candidate/{candidate_instance_id}" in paths
    assert "/api/orders/enqueue" not in paths
    assert all("send_order" not in path for path in paths)
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)
    assert any("POST" in route.methods for route in dry_run_routes)


def _prepare_db(db_path) -> str:
    connection = initialize_database(db_path)
    settings = _settings()
    candidate_id = _insert_strategy_fixture(connection)
    strategy = evaluate_candidate_strategy(connection, candidate_id, settings=settings)
    save_strategy_observation(connection, strategy)
    risk = evaluate_risk_for_candidate(connection, candidate_id, settings=settings)
    save_risk_observation(connection, risk)
    connection.commit()
    connection.close()
    return candidate_id


def _set_api_env(monkeypatch, db_path, *, simulated_fill: str = "false") -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("DRY_RUN_OMS_ENABLED", "true")
    monkeypatch.setenv("DRY_RUN_INTENT_CREATION_ENABLED", "true")
    monkeypatch.setenv("DRY_RUN_SIMULATED_FILL_ENABLED", simulated_fill)
    monkeypatch.setenv("DRY_RUN_ALLOW_WITHOUT_SAFETY_DRAFT_FOR_TESTS", "true")
    monkeypatch.setenv("DRY_RUN_STALE_TICK_SEC", "999999999")
    Settings()
