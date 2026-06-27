from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from storage.sqlite import initialize_database
from tests.test_exit_engine import _insert_position
from tests.test_oms_dry_run import _prepared_connection


def test_dry_run_exit_api_get_endpoints_and_evaluate_token_safety(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "dry-run-exit-api.sqlite3"
    connection, _ = _prepared_connection(db_path)
    position_id = _insert_position(connection, avg_price=100_000)
    connection.close()
    _set_api_env(monkeypatch, db_path)

    with TestClient(app) as client:
        status = client.get("/api/dry-run/exits/status")
        evaluations = client.get("/api/dry-run/exits/evaluations")
        signals = client.get("/api/dry-run/exits/signals")
        intents = client.get("/api/dry-run/exits/intents")
        orders = client.get("/api/dry-run/exits/orders")
        executions = client.get("/api/dry-run/exits/executions")
        runs = client.get("/api/dry-run/exits/runs")
        errors = client.get("/api/dry-run/exits/errors")
        unauthorized = client.post(
            "/api/dry-run/exits/evaluate",
            params={"dry_run_position_id": position_id},
        )
        evaluated = client.post(
            "/api/dry-run/exits/evaluate",
            params={"dry_run_position_id": position_id},
            headers={"X-Local-Token": "secret-token"},
        )
        evaluation_id = evaluated.json()["evaluation"]["exit_evaluation_id"]
        evaluation_detail = client.get(f"/api/dry-run/exits/evaluations/{evaluation_id}")

    assert status.status_code == 200
    assert status.json()["enabled"] is True
    assert status.json()["gateway_command_allowed"] is False
    assert evaluations.status_code == 200
    assert signals.status_code == 200
    assert intents.status_code == 200
    assert orders.status_code == 200
    assert executions.status_code == 200
    assert runs.status_code == 200
    assert errors.status_code == 200
    assert unauthorized.status_code == 401
    assert evaluated.status_code == 200
    assert evaluated.json()["dry_run_only"] is True
    assert evaluated.json()["close_only"] is True
    assert evaluated.json()["broker_order_sent"] is False
    assert evaluation_detail.status_code == 200
    assert evaluation_detail.json()["evaluation"]["signals"]


def test_dry_run_exit_api_manual_lifecycle_is_token_protected_and_simulation_only(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "dry-run-exit-api-fill.sqlite3"
    connection, _ = _prepared_connection(db_path)
    position_id = _insert_position(connection, avg_price=100_000)
    connection.close()
    _set_api_env(monkeypatch, db_path, order_creation="true", simulated_fill="true")

    with TestClient(app) as client:
        unauthorized = client.post(f"/api/dry-run/exits/intents/from-position/{position_id}")
        intent_response = client.post(
            f"/api/dry-run/exits/intents/from-position/{position_id}",
            headers={"X-Local-Token": "secret-token"},
        )
        intent_id = intent_response.json()["intent"]["dry_run_exit_intent_id"]
        order_response = client.post(
            f"/api/dry-run/exits/orders/from-intent/{intent_id}",
            headers={"X-Local-Token": "secret-token"},
        )
        order_id = order_response.json()["order"]["dry_run_exit_order_id"]
        fill_response = client.post(
            f"/api/dry-run/exits/orders/{order_id}/simulate-fill",
            headers={"X-Local-Token": "secret-token"},
        )

    assert unauthorized.status_code == 401
    assert intent_response.status_code == 200
    assert intent_response.json()["intent"]["status"] == "CREATED"
    assert intent_response.json()["intent"]["side"] == "SELL"
    assert intent_response.json()["close_only"] is True
    assert intent_response.json()["broker_order_sent"] is False
    assert order_response.status_code == 200
    assert order_response.json()["order"]["status"] == "CREATED"
    assert order_response.json()["gateway_command_allowed"] is False
    assert fill_response.status_code == 200
    assert fill_response.json()["execution"]["execution_type"] == "SIMULATED_EXIT"
    assert fill_response.json()["execution"]["broker_order_sent"] is False

    connection = initialize_database(db_path)
    gateway_count = connection.execute("SELECT COUNT(*) AS count FROM gateway_commands").fetchone()[
        "count"
    ]
    connection.close()
    assert gateway_count == 0


def test_dry_run_exit_routes_do_not_add_order_enqueue_surface() -> None:
    paths = {route.path for route in app.routes}
    exit_routes = [route for route in app.routes if route.path.startswith("/api/dry-run/exits")]

    assert "/api/dry-run/exits/status" in paths
    assert "/api/dry-run/exits/evaluate" in paths
    assert "/api/orders/enqueue" not in paths
    assert all("send_order" not in path for path in paths)
    assert all("cancel_order" not in path for path in paths)
    assert all("modify_order" not in path for path in paths)
    assert any("POST" in route.methods for route in exit_routes)


def _set_api_env(
    monkeypatch,
    db_path,
    *,
    order_creation: str = "false",
    simulated_fill: str = "false",
) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("DRY_RUN_ALLOW_WITHOUT_SAFETY_DRAFT_FOR_TESTS", "true")
    monkeypatch.setenv("DRY_RUN_EXIT_ENGINE_ENABLED", "true")
    monkeypatch.setenv("DRY_RUN_EXIT_INTENT_CREATION_ENABLED", "true")
    monkeypatch.setenv("DRY_RUN_EXIT_ORDER_CREATION_ENABLED", order_creation)
    monkeypatch.setenv("DRY_RUN_EXIT_SIMULATED_FILL_ENABLED", simulated_fill)
    monkeypatch.setenv("DRY_RUN_EXIT_STALE_TICK_SEC", "999999999")
