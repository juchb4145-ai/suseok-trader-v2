from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from storage.sqlite import initialize_database, open_connection


def test_schema_51_additively_migrates_market_regime_projection_tables(
    tmp_path,
) -> None:
    db_path = tmp_path / "market-regime-schema.sqlite3"
    connection = initialize_database(db_path)
    for table_name in (
        "market_regime_projection_reconcile_issues",
        "market_regime_projection_reconcile_runs",
        "market_regime_projection_routing_decisions",
    ):
        connection.execute(f"DROP TABLE {table_name}")
    connection.execute("UPDATE app_metadata SET value = '51' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    schema_version = migrated.execute(
        "SELECT value FROM app_metadata WHERE key = 'schema_version'"
    ).fetchone()["value"]
    tables = {
        row["name"]
        for row in migrated.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name LIKE 'market_regime_projection_%'
            """
        ).fetchall()
    }
    reconcile_indexes = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA index_list(market_regime_projection_reconcile_runs)"
        ).fetchall()
    }
    routing_indexes = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA index_list(market_regime_projection_routing_decisions)"
        ).fetchall()
    }
    migrated.close()
    rerun = initialize_database(db_path)
    rerun.close()

    assert schema_version == "53"
    assert {
        "market_regime_projection_reconcile_issues",
        "market_regime_projection_reconcile_runs",
        "market_regime_projection_routing_decisions",
    } <= tables
    assert "idx_market_regime_reconcile_runs_created" in reconcile_indexes
    assert "idx_market_regime_routing_event" in routing_indexes
    assert "idx_market_regime_routing_decided" in routing_indexes


def test_schema_52_adds_market_regime_cutover_budget_and_routing_columns(
    tmp_path,
) -> None:
    db_path = tmp_path / "market-regime-cutover-schema.sqlite3"
    connection = initialize_database(db_path)
    connection.execute("DROP INDEX idx_market_regime_routing_effective_skip")
    connection.execute("DROP TABLE market_regime_append_only_budget_state")
    new_columns = (
        "cutover_enabled",
        "global_kill_switch",
        "skip_budget_limit",
        "skip_budget_used",
        "skip_budget_remaining",
        "observe_safe",
        "index_routing_ready",
        "rollback_required",
        "controller_status",
    )
    for column_name in new_columns:
        connection.execute(
            f"ALTER TABLE market_regime_projection_routing_decisions DROP COLUMN {column_name}"
        )
    connection.execute("UPDATE app_metadata SET value = '52' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    schema_version = migrated.execute(
        "SELECT value FROM app_metadata WHERE key = 'schema_version'"
    ).fetchone()["value"]
    columns = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA table_info(market_regime_projection_routing_decisions)"
        ).fetchall()
    }
    indexes = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA index_list(market_regime_projection_routing_decisions)"
        ).fetchall()
    }
    budget_exists = migrated.execute(
        """
        SELECT COUNT(*) AS count FROM sqlite_master
        WHERE type = 'table' AND name = 'market_regime_append_only_budget_state'
        """
    ).fetchone()["count"]
    migrated.close()
    rerun = initialize_database(db_path)
    rerun.close()

    assert schema_version == "53"
    assert set(new_columns) <= columns
    assert "idx_market_regime_routing_effective_skip" in indexes
    assert budget_exists == 1


def test_market_regime_operator_and_dashboard_surfaces_are_observe_safe(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-regime-operator.sqlite3"
    connection = initialize_database(db_path)
    before_commands = _count(connection, "gateway_commands")
    before_orders = _count(connection, "dry_run_orders")
    connection.close()
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_PROFILE", "OBSERVE")
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")

    with TestClient(app) as client:
        unauthorized = client.post("/api/operator/market-regime-projection-reconcile/run-once")
        reconcile = client.post(
            "/api/operator/market-regime-projection-reconcile/run-once?limit=10",
            headers={"X-Local-Token": "test-token"},
        )
        latest = client.get("/api/operator/market-regime-projection-reconcile/latest")
        routing = client.get("/api/operator/market-regime-append-only-routing/status")
        decisions = client.get("/api/operator/market-regime-append-only-routing/decisions?limit=10")
        operator_status = client.get("/api/operator/status")
        dashboard = client.get(
            "/api/dashboard/snapshot?fast=true&sections="
            "market_regime_projection_reconcile,"
            "market_regime_append_only_routing"
        )

    connection = open_connection(db_path)
    after_commands = _count(connection, "gateway_commands")
    after_orders = _count(connection, "dry_run_orders")
    connection.close()

    assert unauthorized.status_code == 401
    assert reconcile.status_code == 200
    assert reconcile.json()["status"] == "FAIL"
    assert reconcile.json()["no_trading_side_effects"] is True
    assert latest.status_code == 200
    assert latest.json()["latest_run"]["run_id"] == reconcile.json()["run_id"]
    assert routing.status_code == 200
    assert routing.json()["effective_skip_inline_count"] == 0
    assert decisions.status_code == 200
    assert decisions.json()["decisions"] == []
    assert operator_status.status_code == 200
    assert "market_regime" in operator_status.json()
    assert dashboard.status_code == 200
    snapshot = dashboard.json()
    assert "market_regime_projection_reconcile" in snapshot
    assert "market_regime_append_only_routing" in snapshot
    assert after_commands == before_commands
    assert after_orders == before_orders


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
