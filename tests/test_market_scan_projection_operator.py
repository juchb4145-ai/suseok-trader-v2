from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from storage.sqlite import initialize_database, open_connection


def test_schema_53_additively_migrates_market_scan_projection_contract(tmp_path) -> None:
    db_path = tmp_path / "market-scan-schema.sqlite3"
    connection = initialize_database(db_path)
    for index_name in (
        "idx_market_scan_snapshots_source_event",
        "idx_market_scan_snapshots_request",
    ):
        connection.execute(f"DROP INDEX {index_name}")
    for table_name in (
        "market_scan_projection_reconcile_issues",
        "market_scan_projection_reconcile_runs",
        "market_scan_projection_routing_decisions",
    ):
        connection.execute(f"DROP TABLE {table_name}")
    for table_name in ("market_scan_snapshots", "market_scan_latest"):
        for column_name in (
            "source_event_id",
            "request_id",
            "parser_status",
            "generated_by",
        ):
            connection.execute(f"ALTER TABLE {table_name} DROP COLUMN {column_name}")
    connection.execute("UPDATE app_metadata SET value = '53' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    schema_version = migrated.execute(
        "SELECT value FROM app_metadata WHERE key = 'schema_version'"
    ).fetchone()["value"]
    snapshot_columns = {
        row["name"] for row in migrated.execute("PRAGMA table_info(market_scan_snapshots)")
    }
    latest_columns = {
        row["name"] for row in migrated.execute("PRAGMA table_info(market_scan_latest)")
    }
    tables = {
        row["name"]
        for row in migrated.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name LIKE 'market_scan_projection_%'
            """
        )
    }
    indexes = {
        row["name"]
        for row in migrated.execute("PRAGMA index_list(market_scan_snapshots)")
    }
    migrated.close()
    initialize_database(db_path).close()

    assert schema_version == "60"
    lineage = {"source_event_id", "request_id", "parser_status", "generated_by"}
    assert lineage <= snapshot_columns
    assert lineage <= latest_columns
    assert {
        "market_scan_projection_reconcile_issues",
        "market_scan_projection_reconcile_runs",
        "market_scan_projection_routing_decisions",
    } <= tables
    assert "idx_market_scan_snapshots_source_event" in indexes
    assert "idx_market_scan_snapshots_request" in indexes


def test_market_scan_operator_and_fast_dashboard_are_observe_safe(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-scan-operator.sqlite3"
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
        unauthorized = client.post(
            "/api/operator/market-scan-projection-reconcile/run-once"
        )
        reconcile = client.post(
            "/api/operator/market-scan-projection-reconcile/run-once?limit=10",
            headers={"X-Local-Token": "test-token"},
        )
        latest = client.get("/api/operator/market-scan-projection-reconcile/latest")
        routing = client.get("/api/operator/market-scan-append-only-routing/status")
        decisions = client.get(
            "/api/operator/market-scan-append-only-routing/decisions?limit=10"
        )
        operator_status = client.get("/api/operator/status")
        dashboard = client.get(
            "/api/dashboard/snapshot?fast=true&sections="
            "market_scan_projection_reconcile,market_scan_append_only_routing"
        )

    connection = open_connection(db_path)
    after_commands = _count(connection, "gateway_commands")
    after_orders = _count(connection, "dry_run_orders")
    connection.close()

    assert unauthorized.status_code == 401
    assert reconcile.status_code == 200
    assert reconcile.json()["status"] == "WARN"
    assert reconcile.json()["no_trading_side_effects"] is True
    assert latest.json()["latest_run"]["run_id"] == reconcile.json()["run_id"]
    assert routing.json()["effective_skip_inline_count"] == 0
    assert decisions.json()["decisions"] == []
    assert "market_scan" in operator_status.json()
    assert "market_scan_projection_reconcile" in dashboard.json()
    assert "market_scan_append_only_routing" in dashboard.json()
    assert after_commands == before_commands
    assert after_orders == before_orders


def test_schema_54_adds_market_scan_cutover_budget_and_controller_columns(
    tmp_path,
) -> None:
    db_path = tmp_path / "market-scan-cutover-schema.sqlite3"
    connection = initialize_database(db_path)
    connection.execute("DROP INDEX idx_market_scan_routing_effective_skip")
    connection.execute("DROP TABLE market_scan_append_only_budget_state")
    columns = (
        "cutover_enabled",
        "global_kill_switch",
        "event_age_sec",
        "event_future_skew_sec",
        "skip_budget_limit",
        "skip_budget_used",
        "skip_budget_remaining",
        "rollback_required",
        "controller_status",
    )
    for column_name in columns:
        connection.execute(
            "ALTER TABLE market_scan_projection_routing_decisions "
            f"DROP COLUMN {column_name}"
        )
    connection.execute("UPDATE app_metadata SET value = '54' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    migrated = initialize_database(db_path)
    schema_version = migrated.execute(
        "SELECT value FROM app_metadata WHERE key = 'schema_version'"
    ).fetchone()["value"]
    migrated_columns = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA table_info(market_scan_projection_routing_decisions)"
        )
    }
    indexes = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA index_list(market_scan_projection_routing_decisions)"
        )
    }
    budget_exists = migrated.execute(
        """
        SELECT COUNT(*) FROM sqlite_master
        WHERE type = 'table' AND name = 'market_scan_append_only_budget_state'
        """
    ).fetchone()[0]
    migrated.close()
    initialize_database(db_path).close()

    assert schema_version == "60"
    assert set(columns) <= migrated_columns
    assert "idx_market_scan_routing_effective_skip" in indexes
    assert budget_exists == 1


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
