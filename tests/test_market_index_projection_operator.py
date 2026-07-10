from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from storage.sqlite import initialize_database, open_connection


def test_schema_48_additively_migrates_market_index_and_context_tables(tmp_path) -> None:
    db_path = tmp_path / "market-index-schema.sqlite3"
    connection = initialize_database(db_path)
    for table_name in (
        "market_index_projection_reconcile_issues",
        "market_index_projection_reconcile_runs",
        "market_index_projection_routing_decisions",
        "market_index_append_only_budget_state",
    ):
        connection.execute(f"DROP TABLE {table_name}")
    connection.execute("DROP INDEX idx_market_regime_snapshots_source_event")
    connection.execute("DROP INDEX idx_candidate_context_market_snapshot")
    connection.execute(
        "ALTER TABLE candidate_context_latest DROP COLUMN market_context_snapshot_id"
    )
    connection.execute("DROP TABLE market_context_latest")
    connection.execute("DROP TABLE market_context_snapshots")
    for column_name in ("source_event_id", "source_projection", "generated_by"):
        connection.execute(
            f"ALTER TABLE market_regime_snapshots DROP COLUMN {column_name}"
        )
    connection.execute(
        "UPDATE app_metadata SET value = '48' WHERE key = 'schema_version'"
    )
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
                WHERE type = 'table'
                    AND (
                        name LIKE 'market_index_projection_%'
                        OR name = 'market_index_append_only_budget_state'
                        OR name LIKE 'market_context_%'
                    )
                """
        ).fetchall()
    }
    regime_columns = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA table_info(market_regime_snapshots)"
        ).fetchall()
    }
    regime_indexes = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA index_list(market_regime_snapshots)"
        ).fetchall()
    }
    candidate_context_columns = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA table_info(candidate_context_latest)"
        ).fetchall()
    }
    candidate_context_indexes = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA index_list(candidate_context_latest)"
        ).fetchall()
    }
    market_context_indexes = {
        row["name"]
        for row in migrated.execute(
            "PRAGMA index_list(market_context_snapshots)"
        ).fetchall()
    }
    market_context_query_plan = [
        str(row["detail"])
        for row in migrated.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT snapshot_id
            FROM market_context_snapshots
            WHERE source_event_id = 'evt_context_schema_probe'
            """
        ).fetchall()
    ]
    lineage_query_plan = [
        str(row["detail"])
        for row in migrated.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT snapshot_id
            FROM market_regime_snapshots
            WHERE source_event_id = 'evt_schema_probe'
            """
        ).fetchall()
    ]
    migrated.close()
    rerun = initialize_database(db_path)
    rerun.close()

    assert schema_version == "51"
    assert {
        "market_index_projection_reconcile_issues",
        "market_index_projection_reconcile_runs",
        "market_index_projection_routing_decisions",
        "market_index_append_only_budget_state",
        "market_context_snapshots",
        "market_context_latest",
    } <= tables
    assert {"source_event_id", "source_projection", "generated_by"} <= regime_columns
    assert "idx_market_regime_snapshots_source_event" in regime_indexes
    assert "market_context_snapshot_id" in candidate_context_columns
    assert "idx_candidate_context_market_snapshot" in candidate_context_indexes
    assert "idx_market_context_snapshots_market_time" in market_context_indexes
    assert "idx_market_context_snapshots_source_event" in market_context_indexes
    assert any(
        "idx_market_context_snapshots_source_event" in detail
        for detail in market_context_query_plan
    )
    assert any(
        "idx_market_regime_snapshots_source_event" in detail
        for detail in lineage_query_plan
    )


def test_market_index_operator_and_dashboard_surfaces_are_safe(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "market-index-operator.sqlite3"
    connection = initialize_database(db_path)
    before_commands = _count(connection, "gateway_commands")
    connection.close()
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_MODE", "OBSERVE")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "false")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")

    with TestClient(app) as client:
        unauthorized = client.post(
            "/api/operator/market-index-projection-reconcile/run-once"
        )
        reconcile = client.post(
            "/api/operator/market-index-projection-reconcile/run-once?limit=10",
            headers={"X-Local-Token": "test-token"},
        )
        latest = client.get("/api/operator/market-index-projection-reconcile/latest")
        routing = client.get("/api/operator/market-index-append-only-routing/status")
        decisions = client.get(
            "/api/operator/market-index-append-only-routing/decisions?limit=10"
        )
        dashboard = client.get(
            "/api/dashboard/snapshot?fast=true&sections="
            "market_indexes,market_index_projection_reconcile,"
            "market_index_append_only_routing"
        )

    connection = open_connection(db_path)
    after_commands = _count(connection, "gateway_commands")
    connection.close()

    assert unauthorized.status_code == 401
    assert reconcile.status_code == 200
    assert reconcile.json()["status"] == "WARN"
    assert reconcile.json()["no_trading_side_effects"] is True
    assert latest.status_code == 200
    assert latest.json()["latest_run"]["run_id"] == reconcile.json()["run_id"]
    assert routing.status_code == 200
    assert routing.json()["effective_skip_inline_count"] == 0
    assert routing.json()["tr_bootstrap_adapter_status"] == "NOT_IMPLEMENTED"
    assert decisions.status_code == 200
    assert decisions.json()["decisions"] == []
    assert dashboard.status_code == 200
    snapshot = dashboard.json()
    assert "market_indexes" in snapshot
    assert "market_index_projection_reconcile" in snapshot
    assert "market_index_append_only_routing" in snapshot
    assert snapshot["market_indexes"]["status"]["source_contract"][
        "tr_bootstrap_adapter_status"
    ] == "NOT_IMPLEMENTED"
    assert after_commands == before_commands


def _count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])
