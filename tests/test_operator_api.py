from __future__ import annotations

from apps.core_api import app
from fastapi.testclient import TestClient
from storage.sqlite import initialize_database, open_connection
from tests.test_live_sim_order_plan_pipeline import _prepared_order_plan_connection


def test_operator_api_read_only_and_rebuild_snapshot_only(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "operator-api.sqlite3"
    connection, _ = _prepared_order_plan_connection(db_path)
    before_counts = _counts(connection)
    connection.close()
    _set_operator_env(monkeypatch, db_path)

    with TestClient(app) as client:
        status = client.get("/api/operator/status?trade_date=2026-06-27")
        lock_status = client.get("/api/operator/runtime-execution-locks/status")
        uniqueness_status = client.get(
            "/api/operator/live-sim/order-plan-uniqueness/status"
        )
        no_buy = client.get("/api/operator/no-buy?trade_date=2026-06-27")
        latest_before = client.get("/api/operator/no-buy/latest?trade_date=2026-06-27")
        unauthorized = client.post("/api/operator/no-buy/rebuild?trade_date=2026-06-27")
        rebuilt = client.post(
            "/api/operator/no-buy/rebuild?trade_date=2026-06-27",
            headers={"X-Local-Token": "secret-token"},
        )
        latest_after = client.get("/api/operator/no-buy/latest?trade_date=2026-06-27")
        realtime_plan = client.get(
            "/api/operator/realtime-subscriptions/plan?trade_date=2026-06-27"
        )
        realtime_unauthorized = client.post(
            "/api/operator/realtime-subscriptions/run-once?trade_date=2026-06-27"
        )
        realtime_run = client.post(
            "/api/operator/realtime-subscriptions/run-once?trade_date=2026-06-27",
            headers={"X-Local-Token": "secret-token"},
        )

    connection = open_connection(db_path)
    after_counts = _counts(connection)
    snapshot_count = connection.execute(
        "SELECT COUNT(*) AS count FROM no_buy_sentinel_snapshots"
    ).fetchone()["count"]
    connection.close()

    assert status.status_code == 200
    assert status.json()["read_only"] is True
    assert status.json()["runtime_execution_locks"]["status"] == "PASS"
    assert status.json()["live_sim_order_plan_uniqueness"]["status"] == "PASS"
    assert lock_status.status_code == 200
    assert lock_status.json()["lock_count"] == 0
    assert lock_status.json()["read_only"] is True
    assert uniqueness_status.status_code == 200
    assert uniqueness_status.json()["status"] == "PASS"
    assert uniqueness_status.json()["lookup_strategy"] == (
        "DIRECT_ORDER_PLAN_ID_INDEX_LOOKUP"
    )
    assert uniqueness_status.json()["duplicate_group_count"] == 0
    assert no_buy.status_code == 200
    assert no_buy.json()["no_order_side_effects"] is True
    assert latest_before.status_code == 200
    assert unauthorized.status_code == 401
    assert rebuilt.status_code == 200
    assert rebuilt.json()["read_only"] is True
    assert latest_after.json()["snapshot"]["snapshot_id"] == rebuilt.json()["snapshot_id"]
    assert realtime_plan.status_code == 200
    assert realtime_plan.json()["read_only"] is True
    assert realtime_plan.json()["queue_commands"] is False
    assert realtime_plan.json()["command_count"] == 0
    assert realtime_unauthorized.status_code == 401
    assert realtime_run.status_code == 200
    assert realtime_run.json()["no_order_side_effects"] is True
    assert realtime_run.json()["command_count"] == 0
    assert after_counts == before_counts
    assert int(snapshot_count) == 1


def test_operator_event_retention_endpoints_are_token_protected(
    tmp_path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "operator-retention.sqlite3"
    initialize_database(db_path).close()
    _set_operator_env(monkeypatch, db_path)

    with TestClient(app) as client:
        status = client.get("/api/operator/event-retention/status")
        unauthorized = client.post("/api/operator/event-retention/prune")
        dry_run = client.post(
            "/api/operator/event-retention/prune",
            headers={"X-Local-Token": "secret-token"},
        )

    assert status.status_code == 200
    assert status.json()["read_only"] is True
    assert status.json()["dry_run_default"] is True
    assert unauthorized.status_code == 401
    assert dry_run.status_code == 200
    assert dry_run.json()["dry_run"] is True


def _set_operator_env(monkeypatch, db_path) -> None:
    monkeypatch.setenv("TRADING_DB_PATH", str(db_path))
    monkeypatch.setenv("TRADING_CORE_TOKEN", "secret-token")
    monkeypatch.setenv("TRADING_PROFILE", "LIVE_SIM_PILOT")
    monkeypatch.setenv("TRADING_MODE", "LIVE_SIM")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_SIM", "true")
    monkeypatch.setenv("TRADING_ALLOW_LIVE_REAL", "false")
    monkeypatch.setenv("LIVE_SIM_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_ROUTING_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_GATEWAY_COMMAND_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ACCOUNT_ID", "SIM-12345678")
    monkeypatch.setenv("LIVE_SIM_KILL_SWITCH", "false")
    monkeypatch.setenv("LIVE_SIM_STALE_TICK_SEC", "999999999")
    monkeypatch.setenv("LIVE_SIM_PILOT_PIPELINE_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED", "true")
    monkeypatch.setenv("LIVE_SIM_ORDER_PLAN_STALE_SEC", "999999999")
    monkeypatch.setenv("REALTIME_SUBSCRIPTION_QUEUE_COMMANDS", "false")


def _counts(connection) -> dict[str, int]:
    tables = ["live_sim_intents", "live_sim_orders", "gateway_commands"]
    return {
        table: int(connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"])
        for table in tables
    }
